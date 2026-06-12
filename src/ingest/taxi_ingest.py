import logging
import os

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.db.connection import JDBC_PROPERTIES, JDBC_URL

logger = logging.getLogger(__name__)

TLC_BASE_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year:04d}-{month:02d}.parquet"
)

# Parquet camelCase -> staging.fact_trip snake_case
COLUMN_RENAMES = {
    "VendorID": "vendor_id",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "Airport_fee": "airport_fee",
}

STAGING_COLUMNS = [
    "vendor_id",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pu_location_id",
    "do_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]


def ingest_tlc_data(spark: SparkSession, year: int, month: int) -> int:
    month_start = f"{year:04d}-{month:02d}-01"
    next_month = (
        f"{year + 1:04d}-01-01" if month == 12 else f"{year:04d}-{month + 1:02d}-01"
    )
    url = TLC_BASE_URL.format(year=year, month=month)
    local_path = f"/tmp/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    logger.info("Downloading TLC parquet", extra={"url": url, "dest": local_path})

    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    try:
        df_raw = spark.read.parquet(local_path)
        raw_count = df_raw.count()
        logger.info(
            "Raw record count",
            extra={"count": raw_count, "year": year, "month": month},
        )

        # Cleaning rules per ETL spec: drop non-positive amounts,
        # zero-distance and zero-passenger trips. TLC monthly files also
        # contain stray pickups outside the nominal month — those would
        # break idempotent period reloads, so they are dropped too.
        df_clean = df_raw.filter(
            (col("total_amount") > 0)
            & (col("trip_distance") > 0)
            & (col("passenger_count") > 0)
            & (col("tpep_pickup_datetime") >= month_start)
            & (col("tpep_pickup_datetime") < next_month)
        )

        for src, dst in COLUMN_RENAMES.items():
            if src in df_clean.columns:
                df_clean = df_clean.withColumnRenamed(src, dst)
        df_clean = df_clean.select(*STAGING_COLUMNS).dropDuplicates()

        count = df_clean.count()
        logger.info(
            "Writing clean records",
            extra={"count": count, "table": "staging.fact_trip"},
        )

        df_clean.write.jdbc(
            url=JDBC_URL,
            table="staging.fact_trip",
            mode="append",
            properties=JDBC_PROPERTIES,
        )
    finally:
        os.remove(local_path)

    logger.info(
        "TLC ingestion complete",
        extra={"year": year, "month": month, "written": count},
    )
    return count
