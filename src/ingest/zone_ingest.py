import logging
import os

import pandas as pd
import requests
from pyspark.sql import SparkSession

from src.db.connection import JDBC_PROPERTIES, JDBC_URL, execute

logger = logging.getLogger(__name__)

ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def ingest_zone_lookup(spark: SparkSession) -> int:
    local_path = "/tmp/taxi_zone_lookup.csv"
    logger.info("Downloading zone lookup", extra={"url": ZONE_LOOKUP_URL})

    response = requests.get(ZONE_LOOKUP_URL, timeout=60)
    response.raise_for_status()
    with open(local_path, "wb") as f:
        f.write(response.content)

    try:
        df_pandas = pd.read_csv(local_path).rename(
            columns={
                "LocationID": "location_id",
                "Borough": "borough",
                "Zone": "zone",
            }
        )[["location_id", "borough", "zone", "service_zone"]]
        df_pandas = df_pandas.dropna(subset=["location_id"])
        df_pandas["location_id"] = df_pandas["location_id"].astype(int)

        # Full refresh: the lookup is a small, slowly changing reference set.
        execute("TRUNCATE staging.zone_lookup")

        df_zones = spark.createDataFrame(df_pandas)
        count = df_zones.count()
        df_zones.write.jdbc(
            url=JDBC_URL,
            table="staging.zone_lookup",
            mode="append",
            properties=JDBC_PROPERTIES,
        )
    finally:
        os.remove(local_path)

    logger.info("Zone lookup ingestion complete", extra={"written": count})
    return count
