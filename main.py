import argparse
import logging
import sys

from src.ingest.logging_config import setup_json_logging

setup_json_logging()

from pyspark.sql import SparkSession

from src.ingest.taxi_ingest import ingest_tlc_data
from src.ingest.weather_ingest import ingest_weather_data
from src.ingest.zone_ingest import ingest_zone_lookup
from src.transform.pipeline import period_bounds, prepare_load, run_pipeline

logger = logging.getLogger(__name__)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("NYCTaxiWeatherDWH")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Load TLC + weather data into the PostgreSQL data warehouse"
    )
    p.add_argument("--year", type=int, default=2023)
    p.add_argument("--start-month", type=int, default=1, dest="start_month")
    p.add_argument("--end-month", type=int, default=1, dest="end_month")
    p.add_argument(
        "--mode",
        choices=["init", "incremental"],
        default="init",
        help="init: full rebuild; incremental: reload only the given period",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    months = list(range(args.start_month, args.end_month + 1))
    start, end = period_bounds(args.year, months)
    logger.info(
        "Load started",
        extra={"year": args.year, "months": months, "mode": args.mode},
    )

    spark = build_spark()
    try:
        prepare_load(args.mode, start, end)
        ingest_zone_lookup(spark)
        for month in months:
            ingest_tlc_data(spark, year=args.year, month=month)
            ingest_weather_data(spark, year=args.year, month=month)
        run_pipeline(spark, year=args.year, months=months)
    except Exception:
        logger.exception("Load failed")
        sys.exit(1)
    finally:
        spark.stop()

    logger.info(
        "Load finished",
        extra={"year": args.year, "months": months, "mode": args.mode},
    )


if __name__ == "__main__":
    main()
