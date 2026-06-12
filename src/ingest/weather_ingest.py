import calendar
import logging
import os

import pandas as pd
import requests
from pyspark.sql import SparkSession
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.db.connection import JDBC_PROPERTIES, JDBC_URL

logger = logging.getLogger(__name__)

# 15-minute observations; aggregated to full hours in dwh.fact_weather.
# Note: the plain archive API ignores minutely_15 — only the
# historical-forecast endpoint serves 15-minute data.
WEATHER_API_BASE = (
    "https://historical-forecast-api.open-meteo.com/v1/forecast"
    "?latitude=40.7128&longitude=-74.0060"
    "&start_date={start_date}&end_date={end_date}"
    "&minutely_15=temperature_2m,precipitation,weathercode,windspeed_10m"
)
MINUTELY_FIELDS = {
    "time",
    "temperature_2m",
    "precipitation",
    "weathercode",
    "windspeed_10m",
}


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _validate_response(data: dict) -> None:
    if "minutely_15" not in data:
        raise ValueError("Missing 'minutely_15' key in Open-Meteo response")
    missing = MINUTELY_FIELDS - set(data["minutely_15"].keys())
    if missing:
        raise ValueError(f"Missing minutely_15 fields: {missing}")


def ingest_weather_data(spark: SparkSession, year: int, month: int) -> int:
    last_day = calendar.monthrange(year, month)[1]
    start_date = f"{year:04d}-{month:02d}-01"
    end_date = f"{year:04d}-{month:02d}-{last_day:02d}"
    url = WEATHER_API_BASE.format(start_date=start_date, end_date=end_date)

    logger.info("Fetching weather data", extra={"year": year, "month": month})
    session = _build_session()
    response = session.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    _validate_response(data)

    block = data["minutely_15"]
    df_pandas = pd.DataFrame(
        {
            "time": pd.to_datetime(block["time"]),
            "temperature_2m": block["temperature_2m"],
            "precipitation": block["precipitation"],
            "weathercode": block["weathercode"],
            "windspeed_10m": block["windspeed_10m"],
        }
    )

    local_path = f"/tmp/weather_{year:04d}_{month:02d}.parquet"
    df_pandas.to_parquet(local_path, index=False)
    logger.info(
        "Saved weather parquet", extra={"dest": local_path, "rows": len(df_pandas)}
    )

    try:
        df_weather = spark.read.parquet(local_path)
        count = df_weather.count()

        logger.info(
            "Writing weather records",
            extra={"count": count, "table": "staging.fact_weather"},
        )
        df_weather.write.jdbc(
            url=JDBC_URL,
            table="staging.fact_weather",
            mode="append",
            properties=JDBC_PROPERTIES,
        )
    finally:
        os.remove(local_path)

    logger.info(
        "Weather ingestion complete",
        extra={"year": year, "month": month, "written": count},
    )
    return count
