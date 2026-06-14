import calendar
import logging
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    date_trunc,
    hour,
    minute,
    row_number,
    sum,
    unix_timestamp,
    when,
)
from pyspark.sql.window import Window

from src.db.connection import JDBC_PROPERTIES, JDBC_URL, execute, fetch_one
from src.transform.dimensions import date_key, load_dim_date, load_dim_location
from src.transform.scd_simulation import overrides_effective_on, planned_changes

logger = logging.getLogger(__name__)


def period_bounds(year: int, months: list[int]) -> tuple[date, date]:
    """Inclusive [first day of first month, last day of last month]."""
    start = date(year, months[0], 1)
    end = date(year, months[-1], calendar.monthrange(year, months[-1])[1])
    return start, end


def prepare_load(mode: str, start: date, end: date) -> None:
    """Clear targets so ingestion + transform stay idempotent per period.

    init: full rebuild — truncate staging and dwh facts.
    incremental: delete only rows belonging to the loaded period.
    """
    if mode == "init":
        execute("TRUNCATE staging.fact_trip, staging.fact_weather")
        execute("TRUNCATE dwh.fact_trip, dwh.fact_weather")
        logger.info("Init load: truncated staging and dwh facts")
        return

    next_day = end + timedelta(days=1)
    execute(
        "DELETE FROM staging.fact_trip "
        "WHERE tpep_pickup_datetime >= %s AND tpep_pickup_datetime < %s",
        (start, next_day),
    )
    execute(
        "DELETE FROM staging.fact_weather WHERE time >= %s AND time < %s",
        (start, next_day),
    )
    for table in ("dwh.fact_trip", "dwh.fact_weather"):
        execute(
            f"DELETE FROM {table} WHERE date_key BETWEEN %s AND %s",
            (date_key(start), date_key(end)),
        )
    logger.info(
        "Incremental load: cleared period",
        extra={"start": start.isoformat(), "end": end.isoformat()},
    )


def _read_staging(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPERTIES)


def _with_date_time_keys(df: DataFrame, ts_column: str) -> DataFrame:
    return df.withColumn(
        "date_key", date_format(col(ts_column), "yyyyMMdd").cast("int")
    ).withColumn(
        "time_key", hour(col(ts_column)) * 10000 + minute(col(ts_column)) * 100
    )


def _build_fact_trip(spark: SparkSession, start: date, end: date) -> None:
    trips = _read_staging(spark, "staging.fact_trip").filter(
        (col("tpep_pickup_datetime") >= start.isoformat())
        & (col("tpep_pickup_datetime") < (end + timedelta(days=1)).isoformat())
    )

    # fact_trip carries the TLC LocationID business key. dim_location is SCD2,
    # so a zone may have several rows; we only need the set of valid business
    # keys to filter out unknown locations.
    known_locations = [
        row.location_id
        for row in _read_staging(spark, "dwh.dim_location")
        .select("location_id")
        .distinct()
        .collect()
    ]

    fact_trip = (
        _with_date_time_keys(trips, "tpep_pickup_datetime")
        .withColumn(
            "trip_duration_sec",
            (
                unix_timestamp(col("tpep_dropoff_datetime"))
                - unix_timestamp(col("tpep_pickup_datetime"))
            ).cast("int"),
        )
        .filter(col("trip_duration_sec") > 0)
        .withColumn(
            "pu_location_key",
            when(col("pu_location_id").isin(known_locations), col("pu_location_id")),
        )
        .withColumn(
            "do_location_key",
            when(col("do_location_id").isin(known_locations), col("do_location_id")),
        )
        .select(
            "date_key",
            "time_key",
            "pu_location_key",
            "do_location_key",
            "trip_distance",
            "fare_amount",
            "tip_amount",
            "total_amount",
            "trip_duration_sec",
            col("passenger_count").cast("int").alias("passenger_count"),
        )
    )

    written = fact_trip.count()
    fact_trip.write.jdbc(
        url=JDBC_URL, table="dwh.fact_trip", mode="append", properties=JDBC_PROPERTIES
    )
    logger.info("wrote dwh.fact_trip", extra={"rows": written})


def _build_fact_weather(spark: SparkSession, start: date, end: date) -> None:
    weather = (
        _read_staging(spark, "staging.fact_weather")
        .filter(
            (col("time") >= start.isoformat())
            & (col("time") < (end + timedelta(days=1)).isoformat())
        )
        .withColumn("hour_ts", date_trunc("hour", col("time")))
    )
    keyed = _with_date_time_keys(weather, "hour_ts")

    hourly = keyed.groupBy("date_key", "time_key").agg(
        avg("temperature_2m").alias("temperature"),
        sum("precipitation").alias("precipitation"),
        avg("windspeed_10m").alias("wind_speed"),
    )

    # Dominant weather code per hour: highest observation count, ties broken
    # by the lower (less severe) code for determinism.
    code_counts = keyed.groupBy("date_key", "time_key", "weathercode").agg(
        count("*").alias("code_count")
    )
    code_window = Window.partitionBy("date_key", "time_key").orderBy(
        col("code_count").desc(), col("weathercode").asc()
    )
    dominant = (
        code_counts.withColumn("rn", row_number().over(code_window))
        .filter(col("rn") == 1)
        .select("date_key", "time_key", col("weathercode").alias("weather_type_key"))
    )

    fact_weather = hourly.join(dominant, ["date_key", "time_key"]).select(
        "date_key",
        "time_key",
        "weather_type_key",
        "temperature",
        "precipitation",
        "wind_speed",
    )

    written = fact_weather.count()
    fact_weather.write.jdbc(
        url=JDBC_URL,
        table="dwh.fact_weather",
        mode="append",
        properties=JDBC_PROPERTIES,
    )
    logger.info("wrote dwh.fact_weather", extra={"rows": written})


def run_pipeline(
    spark: SparkSession,
    year: int,
    months: list[int],
    simulate_scd2: bool = True,
) -> None:
    """Load dimensions, then both fact tables for the given period.

    Assumes prepare_load() already cleared the dwh targets for this period,
    so fact writes are plain appends.

    dim_location is maintained as an SCD2 dimension. Because the real TLC zone
    lookup is static, synthetic service_zone changes (effective on/before the
    period end) are injected when simulate_scd2 is True, so SCD2 versioning is
    exercised end-to-end. Changes take effect from the period start.
    """
    start, end = period_bounds(year, months)

    load_dim_date(start, end)

    overrides = (
        overrides_effective_on(planned_changes(year), end) if simulate_scd2 else None
    )
    load_dim_location(effective_date=start, service_zone_overrides=overrides)

    _build_fact_trip(spark, start, end)
    _build_fact_weather(spark, start, end)

    trip_rows = fetch_one("SELECT COUNT(*) FROM dwh.fact_trip")[0]
    weather_rows = fetch_one("SELECT COUNT(*) FROM dwh.fact_weather")[0]
    logger.info(
        "Pipeline finished",
        extra={"fact_trip_total": trip_rows, "fact_weather_total": weather_rows},
    )
