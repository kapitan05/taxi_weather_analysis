"""Data quality analysis for the staging and dwh layers.

Run after a load:
    uv run python -m src.quality.checks

Writes reports/quality_report.md and reports/quality_report.json;
exits non-zero if any check fails.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel

from src.db.connection import fetch_one
from src.ingest.logging_config import setup_json_logging

logger = logging.getLogger(__name__)

REPORT_DIR = Path("reports")

ROW_COUNT_TABLES = [
    "staging.fact_trip",
    "staging.fact_weather",
    "staging.zone_lookup",
    "dwh.dim_date",
    "dwh.dim_time",
    "dwh.dim_location",
    "dwh.dim_weather_type",
    "dwh.fact_trip",
    "dwh.fact_weather",
]

# name -> SQL returning a single violation count (expected 0)
ZERO_EXPECTED_CHECKS: dict[str, str] = {
    "fact_trip: null keys/measures": """
        SELECT COUNT(*) FROM dwh.fact_trip
        WHERE date_key IS NULL OR time_key IS NULL
           OR trip_distance IS NULL OR total_amount IS NULL
           OR trip_duration_sec IS NULL OR passenger_count IS NULL
    """,
    "fact_trip: non-positive measures": """
        SELECT COUNT(*) FROM dwh.fact_trip
        WHERE trip_distance <= 0 OR total_amount <= 0
           OR trip_duration_sec <= 0 OR passenger_count <= 0
    """,
    "fact_trip: orphaned date_key": """
        SELECT COUNT(*) FROM dwh.fact_trip f
        LEFT JOIN dwh.dim_date d ON f.date_key = d.date_key
        WHERE d.date_key IS NULL
    """,
    # dim_location is SCD2 (no DB FK); RI is enforced here against the live
    # version of each zone (location_id + is_current).
    "fact_trip: orphaned pickup location": """
        SELECT COUNT(*) FROM dwh.fact_trip f
        LEFT JOIN dwh.dim_location l
               ON f.pu_location_key = l.location_id AND l.is_current
        WHERE f.pu_location_key IS NOT NULL AND l.location_id IS NULL
    """,
    "fact_trip: orphaned dropoff location": """
        SELECT COUNT(*) FROM dwh.fact_trip f
        LEFT JOIN dwh.dim_location l
               ON f.do_location_key = l.location_id AND l.is_current
        WHERE f.do_location_key IS NOT NULL AND l.location_id IS NULL
    """,
    "dim_location: >1 current row per zone": """
        SELECT COUNT(*) FROM (
            SELECT location_id FROM dwh.dim_location
            WHERE is_current GROUP BY location_id HAVING COUNT(*) > 1
        ) d
    """,
    "dim_location: current row not open-ended": """
        SELECT COUNT(*) FROM dwh.dim_location
        WHERE is_current AND valid_to <> DATE '9999-12-31'
    """,
    "dim_location: overlapping validity intervals": """
        SELECT COUNT(*) FROM dwh.dim_location a
        JOIN dwh.dim_location b
          ON a.location_id = b.location_id
         AND a.location_sk <> b.location_sk
         AND a.valid_from <= b.valid_to
         AND b.valid_from <= a.valid_to
    """,
    "dim_location: valid_from after valid_to": """
        SELECT COUNT(*) FROM dwh.dim_location WHERE valid_from > valid_to
    """,
    "fact_weather: orphaned weather type": """
        SELECT COUNT(*) FROM dwh.fact_weather f
        LEFT JOIN dwh.dim_weather_type t
               ON f.weather_type_key = t.weather_type_key
        WHERE t.weather_type_key IS NULL
    """,
    "fact_weather: duplicate hours": """
        SELECT COUNT(*) FROM (
            SELECT date_key, time_key FROM dwh.fact_weather
            GROUP BY date_key, time_key HAVING COUNT(*) > 1
        ) d
    """,
    "fact_weather: temperature out of range [-40, 50]": """
        SELECT COUNT(*) FROM dwh.fact_weather
        WHERE temperature NOT BETWEEN -40 AND 50
    """,
    "fact_weather: negative precipitation or wind": """
        SELECT COUNT(*) FROM dwh.fact_weather
        WHERE precipitation < 0 OR wind_speed < 0
    """,
    "staging.fact_trip: duplicate raw trips": """
        SELECT COUNT(*) FROM (
            SELECT 1 FROM staging.fact_trip
            GROUP BY vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime,
                     passenger_count, trip_distance, rate_code_id,
                     store_and_fwd_flag, pu_location_id, do_location_id,
                     payment_type, fare_amount, extra, mta_tax, tip_amount,
                     tolls_amount, improvement_surcharge, total_amount,
                     congestion_surcharge, airport_fee
            HAVING COUNT(*) > 1
        ) d
    """,
}


class CheckResult(BaseModel):
    name: str
    passed: bool
    value: int
    expected: str


def run_checks() -> list[CheckResult]:
    results: list[CheckResult] = []

    for table in ROW_COUNT_TABLES:
        rows = int(fetch_one(f"SELECT COUNT(*) FROM {table}")[0])
        results.append(
            CheckResult(
                name=f"row count: {table}",
                passed=rows > 0,
                value=rows,
                expected="> 0",
            )
        )

    for name, sql in ZERO_EXPECTED_CHECKS.items():
        violations = int(fetch_one(sql)[0])
        results.append(
            CheckResult(name=name, passed=violations == 0, value=violations, expected="0")
        )

    # Consistency staging -> dwh: every staged trip with a positive duration
    # must land in fact_trip exactly once.
    staged = int(
        fetch_one(
            "SELECT COUNT(*) FROM staging.fact_trip "
            "WHERE tpep_dropoff_datetime > tpep_pickup_datetime"
        )[0]
    )
    loaded = int(fetch_one("SELECT COUNT(*) FROM dwh.fact_trip")[0])
    results.append(
        CheckResult(
            name="consistency: staging trips (duration > 0) = dwh.fact_trip",
            passed=staged == loaded,
            value=loaded - staged,
            expected=f"0 (staging={staged}, dwh={loaded})",
        )
    )

    return results


def write_reports(results: list[CheckResult]) -> None:
    REPORT_DIR.mkdir(exist_ok=True)
    generated_at = datetime.now(timezone.utc).isoformat()

    json_payload = {
        "generated_at": generated_at,
        "passed": all(r.passed for r in results),
        "checks": [r.model_dump() for r in results],
    }
    (REPORT_DIR / "quality_report.json").write_text(json.dumps(json_payload, indent=2))

    lines = [
        "# Data Quality Report",
        "",
        f"Generated: {generated_at}",
        "",
        "| Check | Result | Value | Expected |",
        "|-------|--------|-------|----------|",
    ]
    lines += [
        f"| {r.name} | {'PASS' if r.passed else 'FAIL'} | {r.value} | {r.expected} |"
        for r in results
    ]
    (REPORT_DIR / "quality_report.md").write_text("\n".join(lines) + "\n")


def main() -> None:
    setup_json_logging()
    results = run_checks()
    write_reports(results)

    failed = [r for r in results if not r.passed]
    logger.info(
        "Quality checks finished",
        extra={"total": len(results), "failed": len(failed)},
    )
    for r in failed:
        logger.error("Check failed", extra={"check": r.name, "value": r.value})
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
