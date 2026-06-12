import logging
from datetime import date, timedelta

import holidays
from psycopg2.extras import execute_values

from src.db.connection import execute, pg_conn

logger = logging.getLogger(__name__)


def date_key(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def load_dim_date(start: date, end: date) -> int:
    """Upsert one dim_date row per day in [start, end] with US/NY holidays."""
    years = list(range(start.year, end.year + 1))
    ny_holidays = holidays.country_holidays("US", subdiv="NY", years=years)

    rows: list[tuple[int, date, int, int, int, bool, str | None]] = []
    current = start
    while current <= end:
        holiday_name = ny_holidays.get(current)
        rows.append(
            (
                date_key(current),
                current,
                current.year,
                current.month,
                current.day,
                holiday_name is not None,
                holiday_name,
            )
        )
        current += timedelta(days=1)

    with pg_conn() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO dwh.dim_date
                (date_key, full_date, year, month, day, holiday_flag, holiday_name)
            VALUES %s
            ON CONFLICT (date_key) DO NOTHING
            """,
            rows,
        )

    logger.info("dim_date loaded", extra={"days": len(rows)})
    return len(rows)


def load_dim_location() -> None:
    """Upsert dim_location from the staged TLC zone lookup."""
    execute(
        """
        INSERT INTO dwh.dim_location
            (location_key, location_id, borough, zone, service_zone)
        SELECT DISTINCT
            location_id,
            location_id,
            COALESCE(borough, 'Unknown'),
            COALESCE(zone, 'Unknown'),
            COALESCE(service_zone, 'Unknown')
        FROM staging.zone_lookup
        WHERE location_id IS NOT NULL
        ON CONFLICT (location_key) DO NOTHING
        """
    )
    logger.info("dim_location loaded")
