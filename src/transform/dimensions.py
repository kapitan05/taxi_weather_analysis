import logging
from dataclasses import dataclass
from datetime import date, timedelta

import holidays
from psycopg2.extras import execute_values

from src.db.connection import pg_conn

logger = logging.getLogger(__name__)

SCD2_OPEN_END = date(9999, 12, 31)
SCD2_EPOCH = date(1900, 1, 1)

TRACKED_LOCATION_ATTRS = ("borough", "zone", "service_zone")


def date_key(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def load_dim_date(start: date, end: date) -> int:
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


@dataclass(frozen=True)
class LocationVersion:
    """A current (live) SCD2 row for one taxi zone."""

    location_id: int
    borough: str
    zone: str
    service_zone: str
    version: int = 1


@dataclass(frozen=True)
class Scd2Plan:

    closes: list[tuple[int, date]]
    inserts: list[tuple[int, str, str, str, date, int]]


def compute_scd2_changes(
    current: list[LocationVersion],
    incoming: list[LocationVersion],
    effective_date: date,
) -> Scd2Plan:

    current_by_id = {v.location_id: v for v in current}
    closes: list[tuple[int, date]] = []
    inserts: list[tuple[int, str, str, str, date, int]] = []

    for inc in incoming:
        cur = current_by_id.get(inc.location_id)
        if cur is None:
            inserts.append(
                (
                    inc.location_id,
                    inc.borough,
                    inc.zone,
                    inc.service_zone,
                    SCD2_EPOCH,
                    1,
                )
            )
            continue

        changed = any(
            getattr(cur, attr) != getattr(inc, attr) for attr in TRACKED_LOCATION_ATTRS
        )
        if not changed:
            continue

        closes.append((inc.location_id, effective_date - timedelta(days=1)))
        inserts.append(
            (
                inc.location_id,
                inc.borough,
                inc.zone,
                inc.service_zone,
                effective_date,
                cur.version + 1,
            )
        )

    return Scd2Plan(closes=closes, inserts=inserts)


def _fetch_current_locations() -> list[LocationVersion]:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT location_id, borough, zone, service_zone, version
            FROM dwh.dim_location
            WHERE is_current
            """
        )
        return [
            LocationVersion(
                location_id=r[0],
                borough=r[1],
                zone=r[2],
                service_zone=r[3],
                version=r[4],
            )
            for r in cur.fetchall()
        ]


def _fetch_staged_locations(
    overrides: dict[int, str] | None = None,
) -> list[LocationVersion]:
    
    overrides = overrides or {}
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT
                location_id,
                COALESCE(borough, 'Unknown')      AS borough,
                COALESCE(zone, 'Unknown')         AS zone,
                COALESCE(service_zone, 'Unknown') AS service_zone
            FROM staging.zone_lookup
            WHERE location_id IS NOT NULL
            ORDER BY location_id
            """
        )
        rows = cur.fetchall()
    return [
        LocationVersion(
            location_id=r[0],
            borough=r[1],
            zone=r[2],
            service_zone=overrides.get(r[0], r[3]),
        )
        for r in rows
    ]


def _apply_scd2_plan(plan: Scd2Plan) -> None:
    if plan.closes:
        with pg_conn() as conn, conn.cursor() as cur:
            for location_id, new_valid_to in plan.closes:
                cur.execute(
                    """
                    UPDATE dwh.dim_location
                    SET valid_to = %s, is_current = FALSE
                    WHERE location_id = %s AND is_current
                    """,
                    (new_valid_to, location_id),
                )
    if plan.inserts:
        rows = [
            (loc_id, borough, zone, svc, vfrom, SCD2_OPEN_END, True, version)
            for (loc_id, borough, zone, svc, vfrom, version) in plan.inserts
        ]
        with pg_conn() as conn, conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO dwh.dim_location
                    (location_id, borough, zone, service_zone,
                     valid_from, valid_to, is_current, version)
                VALUES %s
                """,
                rows,
            )


def load_dim_location(
    effective_date: date,
    service_zone_overrides: dict[int, str] | None = None,
) -> Scd2Plan:
   
    current = _fetch_current_locations()
    incoming = _fetch_staged_locations(service_zone_overrides)
    plan = compute_scd2_changes(current, incoming, effective_date)
    _apply_scd2_plan(plan)

    logger.info(
        "dim_location SCD2 merge applied",
        extra={"closed": len(plan.closes), "inserted": len(plan.inserts)},
    )
    return plan
