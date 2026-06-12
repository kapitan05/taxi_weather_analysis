import os
import re
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg2
from psycopg2.extensions import connection as PgConnection

JDBC_URL = os.getenv("DB_URL", "jdbc:postgresql://postgres-dwh:5432/nyc_weather_taxi")
JDBC_PROPERTIES = {
    "user": os.getenv("DB_USER", "data_engineer"),
    "password": os.getenv("DB_PASSWORD", "password123"),
    "driver": "org.postgresql.Driver",
}


def _parse_jdbc_url(url: str) -> tuple[str, int, str]:
    match = re.match(r"jdbc:postgresql://([^:/]+)(?::(\d+))?/(\w+)", url)
    if not match:
        raise ValueError(f"Cannot parse JDBC URL: {url!r}")
    return match.group(1), int(match.group(2) or 5432), match.group(3)


@contextmanager
def pg_conn() -> Iterator[PgConnection]:
    host, port, dbname = _parse_jdbc_url(JDBC_URL)
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=JDBC_PROPERTIES["user"],
        password=JDBC_PROPERTIES["password"],
    )
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def execute(sql: str, params: tuple | None = None) -> None:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)


def fetch_one(sql: str, params: tuple | None = None) -> tuple:
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Query returned no rows: {sql}")
        return row
