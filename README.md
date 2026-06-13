# NYC Weather vs Taxi Trips — Data Warehouse

An end-to-end data warehouse project integrating NYC yellow taxi trip records (TLC Trip Record Data) with historical weather data (Open-Meteo). Data is loaded by a PySpark ETL into a PostgreSQL warehouse modelled as a **fact constellation**, and analysed in **Tableau**.

**Data sources:** NYC TLC yellow taxi trip records (Parquet) · TLC taxi zone lookup (CSV) · Open-Meteo historical weather API (15-minute JSON)
**Stack:** PySpark · PostgreSQL · Tableau · Docker Compose

## Architecture

```
TLC Parquet ──────┐
TLC zone CSV ─────┼─▶  PySpark ETL  ─▶  PostgreSQL DWH      ─▶  Tableau
Open-Meteo API ───┘     staging.*        dwh.* (constellation)
```

The load runs in two phases:

1. **Ingest** — downloads TLC Parquet, the zone lookup CSV and Open-Meteo 15-minute observations into `staging.*` landing tables
2. **Transform** — builds the constellation in `dwh.*`: shared dimensions and two fact tables

### Warehouse model (fact constellation)

| Table | Grain / content |
|-------|-----------------|
| `dwh.dim_date` | One row per day; `date_key` = YYYYMMDD; US/NY holiday flag + name |
| `dwh.dim_time` | One row per minute; `time_key` = HHMMSS; time-of-day bucket |
| `dwh.dim_location` | TLC taxi zones (borough → zone → service zone) |
| `dwh.dim_weather_type` | WMO weather codes (Clear, Rain, Snow, …) |
| `dwh.fact_trip` | One row per trip: distance, fare, tip, total, duration, passengers |
| `dwh.fact_weather` | One row per hour (aggregated from 15-min data): temperature, precipitation, wind speed, dominant weather type |

`dim_time` and `dim_weather_type` are seeded statically by `src/db/schema.sql`; `dim_date` and `dim_location` are maintained by the pipeline.

## Quick start

```bash
docker compose up -d
```

PostgreSQL is available on `localhost:5432` (db `nyc_weather_taxi`).

## Running the ETL

With the stack running, execute the load inside the ETL container:

```bash
# Initial (full) load
docker compose exec etl-runner uv run python main.py \
  --year 2023 --start-month 1 --end-month 1 --mode init

# Next iteration — reloads only the given period, idempotent
docker compose exec etl-runner uv run python main.py \
  --year 2023 --start-month 2 --end-month 2 --mode incremental
```

| Flag | Default | Description |
|------|---------|-------------|
| `--year` | `2023` | Calendar year to load |
| `--start-month` | `1` | First month (inclusive) |
| `--end-month` | `1` | Last month (inclusive) |
| `--mode` | `init` | `init`: truncate + full rebuild; `incremental`: delete + reload only the period |

Both modes are safe to re-run: the target period is cleared in `staging` and `dwh` before loading, so no duplicates are produced.

## Data quality analysis

After a load, run the quality checks (row counts, null/range checks, referential integrity, staging↔dwh consistency):

```bash
docker compose exec etl-runner uv run python -m src.quality.checks
```

Results are written to `reports/quality_report.md` and `reports/quality_report.json`; the command exits non-zero if any check fails.

## BI layer (Tableau)

Connect Tableau to PostgreSQL:

- **Server/port:** `localhost:5432` · **Database:** `nyc_weather_taxi` · **Schema:** `dwh`
- Join facts to dimensions on `date_key`, `time_key`, `*_location_key`, `weather_type_key`
- Hierarchies: Date (Year → Month → Day), Time (Time of day → Hour → Minute), Location (Borough → Zone → Service zone)

### Fresh start / reset

```bash
docker compose down -v   # removes containers and pgdata volume
docker compose up -d
```

## Project layout

```
src/
  ingest/      PySpark ingestion — TLC Parquet, zone lookup CSV, Open-Meteo 15-min → staging.*
  transform/   Dimension + fact builders (constellation), incremental load control
  quality/     Data quality checks → reports/quality_report.{md,json}
  db/          schema.sql — idempotent DDL + static dim seeds; psycopg2 connection helpers
docker/
  Dockerfile.etl   python:3.12-slim + JRE + uv
```
