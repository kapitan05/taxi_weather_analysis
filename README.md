# NYC Weather vs Taxi Trips — Data Warehouse

An end-to-end data warehouse project integrating NYC yellow taxi trip records (TLC Trip Record Data) with historical weather data (Open-Meteo). Data is loaded by a PySpark ETL into a PostgreSQL warehouse modelled as a **fact constellation**.

**Data sources:** NYC TLC yellow taxi trip records (Parquet) · TLC taxi zone lookup (CSV) · Open-Meteo historical weather API (15-minute JSON)
**Stack:** PySpark · PostgreSQL · Metabase · Docker Compose

## Architecture

```
TLC Parquet ──────┐
TLC zone CSV ─────┼─▶  PySpark ETL  ─▶  PostgreSQL DWH      ─▶  Metabase
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
| `dwh.dim_location` | TLC taxi zones (borough → zone → service zone), **SCD2** — versioned history of `service_zone` |
| `dwh.dim_weather_type` | WMO weather codes (Clear, Rain, Snow, …) |
| `dwh.fact_trip` | One row per trip: distance, fare, tip, total, duration, passengers |
| `dwh.fact_weather` | One row per hour (aggregated from 15-min data): temperature, precipitation, wind speed, dominant weather type |

`dim_time` and `dim_weather_type` are seeded statically by `src/db/schema.sql`; `dim_date` and `dim_location` are maintained by the pipeline.

### SCD2 on `dim_location`

`dim_location` is a slowly-changing dimension of type 2: each row is one *version* of a taxi zone, keyed by a surrogate `location_sk`, with the TLC `location_id` as the stable business key plus `valid_from` / `valid_to` / `is_current` / `version`. When a zone's `service_zone` (or borough/zone) changes, the current row is closed (`valid_to` set, `is_current = false`) and a new current version is inserted — so the full history is retained.

The real TLC zone lookup is static, so `src/transform/scd_simulation.py` injects deterministic, clearly-synthetic `service_zone` reclassifications (effective mid-year) to exercise the SCD2 behaviour; pass `simulate_scd2=False` to `run_pipeline` to disable. `fact_trip` stores the `location_id` business key (no DB FK, since `location_id` is non-unique across versions); queries resolve the live version via `pu_location_key = dim_location.location_id AND is_current`, and `src/quality/checks.py` enforces referential integrity plus SCD2 invariants (one current row per zone, no overlapping intervals, open-ended current row).

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

### Reporting layer

```bash
docker compose exec etl-runner uv run python -m src.reporting.render
```


### Metabase (interactive BI dashboard)

Metabase runs as part of the stack (`docker compose up -d`) on **http://localhost:3000**. After a data load and once the `rpt` views exist, auto-provision the connection, six questions and a dashboard:

```bash
docker compose exec etl-runner uv run python -m src.reporting.metabase_setup
```

Then open http://localhost:3000 and log in with **admin@nyc-taxi.local / Metabase123!** — the **NYC Taxi & Weather** dashboard is under *Dashboards*. 

### Tests

```bash
uv run pytest          # pure unit tests: SCD2 diff, simulation, reporting shaping
```

### Fresh start / reset

```bash
docker compose down -v   # removes containers and pgdata volume
docker compose up -d
```

## Project layout

```
src/
  ingest/      PySpark ingestion — TLC Parquet, zone lookup CSV, Open-Meteo 15-min → staging.*
  transform/   Dimension + fact builders (constellation), SCD2 dim_location + simulation, incremental load control
  reporting/   rpt views (views.sql) + headless report rendering (render.py)
  quality/     Data quality checks → reports/quality_report.{md,json}
  db/          schema.sql — idempotent DDL + static dim seeds; psycopg2 connection helpers
tests/         Pure unit tests (SCD2 diff, simulation, reporting shaping)
docker/
  Dockerfile.etl   python:3.12-slim + JRE + uv
```
