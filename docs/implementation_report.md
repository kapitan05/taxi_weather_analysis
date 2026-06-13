# Implementation Report — task.md Coverage

Maps each requirement from `task.md` to its implementation in this repository.
Verified end-to-end on 2026-06-11 (January 2023, Docker Compose stack).

## Status overview

| task.md requirement | Status | Implementation |
|---|---|---|
| Implementacja modelu hurtowni danych | ✅ Done | `src/db/schema.sql` |
| Moduł pozyskiwania i przetwarzania danych (ETL/ELT) | ✅ Done | `src/ingest/`, `src/transform/`, `main.py` |
| Analiza jakości danych | ✅ Done | `src/quality/checks.py` |
| Warstwa OLAP i BI (model, hierarchie, metryki, KPIs) | 🟡 Partial | DWH model + hierarchies ready for Tableau (`README.md` → *BI layer*); Tableau workbook not yet in repo |
| Finalne raporty biznesowe | ⏳ Pending | To be built in Tableau on top of `dwh.*` |
| Zestaw testów funkcjonalnych | 🟡 Partial | Load scenarios + consistency verified by executable quality checks and documented runs (below); automated pytest suite pending |
| Prezentacja rezultatów | ⏳ Pending | Outside repo scope |
| Dokument STTM | ⏳ Pending | — |

## 1. Model hurtowni danych

Fact constellation (model konstelacji faktów) in PostgreSQL, schema `dwh`,
defined in idempotent DDL applied by the `db-init` compose service
(`docker-compose.yml`).

| Table | DDL | Content |
|---|---|---|
| `dwh.dim_date` | `src/db/schema.sql:61` | day grain, `date_key` = YYYYMMDD, US/NY holiday flag + name |
| `dwh.dim_time` | `src/db/schema.sql:71` | minute grain, `time_key` = HHMMSS, time-of-day bucket |
| `dwh.dim_location` | `src/db/schema.sql:78` | TLC taxi zones: borough / zone / service zone |
| `dwh.dim_weather_type` | `src/db/schema.sql:86` | WMO weather codes (Clear, Rain, Snow, …) |
| `dwh.fact_trip` | `src/db/schema.sql:92` | trip grain: distance, fare, tip, total, duration, passengers |
| `dwh.fact_weather` | `src/db/schema.sql:106` | hour grain: temperature, precipitation, wind speed, dominant weather type |

- Real PK/FK constraints + indexes on all fact foreign keys (`src/db/schema.sql:117–121`)
- Static dimension seeds in DDL: `dim_time` 1440 rows (`schema.sql:128`), `dim_weather_type` 28 WMO codes (`schema.sql:143`)
- Raw landing zone kept separate in schema `staging` (`schema.sql:15,39,49`)

## 2. Proces ETL/ELT

Orchestrated by `main.py` (`main.py:43`), runs inside the `etl-runner` container.

**Ekstrakcja** (`src/ingest/`):
- TLC Parquet download + read — `ingest_tlc_data()`, `src/ingest/taxi_ingest.py:49`
- Open-Meteo 15-minute observations incl. wind speed — `ingest_weather_data()`, `src/ingest/weather_ingest.py:53` (endpoint at `weather_ingest.py:18`)
- TLC taxi zone lookup CSV — `ingest_zone_lookup()`, `src/ingest/zone_ingest.py:15`

**Transformacja:**
- Cleaning: drop trips with non-positive `total_amount`/`trip_distance`/`passenger_count`, out-of-month pickups, exact duplicates — `src/ingest/taxi_ingest.py:72–87`
- Source→staging column standardisation — `COLUMN_RENAMES`, `src/ingest/taxi_ingest.py:18`
- `date_key`/`time_key` derivation from timestamps — `_with_date_time_keys()`, `src/transform/pipeline.py:71`
- Trip duration in seconds — `src/transform/pipeline.py:79` (`_build_fact_trip`)
- 15-min → hourly weather aggregation with dominant weather code — `_build_fact_weather()`, `src/transform/pipeline.py:131`
- Dimension loading: `dim_date` with `holidays` (US/NY) — `load_dim_date()`, `src/transform/dimensions.py:16`; `dim_location` upsert — `dimensions.py:54`

**Scenariusze ładowania** (`--mode`, `main.py:35`):
- `init` — truncate staging + facts, full rebuild
- `incremental` — delete only the loaded period, then append (`prepare_load()`, `src/transform/pipeline.py:34`); idempotent, safe to re-run

## 3. Analiza jakości danych

`src/quality/checks.py`, run via `uv run python -m src.quality.checks`.

- 19 checks: row counts, null/range checks on measures, referential integrity facts↔dims, duplicate detection, staging↔dwh consistency (`run_checks()`, `checks.py:102`; SQL catalogue at `checks.py:38`)
- Output: `reports/quality_report.md` + `.json` (`write_reports()`, `checks.py:143`); non-zero exit on failure

Findings from the January 2023 run (handled in cleaning): 84 stray
out-of-month pickups and 1 exact-duplicate trip pair in the source file.

## 4. Potwierdzenie działania (wyniki weryfikacji, 2026-06-11)

| Test | Wynik |
|---|---|
| Init load 2023-01 | 3 066 766 raw → 2 884 355 rows in `dwh.fact_trip`; 2 976 15-min obs → 744 hourly rows in `dwh.fact_weather` (31×24 ✓) |
| Incremental re-load same period | identical totals — no duplicates |
| Quality checks | 19/19 PASS, staging↔dwh consistency exact (2 884 355 = 2 884 355) |
| Schema re-apply | idempotent, 0 errors |

## 5. Pozostałe do zrobienia

- Tableau workbook + 6 business reports (report.pdf §7) on `dwh.*`
- Automated pytest suite (unit: transforms; integration: load scenarios against ephemeral Postgres)
- STTM document, final PDF report, test-evidence write-ups (cel / kroki / oczekiwany wynik / potwierdzenie)
