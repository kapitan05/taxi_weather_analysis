# Docker Compose Environment

> **Note:** the production compose file has been removed. There is one environment: `docker-compose.yml`.


## Files

| File | Purpose |
|---|---|
| `docker-compose.yml` | Local development |
| `docker-compose.prod.yml` | Production deployment |

Both files define the same three services (`postgres-dwh`, `db-init`, `etl-runner`) and apply the same schema (`src/db/schema.sql`). The differences are in credentials, restart behaviour, code mounting, and healthcheck cadence.

---

## Differences

### 1. Credentials

**Dev** — hardcoded in the compose file, no `.env` required:
```yaml
# docker-compose.yml
environment:
  POSTGRES_USER: data_engineer
  POSTGRES_PASSWORD: password123
  POSTGRES_DB: nyc_weather_taxi
```

**Prod** — all secrets come from a `.env` file (copy from `.env.example`):
```yaml
# docker-compose.prod.yml
environment:
  POSTGRES_USER: ${POSTGRES_USER}
  POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
  POSTGRES_DB: ${POSTGRES_DB}
```
The ETL container also receives its JDBC connection via env vars in prod (`DB_URL`, `DB_USER`, `DB_PASSWORD`), whereas in dev those fall back to the hardcoded defaults in `src/db/connection.py:JDBC_PROPERTIES`.

---

### 2. Source-code mounting (hot-reload vs baked image)

**Dev** — `src/` and `main.py` are volume-mounted into the container:
```yaml
# docker-compose.yml  etl-runner
volumes:
  - ./src:/app/src
  - ./main.py:/app/main.py
```
Any change to Python files on the host is picked up immediately without rebuilding the image. This is the reason the column-rename fix (`src/ingest/taxi_ingest.py`) and the weather-endpoint fix (`src/ingest/weather_ingest.py`) took effect with just a re-`exec` during development.

**Prod** — no volume mount on `etl-runner`. Code is baked into the image at build time (`COPY src/ ./src/` in `docker/Dockerfile.etl`). To deploy a code change you must rebuild: `docker compose -f docker-compose.prod.yml build etl-runner`.

---

### 3. Restart policy

**Dev** — no restart policy. Services stay down if they crash (so you notice failures).

**Prod** — `postgres-dwh` has `restart: always`, keeping the warehouse available across host reboots and transient crashes.

```yaml
# docker-compose.prod.yml  postgres-dwh
restart: always
```

The ETL runner intentionally has no restart policy in either environment — it is a one-shot job triggered manually, not a long-running daemon.

---

### 4. Healthcheck cadence

**Dev** — faster polling for a quicker startup experience:
```yaml
interval: 5s
timeout: 5s
retries: 10
```

**Prod** — slower polling, less overhead on a running server:
```yaml
interval: 10s
timeout: 5s
retries: 10
```

---

### 5. Named volumes and networks

**Dev** uses `pgdata` / `data-network`.
**Prod** uses `pgdata_prod` / `prod-network`.

These are distinct so that `docker compose down -v` in dev cannot accidentally wipe prod data even if both stacks were running on the same host. Reset prod intentionally with:
```bash
docker compose -f docker-compose.prod.yml down -v
```

---

## Usage

```bash
# Dev
docker compose up -d
docker compose exec etl-runner uv run python main.py --year 2023 --start-month 1 --end-month 1 --mode init

# Prod
cp .env.example .env   # fill in real credentials
docker compose -f docker-compose.prod.yml up -d --build
docker compose -f docker-compose.prod.yml exec etl-runner \
  uv run python main.py --year 2023 --start-month 1 --end-month 1 --mode init
```

Tableau connects to `localhost:5432` in both environments (port exposed in both compose files). In a cloud/VM prod deployment, restrict the exposed port to the Tableau host via firewall rules rather than removing the port binding.
