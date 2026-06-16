from __future__ import annotations

import logging
import os
import time

import requests

logger = logging.getLogger(__name__)

METABASE_URL = os.getenv("METABASE_URL", "http://metabase:3000").rstrip("/")
DWH_HOST = os.getenv("DWH_HOST", "postgres-dwh")
DWH_PORT = int(os.getenv("DWH_PORT", "5432"))
DWH_DB = os.getenv("DWH_DB", "nyc_weather_taxi")
DWH_USER = os.getenv("DWH_USER", "data_engineer")
DWH_PASS = os.getenv("DWH_PASS", "password123")

ADMIN_EMAIL = os.getenv("MB_ADMIN_EMAIL", "admin@nyc-taxi.local")
ADMIN_PASSWORD = os.getenv("MB_ADMIN_PASSWORD", "Metabase123!")

DB_NAME = "NYC Weather Taxi DWH"
DASHBOARD_NAME = "NYC Taxi & Weather"

GEOJSON_MAP_KEY = "nyc_taxi_zones"
GEOJSON_MAP_NAME = "NYC Taxi Zones"
GEOJSON_MAP_URL = os.getenv("GEOJSON_MAP_URL", "http://geojson/taxi_zones.geojson")
GEOJSON_REGION_KEY = "LocationID"   
GEOJSON_REGION_NAME = "zone"
MAP_CARD_NAME = "7. Trips by pickup zone (map)"
MAP_CARD_SQL = "SELECT location_id::text AS location_id, trip_count FROM rpt.v_zone_trips"

CARDS: dict[str, dict[str, str]] = {
    "1. Trips by precipitation band": dict(
        display="bar", dim="precip_band", metric="avg_trips_per_day",
        sql="SELECT precip_band, AVG(trip_count) AS avg_trips_per_day "
            "FROM rpt.v_precip_trips GROUP BY precip_band "
            "ORDER BY CASE precip_band WHEN 'Dry' THEN 1 WHEN 'Light' THEN 2 "
            "WHEN 'Moderate' THEN 3 ELSE 4 END",
    ),
    "2. Avg duration vs temperature": dict(
        display="line", dim="temperature_c", metric="avg_duration_min",
        sql="SELECT ROUND(avg_temperature::numeric, 0) AS temperature_c, "
            "AVG(avg_duration_min) AS avg_duration_min FROM rpt.v_temp_duration "
            "GROUP BY 1 ORDER BY 1",
    ),
    "3. Monthly trip trend": dict(
        display="line", dim="month", metric="trip_count",
        sql="SELECT make_date(year, month, 1) AS month, trip_count "
            "FROM rpt.v_monthly_trips ORDER BY year, month",
    ),
    "4. Trips by pickup borough": dict(
        display="bar", dim="pickup_borough", metric="trips",
        sql="SELECT pickup_borough, SUM(trip_count) AS trips "
            "FROM rpt.v_daily_kpi GROUP BY pickup_borough ORDER BY trips DESC",
    ),
    "5. Seasonal comparison": dict(
        display="bar", dim="season", metric="trip_count",
        sql="SELECT season, trip_count FROM rpt.v_seasonal "
            "ORDER BY CASE season WHEN 'Winter' THEN 1 WHEN 'Spring' THEN 2 "
            "WHEN 'Summer' THEN 3 ELSE 4 END",
    ),
    "6. Hourly average temperature": dict(
        display="line", dim="hour", metric="avg_temperature",
        sql="SELECT hour, AVG(temperature) AS avg_temperature "
            "FROM rpt.v_hourly_weather GROUP BY hour ORDER BY hour",
    ),
}


class Metabase:
    def __init__(self, base_url: str):
        self.base = base_url
        self.s = requests.Session()

    def wait_until_up(self, timeout: int = 180) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                r = self.s.get(f"{self.base}/api/health", timeout=5)
                if r.ok:
                    return
            except requests.RequestException:
                pass
            time.sleep(3)
        raise RuntimeError(f"Metabase not reachable at {self.base}")

    def _props(self) -> dict:
        return self.s.get(f"{self.base}/api/session/properties", timeout=15).json()

    def _login(self) -> str:
        logger.info("Logging in to Metabase")
        r = self.s.post(
            f"{self.base}/api/session",
            json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["id"]

    def authenticate(self) -> None:
        props = self._props()
        token = props.get("setup-token")
        if props.get("has-user-setup") or not token:
            session = self._login()
        else:
            logger.info("Running first-time Metabase setup")
            r = self.s.post(
                f"{self.base}/api/setup",
                json={
                    "token": token,
                    "user": {
                        "first_name": "Admin",
                        "last_name": "User",
                        "email": ADMIN_EMAIL,
                        "password": ADMIN_PASSWORD,
                        "site_name": "NYC Taxi DWH",
                    },
                    "prefs": {"site_name": "NYC Taxi DWH", "allow_tracking": False},
                },
                timeout=30,
            )
            if r.status_code == 403:
                session = self._login()
            else:
                r.raise_for_status()
                session = r.json()["id"]
        self.s.headers.update({"X-Metabase-Session": session})

    def get_or_create_database(self) -> int:
        existing = self.s.get(f"{self.base}/api/database", timeout=30).json()
        items = existing.get("data", existing) if isinstance(existing, dict) else existing
        for db in items:
            if db.get("name") == DB_NAME:
                logger.info("Reusing Metabase database connection", extra={"id": db["id"]})
                return db["id"]
        r = self.s.post(
            f"{self.base}/api/database",
            json={
                "engine": "postgres",
                "name": DB_NAME,
                "details": {
                    "host": DWH_HOST,
                    "port": DWH_PORT,
                    "dbname": DWH_DB,
                    "user": DWH_USER,
                    "password": DWH_PASS,
                    "ssl": False,
                    "tunnel-enabled": False,
                },
                "is_full_sync": True,
            },
            timeout=60,
        )
        r.raise_for_status()
        db_id = r.json()["id"]
        logger.info("Created Metabase database connection", extra={"id": db_id})
        self.s.post(f"{self.base}/api/database/{db_id}/sync_schema", timeout=30)
        return db_id

    def existing_card_ids(self) -> dict[str, int]:
        cards = self.s.get(f"{self.base}/api/card", timeout=30).json()
        return {c["name"]: c["id"] for c in cards}

    def upsert_card(
        self,
        name: str,
        display: str,
        sql: str,
        viz: dict,
        db_id: int,
        existing: dict[str, int],
    ) -> int:
        payload = {
            "name": name,
            "dataset_query": {
                "type": "native",
                "native": {"query": sql},
                "database": db_id,
            },
            "display": display,
            "visualization_settings": viz,
        }
        if name in existing:
            r = self.s.put(
                f"{self.base}/api/card/{existing[name]}", json=payload, timeout=60
            )
            r.raise_for_status()
            return existing[name]
        r = self.s.post(f"{self.base}/api/card", json=payload, timeout=60)
        r.raise_for_status()
        return r.json()["id"]

    def register_geojson_map(self) -> None:
        try:
            current = self.s.get(
                f"{self.base}/api/setting/custom-geojson", timeout=30
            ).json()
        except Exception:
            current = None
        maps = current if isinstance(current, dict) else {}
        maps[GEOJSON_MAP_KEY] = {
            "name": GEOJSON_MAP_NAME,
            "url": GEOJSON_MAP_URL,
            "region_key": GEOJSON_REGION_KEY,
            "region_name": GEOJSON_REGION_NAME,
        }
        r = self.s.put(
            f"{self.base}/api/setting/custom-geojson",
            json={"value": maps},
            timeout=60,
        )
        r.raise_for_status()
        logger.info("Registered custom GeoJSON map", extra={"key": GEOJSON_MAP_KEY})

    def get_or_create_dashboard(self) -> int:
        dashboards = self.s.get(f"{self.base}/api/dashboard", timeout=30).json()
        for d in dashboards:
            if d.get("name") == DASHBOARD_NAME:
                return d["id"]
        r = self.s.post(
            f"{self.base}/api/dashboard",
            json={"name": DASHBOARD_NAME, "description": "Auto-provisioned report dashboard"},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()["id"]

    def add_cards_to_dashboard(self, dash_id: int, card_ids: list[int]) -> None:
        dashcards = []
        for i, cid in enumerate(card_ids):
            dashcards.append(
                {
                    "id": -(i + 1), 
                    "card_id": cid,
                    "row": (i // 2) * 5,
                    "col": (i % 2) * 9,
                    "size_x": 9,
                    "size_y": 5,
                    "parameter_mappings": [],
                    "visualization_settings": {},
                }
            )
        r = self.s.put(
            f"{self.base}/api/dashboard/{dash_id}",
            json={"dashcards": dashcards},
            timeout=60,
        )
        if r.ok:
            return
        logger.info("PUT dashcards not accepted; falling back to per-card POST")
        for dc in dashcards:
            self.s.post(
                f"{self.base}/api/dashboard/{dash_id}/cards",
                json={
                    "cardId": dc["card_id"],
                    "row": dc["row"],
                    "col": dc["col"],
                    "size_x": dc["size_x"],
                    "size_y": dc["size_y"],
                },
                timeout=30,
            ).raise_for_status()


def provision() -> str:
    mb = Metabase(METABASE_URL)
    mb.wait_until_up()
    mb.authenticate()
    db_id = mb.get_or_create_database()

    existing = mb.existing_card_ids()
    card_ids: list[int] = []
    for name, c in CARDS.items():
        viz = {"graph.dimensions": [c["dim"]], "graph.metrics": [c["metric"]]}
        cid = mb.upsert_card(name, c["display"], c["sql"], viz, db_id, existing)
        logger.info("Upserted card", extra={"card": name, "id": cid})
        card_ids.append(cid)

    try:
        mb.register_geojson_map()
        map_viz = {
            "map.type": "region",
            "map.region": GEOJSON_MAP_KEY,
            "map.metric_column": "trip_count",
            "map.dimension_column": "location_id",
        }
        mid = mb.upsert_card(MAP_CARD_NAME, "map", MAP_CARD_SQL, map_viz, db_id, existing)
        logger.info("Upserted map card", extra={"card": MAP_CARD_NAME, "id": mid})
        card_ids.append(mid)
    except Exception as exc:  
        logger.warning("Skipped zone map", extra={"error": str(exc)})

    dash_id = mb.get_or_create_dashboard()
    mb.add_cards_to_dashboard(dash_id, card_ids)
    url = f"{METABASE_URL}/dashboard/{dash_id}"
    logger.info("Dashboard ready", extra={"url": url})
    return url


def main() -> None:
    from src.ingest.logging_config import setup_json_logging

    setup_json_logging()
    url = provision()
    print(f"Metabase dashboard provisioned. Open: http://localhost:3000/dashboard "
          f"(login {ADMIN_EMAIL} / {ADMIN_PASSWORD}); API URL was {url}")


if __name__ == "__main__":
    main()
