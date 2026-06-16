
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

SOURCE_URL = os.getenv(
    "GEOJSON_SOURCE_URL",
    "https://raw.githubusercontent.com/chkp-fernandom/nyc-taxi-map/master/data/zones.geojson",
)
OUT = Path(os.getenv("GEOJSON_OUT", "geo/taxi_zones.geojson"))


def prepare() -> Path:
    logger.info("Downloading taxi-zone GeoJSON", extra={"url": SOURCE_URL})
    resp = requests.get(SOURCE_URL, timeout=120)
    resp.raise_for_status()
    gj = resp.json()

    if gj.get("type") != "FeatureCollection":
        gj = {"type": "FeatureCollection", "features": gj["features"]}
    if not gj.get("features"):
        raise ValueError("Downloaded GeoJSON contains no features")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(gj), encoding="utf-8")
    logger.info(
        "Wrote repaired GeoJSON",
        extra={"path": str(OUT), "features": len(gj["features"])},
    )
    return OUT


def main() -> None:
    from src.ingest.logging_config import setup_json_logging

    setup_json_logging()
    path = prepare()
    print(
        f"GeoJSON ready at {path}. Inside the stack Metabase can load it from "
        f"http://geojson/taxi_zones.geojson (host: http://localhost:8088/taxi_zones.geojson)."
    )


if __name__ == "__main__":
    main()
