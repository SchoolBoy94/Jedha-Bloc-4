from __future__ import annotations
from dotenv import load_dotenv
import json, os, sys
from typing import Dict, List
import pandas as pd
import requests_cache, openmeteo_requests
from retry_requests import retry
from kafka import KafkaProducer

sys.path.append("/opt/airflow/scripts")   # ← Airflow voit maintenant tes scripts

# from scripts.notifications.notif_quality_air import notify_if_poor


# ─── Config ─────────────────────────────────────────────────────────
from scripts.utils.env_loader import load_env_file

# Charge l'env approprié (.env en Airflow, tests/.env_test en Jenkins si ENV_FILE est défini)
load_env_file()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_AQI    = os.getenv("TOPIC_AQI_RAW") # Rename it to "TOPIC_AQI_CURRENT"
CITIES_FILE  = os.getenv("CITIES")
URL          = os.getenv("API_URL")

AQI_VARS: List[str] = [
    "european_aqi","pm10","pm2_5","carbon_monoxide","nitrogen_dioxide",
    "sulphur_dioxide","ozone","ammonia","uv_index_clear_sky","uv_index",
    "dust","aerosol_optical_depth","ragweed_pollen","olive_pollen",
    "mugwort_pollen","grass_pollen","birch_pollen","alder_pollen",
]


# ─── Client Open‑Meteo ──────────────────────────────────────────────
session = retry(requests_cache.CachedSession(".cache_aqi", expire_after=3600),
                retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=session)

def _load_cities() -> List[dict]:
    if not os.path.exists(CITIES_FILE):
        sys.exit(f"❌ {CITIES_FILE} introuvable")
    return json.load(open(CITIES_FILE, "r", encoding="utf-8"))

def _params(lat: float, lon: float) -> Dict[str, object]:
    return {"latitude": lat, "longitude": lon, "current": AQI_VARS, "timezone": "UTC"}

# ─── Producer main ─────────────────────────────────────────────────
def push_current_aqi_to_kafka(run_id: str) -> int:
    prod = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    sent = 0
    for city in _load_cities():
        try:
            resp = openmeteo.weather_api(URL, params=_params(city["lat"], city["lon"]))[0]
        except Exception as e:
            print(f"API error {city['name']}: {e}"); continue

        cur = resp.Current()
        record: Dict[str, object] = {
            "run_id": run_id,                      # ← marqueur d’exécution
            "city": city["name"],
            "latitude": resp.Latitude(),
            "longitude": resp.Longitude(),
            "time": pd.to_datetime(cur.Time(), unit="s", utc=True).isoformat(),
            "interval": cur.Interval(),
        }
        for i, v in enumerate(AQI_VARS):
            record[v] = cur.Variables(i).Value()

        # Notification Discord si air de mauvaise qualité (fonction modulaire)
        # notify_if_poor(record)

        prod.send(TOPIC_AQI, value=record); sent += 1

    prod.flush(); prod.close()
    print(f"✅ {sent} messages publiés sur {TOPIC_AQI}")
    return sent

if __name__ == "__main__":
    push_current_aqi_to_kafka(run_id="debug-" + os.environ.get("USER",""))
