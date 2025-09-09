from __future__ import annotations
import os, sys, json, logging
from typing import List, Optional
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from kafka import KafkaConsumer
from scripts.utils.env_loader import load_env_file

load_env_file()

# ─── Logger ────────────────────────────────────────────────────────────────
log = logging.getLogger("insert_table_raw")
log.setLevel(logging.INFO)
log.addHandler(logging.StreamHandler(sys.stdout))

# ─── ENV / Config ─────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
TOPIC_AQI    = os.getenv("TOPIC_AQI_RAW")      # ex: aqi_current
TABLE_RAW    = os.getenv("TABLE_AQI_RAW")      # ex: aqi_raw

if not all([KAFKA_BROKER, TOPIC_AQI, TABLE_RAW]):
    raise RuntimeError("ENV manquantes: KAFKA_BROKER / TOPIC_AQI_RAW / TABLE_AQI_RAW")

DB_CONN_INFO = {
    "host": os.getenv("POSTGRES_HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}

EXPECTED_COLS: List[str] = [
    "run_id", "time", "city", "latitude", "longitude", "interval",
    "european_aqi", "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
    "sulphur_dioxide", "ozone", "ammonia", "uv_index_clear_sky", "uv_index",
    "dust", "aerosol_optical_depth", "ragweed_pollen", "olive_pollen",
    "mugwort_pollen", "grass_pollen", "birch_pollen", "alder_pollen",
]

# ─── Insert helper ────────────────────────────────────────────────────────
def df_to_postgres_batch(df: pd.DataFrame, table: str) -> int:
    """Insert en batch; retourne le nombre de lignes insérées."""
    if df.empty:
        return 0
    # Remplace NaN par None pour psycopg2
    rows = df.where(pd.notnull(df), None).values.tolist()
    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})"
    with psycopg2.connect(**DB_CONN_INFO) as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, rows)
        conn.commit()
    log.info("✅ INSERT %s lignes → %s", len(df), table)
    return len(df)

# ─── Consumer principal ───────────────────────────────────────────────────
def consume_kafka_to_table(run_id: str, max_records: Optional[int] = None) -> int:
    cons = KafkaConsumer(
        TOPIC_AQI,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,                  # on commit après l’insert
        group_id=f"aqi_raw_loader_{run_id}",      # 1 run = 1 groupe (offsets isolés)
        consumer_timeout_ms=10_000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    records = []
    for msg in cons:
        val = msg.value
        if val.get("run_id") != run_id:
            continue
        records.append(val)
        if max_records and len(records) >= max_records:   # ← on compte les matchs
            break

    if not records:
        cons.close()
        log.warning("⚠️  Aucun message pour run_id=%s — rien inséré.", run_id)
        return 0

    df = pd.DataFrame(records).reindex(columns=EXPECTED_COLS, fill_value=None)

    # # Cast explicite vers datetime (TIMESTAMPTZ)
    # if "time" in df.columns:
    #     df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")

    # Clé technique non stockée dans aqi_raw
    df.drop(columns=["run_id"], inplace=True, errors="ignore")

    inserted = df_to_postgres_batch(df, TABLE_RAW)

    # Commit des offsets UNIQUEMENT si l’insert a réussi
    cons.commit()
    cons.close()
    return inserted

if __name__ == "__main__":
    consume_kafka_to_table(run_id="debug")
