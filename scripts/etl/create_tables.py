from __future__ import annotations
import os
import psycopg2
from psycopg2 import sql
from scripts.utils.env_loader import load_env_file

# Charge .env (Airflow) ou tests/.env_test (CI) selon ENV_FILE
load_env_file()

# Connexion Postgres
DB_CONN_INFO = {
    "host": os.getenv("POSTGRES_HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}

# Noms de tables depuis le .env (par défaut: aqi_raw / aqi_transform)
TABLE_RAW = os.getenv("TABLE_AQI_RAW")
TABLE_TRANSFORM = os.getenv("TABLE_AQI_TRANSFORM")

# DDL avec placeholder sécurisé {tbl}
DDL_RAW = sql.SQL("""
CREATE TABLE IF NOT EXISTS {tbl} (
    id SERIAL PRIMARY KEY,
    city TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    time TIMESTAMPTZ NOT NULL,
    interval INTEGER,
    european_aqi INTEGER,
    pm10 NUMERIC,
    pm2_5 NUMERIC,
    carbon_monoxide NUMERIC,
    nitrogen_dioxide NUMERIC,
    sulphur_dioxide NUMERIC,
    ozone NUMERIC,
    aerosol_optical_depth NUMERIC,
    dust NUMERIC,
    uv_index NUMERIC,
    uv_index_clear_sky NUMERIC,
    ammonia NUMERIC,
    alder_pollen NUMERIC,
    birch_pollen NUMERIC,
    grass_pollen NUMERIC,
    mugwort_pollen NUMERIC,
    olive_pollen NUMERIC,
    ragweed_pollen NUMERIC
);
""")

DDL_TRANSFORM = sql.SQL("""
CREATE TABLE IF NOT EXISTS {tbl} (
    id SERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL,
    city TEXT NOT NULL,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    european_aqi NUMERIC,
    pm2_5 NUMERIC,
    pm10 NUMERIC,
    carbon_monoxide NUMERIC,
    sulphur_dioxide NUMERIC,
    uv_index NUMERIC
);
""")

def create_table(ddl_template: sql.SQL, table_name: str) -> None:
    with psycopg2.connect(**DB_CONN_INFO) as conn, conn.cursor() as cur:
        cur.execute(ddl_template.format(tbl=sql.Identifier(table_name)))
    print(f"✅ Table {table_name} créée (ou déjà existante)")

def create_tables() -> None:
    create_table(DDL_RAW, TABLE_RAW)
    create_table(DDL_TRANSFORM, TABLE_TRANSFORM)

if __name__ == "__main__":
    create_tables()
