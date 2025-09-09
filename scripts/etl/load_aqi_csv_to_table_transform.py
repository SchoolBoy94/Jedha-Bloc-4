from __future__ import annotations
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from scripts.utils.env_loader import load_env_file

# Charge l'env approprié (.env en Airflow / tests/.env_test en CI si ENV_FILE est défini)
load_env_file()

DB_CONN_INFO = {
    "host": os.getenv("POSTGRES_HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}

TABLE_TRANSFORM = os.getenv("TABLE_AQI_TRANSFORM")

CSV_PATH = os.getenv("CSV_PATH")

TRANSFORM_COLUMNS = [
    "time", "city", "latitude", "longitude",
    "european_aqi", "pm2_5", "pm10",
    "carbon_monoxide", "sulphur_dioxide", "uv_index",
]

def load_transform_csv(csv_path: str | None = None) -> int:
    """
    Charge un CSV AQI dans la table aqi_transform.
    - Si csv_path n'est pas fourni, utilise CSV_PATH depuis l'ENV.
    - Retourne le nombre de lignes insérées.
    """
    path = csv_path or CSV_PATH
    if not path:
        print("❌ CSV_PATH introuvable dans l'environnement et aucun chemin fourni.")
        return 0

    print(f"📂 Lecture du CSV: {path}")
    df = pd.read_csv(path)
    if df.empty:
        print("⚠️  Le fichier CSV est vide.")
        return 0

    for col in TRANSFORM_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df[TRANSFORM_COLUMNS]

    rows = df.where(pd.notnull(df), None).values.tolist()
    insert_sql = f"INSERT INTO {TABLE_TRANSFORM} ({', '.join(TRANSFORM_COLUMNS)}) VALUES %s"

    print(f"🔄 Insertion de {len(rows)} lignes dans aqi_transform...")
    with psycopg2.connect(**DB_CONN_INFO) as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows)

    print("✅ Insertion terminée avec succès.")
    return len(rows)

if __name__ == "__main__":
    load_transform_csv()
