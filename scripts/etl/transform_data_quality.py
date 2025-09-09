from __future__ import annotations
import os
from typing import List, Tuple
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

from scripts.utils.env_loader import load_env_file

# ── Chargement ENV (.env en Airflow, tests/.env_test si ENV_FILE défini)
load_env_file()

# ── Connexion Postgres
DB_CONN_INFO = {
    "host": os.getenv("POSTGRES_HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}

# Nom de table (prend d’abord TABLE_AQI_TRANSFORM, sinon fallback)
TABLE_TRANS = os.getenv("TABLE_AQI_TRANSFORM") or os.getenv("TABLE_TRANS") or "aqi_transform"

# Colonnes « transform » attendues
TRANSFORM_COLUMNS: List[str] = [
    "time", "city", "latitude", "longitude",
    "european_aqi", "pm2_5", "pm10",
    "carbon_monoxide", "sulphur_dioxide", "uv_index",
]

NUMERIC_COLS: List[str] = [
    "latitude", "longitude", "european_aqi", "pm2_5", "pm10",
    "carbon_monoxide", "sulphur_dioxide", "uv_index",
]

CRITICAL_COLS: List[str] = ["time", "city", "latitude", "longitude"]


# ──────────────────────────────────────────────────────────────────────────────
# Chargement des données
# ──────────────────────────────────────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    try:
        url = (
            f"postgresql+psycopg2://{DB_CONN_INFO['user']}:"
            f"{DB_CONN_INFO['password']}@{DB_CONN_INFO['host']}:"
            f"{DB_CONN_INFO['port']}/{DB_CONN_INFO['database']}"
        )
        engine = create_engine(url)
        return pd.read_sql(f'SELECT * FROM {TABLE_TRANS};', con=engine)
    except Exception as e:
        print(f"❌ Erreur lors de la lecture de la base : {e}")
        return pd.DataFrame()


# ──────────────────────────────────────────────────────────────────────────────
# Checks de qualité (impression console) → True si anomalies
# ──────────────────────────────────────────────────────────────────────────────
def data_quality_checks(df: pd.DataFrame) -> bool:
    anomalies = False
    now_utc = pd.Timestamp.now(tz="UTC")

    print("\n1) Schéma / dtypes :")
    print(df.dtypes)

    print("\n2) Valeurs manquantes par colonne :")
    missing = df.isna().sum()
    print(missing)
    if missing.any():
        print("❌ Anomalie : valeurs manquantes détectées")
        anomalies = True

    print("\n3) Doublons (sur city,time) :")
    key_dups = df.duplicated(subset=["city", "time"]).sum() if {"city", "time"}.issubset(df.columns) else 0
    print(f"Doublons city+time : {key_dups}")
    if key_dups > 0:
        print("❌ Anomalie : doublons sur (city,time)")
        anomalies = True

    print("\n4) Statistiques descriptives (numériques) :")
    try:
        print(df[NUMERIC_COLS].describe())
    except Exception:
        pass

    print("\n5) Règles de domaine :")
    # time futur
    if "time" in df:
        # conversion douce pour le check
        time_parsed = pd.to_datetime(df["time"], errors="coerce", utc=True)
        future_cnt = (time_parsed > now_utc).sum()
        print(f"   time dans le futur : {future_cnt}")
        if future_cnt > 0:
            print("❌ Anomalie : dates futures")
            anomalies = True

    # lat/lon plausibles
    lat_bad = (~df.get("latitude", pd.Series(dtype=float)).between(-90, 90)).sum() if "latitude" in df else 0
    lon_bad = (~df.get("longitude", pd.Series(dtype=float)).between(-180, 180)).sum() if "longitude" in df else 0
    print(f"   latitude hors [-90,90] : {lat_bad}")
    print(f"   longitude hors [-180,180] : {lon_bad}")
    if lat_bad or lon_bad:
        print("❌ Anomalie : coordonnées invalides")
        anomalies = True

    # Polluants / indices non négatifs
    negatives = {}
    for col in ["european_aqi", "pm2_5", "pm10", "carbon_monoxide", "sulphur_dioxide", "uv_index"]:
        if col in df:
            col_num = pd.to_numeric(df[col], errors="coerce")
            negatives[col] = (col_num < 0).sum()
    if negatives:
        print("   valeurs négatives (par col) :", negatives)
        if any(v > 0 for v in negatives.values()):
            print("❌ Anomalie : valeurs négatives détectées")
            anomalies = True

    return anomalies


# ──────────────────────────────────────────────────────────────────────────────
# Nettoyage conservateur
# ──────────────────────────────────────────────────────────────────────────────
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    print("\n🧹 Nettoyage des données…")
    df = df.copy()

    # Garder les colonnes connues si présentes
    cols_present = [c for c in TRANSFORM_COLUMNS if c in df.columns]
    df = df[cols_present]

    # Types / conversions
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)

    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Lignes critiques complètes
    df = df.dropna(subset=[c for c in CRITICAL_COLS if c in df.columns])

    # Domain rules
    if "latitude" in df.columns:
        df = df[df["latitude"].between(-90, 90)]
    if "longitude" in df.columns:
        df = df[df["longitude"].between(-180, 180)]
    if "time" in df.columns:
        df = df[df["time"] <= pd.Timestamp.now(tz="UTC")]

    for c in ["european_aqi", "pm2_5", "pm10", "carbon_monoxide", "sulphur_dioxide", "uv_index"]:
        if c in df.columns:
            df[c] = df[c].clip(lower=0)

    # Remplissages prudents sur numériques manquants (médiane)
    for c in [col for col in NUMERIC_COLS if col in df.columns]:
        if df[c].isna().any():
            df[c] = df[c].fillna(df[c].median())

    # Déduplication clé (city,time)
    if {"city", "time"}.issubset(df.columns):
        df = df.sort_values("time").drop_duplicates(subset=["city", "time"], keep="last")

    # Ré-ordonne / complète les colonnes attendues
    for c in TRANSFORM_COLUMNS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[TRANSFORM_COLUMNS]

    print("✅ Nettoyage terminé.")
    return df


# ──────────────────────────────────────────────────────────────────────────────
# Réécriture table (TRUNCATE + INSERT batch)
# ──────────────────────────────────────────────────────────────────────────────
def update_data(df: pd.DataFrame) -> None:
    print(f"\n🗄️  Réécriture de {TABLE_TRANS}…")
    # psycopg2 gère très bien les datetime tz-aware ; sinon convertis en str ISO
    # df["time"] = df["time"].astype(str)

    records = []
    for row in df.itertuples(index=False):
        tup = tuple(
            (val.to_pydatetime() if isinstance(val, pd.Timestamp) else val)
            for val in row
        )
        records.append(tup)

    with psycopg2.connect(**DB_CONN_INFO) as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {TABLE_TRANS};")
            placeholders = "(" + ",".join(["%s"] * len(TRANSFORM_COLUMNS)) + ")"
            sql = f"INSERT INTO {TABLE_TRANS} ({', '.join(TRANSFORM_COLUMNS)}) VALUES %s"
            execute_values(cur, sql, records, template=placeholders)
        conn.commit()
    print(f"✅ {len(records)} lignes écrites dans {TABLE_TRANS}.")


# ──────────────────────────────────────────────────────────────────────────────
# Orchestration
# ──────────────────────────────────────────────────────────────────────────────
def quality_data():
    df = load_data()
    if df.empty:
        print(f"❌ Aucune donnée trouvée dans `{TABLE_TRANS}`.")
        return

    print(f"🔎 Contrôle qualité sur {len(df)} lignes…")
    anomalies = data_quality_checks(df)

    if anomalies:
        print("⚠️  Anomalies détectées. Nettoyage en cours…")
        cleaned_df = clean_data(df)
        print(f"📉 Lignes après nettoyage : {len(cleaned_df)}")
        update_data(cleaned_df)
    else:
        print("✅ Données conformes. Aucun nettoyage requis.")

if __name__ == "__main__":
    quality_data()
