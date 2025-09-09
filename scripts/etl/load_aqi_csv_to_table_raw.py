from __future__ import annotations
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from scripts.utils.env_loader import load_env_file

# Charge l'ENV approprié (.env en Airflow, tests/.env_test en CI si ENV_FILE est défini)
load_env_file()

DB_CONN_INFO = {
    "host": os.getenv("POSTGRES_HOST"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT"),
}

CSV_PATH = os.getenv("CSV_PATH")

TABLE_RAW = os.getenv("TABLE_AQI_RAW")

DB_COLUMNS = [
    "city", "latitude", "longitude", "time", "interval", "european_aqi",
    "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
    "sulphur_dioxide", "ozone", "aerosol_optical_depth", "dust",
    "uv_index", "uv_index_clear_sky", "ammonia",
    "alder_pollen", "birch_pollen", "grass_pollen", "mugwort_pollen",
    "olive_pollen", "ragweed_pollen",
]

def load_csv_to_postgres(csv_path: str | None = None) -> int:
    """
    Charge un CSV AQI dans la table aqi_raw.
    - Si csv_path n'est pas fourni, utilise CSV_PATH depuis l'ENV.
    - Retourne le nombre de lignes insérées.
    """
    path = csv_path or CSV_PATH
    if not path:
        print("❌ CSV_PATH introuvable dans l'environnement et aucun chemin fourni à la fonction.")
        return 0

    print(f"📂 Lecture du CSV: {path}")
    df = pd.read_csv(path)
    if df.empty:
        print("⚠️  Le fichier CSV est vide.")
        return 0

    if "interval" not in df.columns:
        df["interval"] = None

    for col in DB_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df[DB_COLUMNS]

    rows = df.where(pd.notnull(df), None).values.tolist()
    insert_sql = f"INSERT INTO {TABLE_RAW} ({', '.join(DB_COLUMNS)}) VALUES %s"

    print(f"🔄 Insertion de {len(rows)} lignes dans aqi_raw...")
    with psycopg2.connect(**DB_CONN_INFO) as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows)

    print("✅ Insertion terminée avec succès.")
    return len(rows)

if __name__ == "__main__":
    load_csv_to_postgres()















# from __future__ import annotations
# import os
# import pandas as pd
# from sqlalchemy import create_engine
# from sqlalchemy.exc import SQLAlchemyError
# from scripts.utils.env_loader import load_env_file

# # Charge l'ENV approprié (.env en Airflow, tests/.env_test en CI si ENV_FILE est défini)
# load_env_file()

# # Informations de connexion à la base de données
# DB_CONN_INFO = {
#     "host": os.getenv("POSTGRES_HOST"),
#     "database": os.getenv("POSTGRES_DB"),
#     "user": os.getenv("POSTGRES_USER"),
#     "password": os.getenv("POSTGRES_PASSWORD"),
#     "port": os.getenv("POSTGRES_PORT"),
# }

# # Variables liées au fichier CSV et à la table de base de données
# CSV_PATH = os.getenv("CSV_PATH")
# TABLE_RAW = os.getenv("TABLE_AQI_RAW")

# # Colonnes attendues dans le DataFrame pour l'insertion
# DB_COLUMNS = [
#     "city", "latitude", "longitude", "time", "interval", "european_aqi",
#     "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
#     "sulphur_dioxide", "ozone", "aerosol_optical_depth", "dust",
#     "uv_index", "uv_index_clear_sky", "ammonia",
#     "alder_pollen", "birch_pollen", "grass_pollen", "mugwort_pollen",
#     "olive_pollen", "ragweed_pollen",
# ]

# # URL de la connexion avec SQLAlchemy
# SQLALCHEMY_DATABASE_URI = f"postgresql+psycopg2://{DB_CONN_INFO['user']}:{DB_CONN_INFO['password']}@{DB_CONN_INFO['host']}:{DB_CONN_INFO['port']}/{DB_CONN_INFO['database']}"

# # Création de l'engine SQLAlchemy
# engine = create_engine(SQLALCHEMY_DATABASE_URI)

# def load_csv_to_postgres(csv_path: str | None = None) -> int:
#     """
#     Charge un CSV AQI dans la table aqi_raw.
#     Si csv_path n'est pas fourni, utilise CSV_PATH depuis l'ENV.
#     Retourne le nombre de lignes insérées.
#     """
#     # Détermine le chemin du fichier CSV à charger
#     path = csv_path or CSV_PATH
#     if not path:
#         print("❌ CSV_PATH introuvable dans l'environnement et aucun chemin fourni à la fonction.")
#         return 0

#     print(f"📂 Lecture du CSV: {path}")
#     df = pd.read_csv(path)
    
#     if df.empty:
#         print("⚠️ Le fichier CSV est vide.")
#         return 0

#     # Ajout de la colonne "interval" si elle est absente
#     if "interval" not in df.columns:
#         df["interval"] = None

#     # Ajouter des colonnes manquantes avec des valeurs nulles
#     for col in DB_COLUMNS:
#         if col not in df.columns:
#             df[col] = None

#     # Convertir la colonne "time" au format datetime
#     df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")

#     # Réorganiser les colonnes selon l'ordre défini dans DB_COLUMNS
#     df = df[DB_COLUMNS]

#     # Préparer les données pour l'insertion dans la base de données
#     rows = df.where(pd.notnull(df), None).values.tolist()
#     insert_sql = f"INSERT INTO {TABLE_RAW} ({', '.join(DB_COLUMNS)}) VALUES %s"

#     print(f"🔄 Insertion de {len(rows)} lignes dans aqi_raw...")

#     # Utilisation de SQLAlchemy pour insérer les données
#     try:
#         with engine.connect() as conn:
#             # Exécution de la requête d'insertion
#             conn.execute(insert_sql, rows)
#         print("✅ Insertion terminée avec succès.")
#     except SQLAlchemyError as e:
#         print(f"❌ Erreur lors de l'insertion dans la base de données: {e}")
#         return 0

#     return len(rows)

# if __name__ == "__main__":
#     load_csv_to_postgres()
