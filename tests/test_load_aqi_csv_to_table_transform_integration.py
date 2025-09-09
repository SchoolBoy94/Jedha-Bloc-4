from __future__ import annotations

import os
import uuid
import tempfile
import importlib
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from psycopg2 import sql
import pytest

from scripts.utils.env_loader import load_env_file


def _ensure_database_exists(host: str, dbname: str, user: str, password: str | None, port: str | int) -> None:
    """
    S'assure que la base POSTGRES_DB existe (connexion sur 'postgres' puis CREATE DATABASE IF NOT EXISTS).
    Inoffensif si la base existe déjà.
    """
    dsn_admin = {"host": host, "database": "postgres", "user": user, "password": password, "port": port}
    with psycopg2.connect(**dsn_admin) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (dbname,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            conn.commit()


@pytest.mark.integration
def test_load_aqi_csv_to_table_transform_integration(monkeypatch):
    """
    - Charge .env_test
    - Vérifie reachability Postgres (et crée la DB si manquante)
    - Crée/garantit l'existence de la table de test (via create_tables)
    - Génère un CSV temporaire conforme aux colonnes transform
    - Déclenche l'import (load_transform_csv) en passant CSV_PATH via l'ENV
    - Vérifie le nombre de lignes insérées
    - Nettoie : DELETE des lignes insérées + suppression du CSV
    """

    # 1) ENV de test
    tests_env = os.path.join(os.getcwd(), "tests", ".env_test")
    monkeypatch.setenv("ENV_FILE", tests_env)
    load_env_file()

    host = os.getenv("POSTGRES_HOST")
    dbname = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    port = os.getenv("POSTGRES_PORT")

    table_transf = os.getenv("TABLE_AQI_TRANSFORM")
    # suffix = os.getenv("nombre")
    
    assert table_transf, "TABLE_AQI_TRANSFORM doit être défini dans tests/.env_test"

    db_params = {"host": host, "database": dbname, "user": user, "password": password, "port": port}

    # 2) Reachability + création DB si besoin
    try:
        _ensure_database_exists(host, dbname, user, password, port)
        with psycopg2.connect(**db_params):
            pass
    except Exception as e:
        pytest.skip(f"Postgres injoignable avec paramètres de tests: {e}")

    # 3) Garantir l'existence de la table via votre module create_tables (lit l'ENV)
    mod_tables = importlib.import_module("scripts.etl.create_tables")
    mod_tables = importlib.reload(mod_tables)
    mod_tables.create_tables()

    # 4) Générer un CSV temporaire avec 3 lignes "propres"
    suffix = uuid.uuid4().hex[:6]
    ts = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat()

    df = pd.DataFrame(
        [
            {
                "time": ts,
                "city": f"TParis_{suffix}",
                "latitude": 48.8566,
                "longitude": 2.3522,
                "european_aqi": 50.0,
                "pm2_5": 7.5,
                "pm10": 12.0,
                "carbon_monoxide": 0.10,
                "sulphur_dioxide": 1.2,
                "uv_index": 2.0,
            },
            {
                "time": ts,
                "city": f"TLyon_{suffix}",
                "latitude": 45.7640,
                "longitude": 4.8357,
                "european_aqi": 60.0,
                "pm2_5": 8.0,
                "pm10": 14.0,
                "carbon_monoxide": 0.12,
                "sulphur_dioxide": 1.1,
                "uv_index": 1.5,
            },
            {
                "time": ts,
                "city": f"TMarseille_{suffix}",
                "latitude": 43.2965,
                "longitude": 5.3698,
                "european_aqi": 55.0,
                "pm2_5": 6.8,
                "pm10": 13.0,
                "carbon_monoxide": 0.09,
                "sulphur_dioxide": 1.0,
                "uv_index": 2.2,
            },
        ]
    )

    tmp_csv = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8")
    try:
        df.to_csv(tmp_csv.name, index=False)

        # 5) Pré-nettoyage (au cas où) : on supprime d'éventuelles occurrences d'un run précédent
        cities = df["city"].tolist()
        with psycopg2.connect(**db_params) as c, c.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {} WHERE city = ANY(%s) AND time = %s").format(sql.Identifier(table_transf)),
                (cities, ts),
            )
            c.commit()

        # 6) Paramétrer CSV_PATH pour le module
        monkeypatch.setenv("CSV_PATH", tmp_csv.name)

        # 7) Importer/reloader le module cible puis lancer l'import
        mod = importlib.import_module("scripts.etl.load_aqi_csv_to_table_transform")
        mod = importlib.reload(mod)
        
        # ⬇️  ICI: on passe le chemin explicite, donc pas d'utilisation de CSV_PATH du module
        inserted = mod.load_transform_csv(csv_path=tmp_csv.name)
        assert inserted == 3, f"Nombre de lignes insérées inattendu: {inserted}"

        # 8) Vérification en base
        with psycopg2.connect(**db_params) as c, c.cursor() as cur:
            cur.execute(
                sql.SQL("SELECT COUNT(*) FROM {} WHERE city = ANY(%s) AND time = %s").format(sql.Identifier(table_transf)),
                (cities, ts),
            )
            count = cur.fetchone()[0]
        assert count == 3, f"Lignes attendues non trouvées dans {table_transf}: {count}/3"

    finally:
        # 5) Cleanup ciblé: supprime uniquement les lignes insérées par ce test
        try:
            with psycopg2.connect(**db_params) as c, c.cursor() as cur:
                truncate_q = sql.SQL("TRUNCATE TABLE {}").format(
                    sql.Identifier(table_transf)
                )
                cur.execute(truncate_q)
                c.commit()
        except Exception:
            pass  # best effort

        try:
            os.unlink(tmp_csv.name)
        except Exception:
            pass
