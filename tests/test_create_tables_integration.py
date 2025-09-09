from __future__ import annotations
import os
import importlib
import psycopg2
from psycopg2 import sql
import pytest

from scripts.utils.env_loader import load_env_file


def _ensure_database_exists(host: str, dbname: str, user: str, password: str, port: str) -> None:
    """
    Crée la base `dbname` si elle n'existe pas encore.
    Connexion via la DB d'admin 'postgres' (hors transaction).
    """
    # Connexion serveur (DB 'postgres') pour pouvoir faire CREATE DATABASE
    conn = psycopg2.connect(host=host, database="postgres", user=user, password=password, port=port)
    try:
        conn.autocommit = True  # CREATE DATABASE interdit dans une transaction
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    finally:
        conn.close()


def _table_exists(db_params: dict, table_name: str) -> bool:
    """
    Vérifie l'existence d'une table dans le schéma public.
    Gère correctement la casse (maj/min) grâce à un paramètre.
    """
    q = """
        SELECT 1
        FROM pg_catalog.pg_tables
        WHERE schemaname = 'public' AND tablename = %s
        LIMIT 1
    """
    with psycopg2.connect(**db_params) as c, c.cursor() as cur:
        cur.execute(q, (table_name,))
        return cur.fetchone() is not None


@pytest.mark.integration
def test_create_tables_integration(monkeypatch):
    # 1) ENV de test
    tests_env = os.path.join(os.getcwd(), "tests", ".env_test")
    monkeypatch.setenv("ENV_FILE", tests_env)
    load_env_file()

    host = os.getenv("POSTGRES_HOST")
    dbname = os.getenv("POSTGRES_DB")     # ← on utilise exactement la DB définie dans .env_test
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    port = os.getenv("POSTGRES_PORT")

    assert host and dbname and user and port, "Variables POSTGRES_* manquantes dans tests/.env_test"

    # 2) Créer la base si nécessaire (avant d'appeler create_tables)
    _ensure_database_exists(host, dbname, user, password, port)

    # Paramètres vers la DB cible (celle du test)
    db_params = {
        "host": host,
        "database": dbname,
        "user": user,
        "password": password,
        "port": port,
    }

    # Skip propre si Postgres down
    try:
        with psycopg2.connect(**db_params):
            pass
    except Exception as e:
        pytest.skip(f"Postgres injoignable avec paramètres de tests: {e}")

    # 3) Récupère les noms de tables depuis .env_test
    table_raw = os.getenv("TABLE_AQI_RAW")
    table_trans = os.getenv("TABLE_AQI_TRANSFORM")
    assert table_raw and table_trans, "TABLE_AQI_RAW / TABLE_AQI_TRANSFORM manquantes dans tests/.env_test"

    # On re-propage explicitement ces valeurs dans l'env du module appelé
    monkeypatch.setenv("TABLE_AQI_RAW", table_raw)
    monkeypatch.setenv("TABLE_AQI_TRANSFORM", table_trans)

    # 4) Import/reload du module puis exécution
    mod = importlib.import_module("scripts.etl.create_tables")
    mod = importlib.reload(mod)
    mod.create_tables()

    # 5) Vérifications d’existence
    assert _table_exists(db_params, table_raw), f"Table absente: {table_raw}"
    assert _table_exists(db_params, table_trans), f"Table absente: {table_trans}"

    # 6) Idempotence (ré-appel sans erreur)
    mod.create_tables()
    assert _table_exists(db_params, table_raw)
    assert _table_exists(db_params, table_trans)

    # 7) Cleanup (supprime les tables créées pour ce test)
    # with psycopg2.connect(**db_params) as c, c.cursor() as cur:
    #     cur.execute(sql.SQL('DROP TABLE IF EXISTS {} CASCADE').format(sql.Identifier(table_raw)))
    #     cur.execute(sql.SQL('DROP TABLE IF EXISTS {} CASCADE').format(sql.Identifier(table_trans)))
    #     c.commit()
