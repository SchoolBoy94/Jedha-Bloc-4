from __future__ import annotations
import os
import importlib
from datetime import datetime, timedelta, timezone

import pytest
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from scripts.utils.env_loader import load_env_file


def _ensure_database_exists(host: str, dbname: str, user: str, password: str | None, port: str | int) -> None:
    """Crée la DB si elle n'existe pas (connexion à postgres)."""
    dsn_admin = {"host": host, "database": "postgres", "user": user, "password": password, "port": port}
    with psycopg2.connect(**dsn_admin) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (dbname,))
        if cur.fetchone() is None:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            conn.commit()


@pytest.mark.integration
def test_data_quality_integration(monkeypatch):
    # 1) ENV de test
    tests_env = os.path.join(os.getcwd(), "tests", ".env_test")
    monkeypatch.setenv("ENV_FILE", tests_env)
    load_env_file()

    host = os.getenv("POSTGRES_HOST")
    dbname = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    port = os.getenv("POSTGRES_PORT")
    table_trans = os.getenv("TABLE_AQI_TRANSFORM") or "aqi_transform_test"

    assert host and dbname and user and port, "Variables POSTGRES_* manquantes"
    assert table_trans, "TABLE_AQI_TRANSFORM manquante"

    # 2) Reachability + création DB au besoin
    try:
        _ensure_database_exists(host, dbname, user, password, port)
        with psycopg2.connect(host=host, database=dbname, user=user, password=password, port=port):
            pass
    except Exception as e:
        pytest.skip(f"Postgres injoignable avec paramètres de tests: {e}")

    db_params = {"host": host, "database": dbname, "user": user, "password": password, "port": port}

    # 3) Garantir l'existence de la table via votre module create_tables
    mod_tables = importlib.import_module("scripts.etl.create_tables")
    mod_tables = importlib.reload(mod_tables)
    mod_tables.create_tables()  # lit TABLE_AQI_TRANSFORM depuis l'ENV

    # 4) Préparer des données avec anomalies
    now = datetime.now(timezone.utc)
    ts_ok = (now - timedelta(days=1)).isoformat()
    ts_future = (now + timedelta(days=1)).isoformat()

    # Villes unique-tag pour filtrer/clean
    suffix = os.getenv("nombre") or "IT"
    city_dup = f"IT_Dup_{suffix}"
    city_neg = f"IT_Neg_{suffix}"
    city_future = f"IT_Future_{suffix}"
    city_badlat = f"IT_BadLat_{suffix}"
    city_misslon = f"IT_MissLon_{suffix}"

    # Colonnes attendues par transform: time, city, latitude, longitude,
    # european_aqi, pm2_5, pm10, carbon_monoxide, sulphur_dioxide, uv_index
    rows = [
        # Doublons sur (city,time) -> on garde la dernière (pm10=20)
        (ts_ok, city_dup,    48.8566,  2.3522, 50.0, 7.5, 10.0, 0.10, 1.2, 2.0),
        (ts_ok, city_dup,    48.8566,  2.3522, 50.0, 7.5, 20.0, 0.10, 1.2, 2.5),
        # Valeur négative -> uv_index doit être clippé à 0
        (ts_ok, city_neg,    45.7640,  4.8357, 60.0, 8.0, 14.0, 0.12, 1.1, -5.0),
        # Date future -> supprimée
        (ts_future, city_future, 43.2965, 5.3698, 55.0, 6.8, 13.0, 0.09, 1.0, 2.2),
        # Latitude invalide -> supprimée
        (ts_ok, city_badlat, 200.0,    2.0000, 55.0, 6.8, 13.0, 0.09, 1.0, 1.0),
        # Longitude manquante (critique) -> supprimée
        (ts_ok, city_misslon, 48.9000, None,   55.0, 6.8, 13.0, 0.09, None, 1.0),
    ]

    # 5) État initial propre: on supprime d'éventuelles entrées précédentes
    with psycopg2.connect(**db_params) as c, c.cursor() as cur:
        cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_trans)))
        c.commit()

    # 6) Insert des lignes brutes (avec anomalies)
    insert_sql = sql.SQL("INSERT INTO {} "
                         "(time, city, latitude, longitude, european_aqi, pm2_5, pm10, carbon_monoxide, sulphur_dioxide, uv_index) "
                         "VALUES %s").format(sql.Identifier(table_trans))
    with psycopg2.connect(**db_params) as conn, conn.cursor() as cur:
        execute_values(cur, insert_sql.as_string(conn), rows)
        conn.commit()

    # 7) Importer et exécuter la qualité
    mod_q = importlib.import_module("scripts.etl.transform_data_quality")
    mod_q = importlib.reload(mod_q)
    # S'assurer que le module pointe bien sur la bonne table (au cas où)
    mod_q.TABLE_TRANS = table_trans

    mod_q.quality_data()

    # 8) Vérifications: seules 2 lignes doivent rester (dup -> 1, neg -> 1)
    with psycopg2.connect(**db_params) as c, c.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT city, pm10, uv_index, time, latitude, longitude FROM {} "
                    "WHERE city IN (%s, %s)").format(sql.Identifier(table_trans)),
            (city_dup, city_neg),
        )
        kept = cur.fetchall()

    # Il doit y avoir exactement 2 lignes (dup et neg)
    assert len(kept) == 2, f"Lignes conservées inattendues: {len(kept)} (attendu 2)"

    # Vérifs par ville
    data = {row[0]: row[1:] for row in kept}  # city -> (pm10, uv_index, time, lat, lon)

    # Doublon : on garde la dernière avec pm10=20
    assert city_dup in data
    pm10_dup = data[city_dup][0]
    assert pm10_dup == 20.0, f"pm10 attendu 20.0 pour {city_dup}, obtenu {pm10_dup}"

    # Négatif clippé
    assert city_neg in data
    uv_neg = data[city_neg][1]
    assert uv_neg == 0 or uv_neg == 0.0, f"uv_index attendu 0 pour {city_neg}, obtenu {uv_neg}"

    # Tous les enregistrements restants doivent être dans le passé et coords plausibles
    for city, (pm10, uv, t_iso, lat, lon) in data.items():
        # time <= now
        t = t_iso if isinstance(t_iso, datetime) else datetime.fromisoformat(str(t_iso))
        assert t <= datetime.now(timezone.utc), f"{city}: time futur non nettoyé"
        # lat/lon dans les bornes
        assert -90 <= float(lat) <= 90, f"{city}: latitude hors bornes"
        assert -180 <= float(lon) <= 180, f"{city}: longitude hors bornes"

    # 9) Cleanup: table propre pour les autres tests
    with psycopg2.connect(**db_params) as c, c.cursor() as cur:
        cur.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table_trans)))
        c.commit()
