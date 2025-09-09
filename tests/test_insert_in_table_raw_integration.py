from __future__ import annotations
import os, uuid, json, time, importlib, socket
import pytest
import psycopg2
from psycopg2 import sql
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from scripts.utils.env_loader import load_env_file


def _ensure_database_exists(host: str, dbname: str, user: str, password: str | None, port: str | int) -> None:
    """Crée la base `dbname` si absente (connexion via DB 'postgres')."""
    conn = psycopg2.connect(host=host, database="postgres", user=user, password=password, port=port)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (dbname,))
            if cur.fetchone() is None:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    finally:
        conn.close()


@pytest.mark.integration
def test_insert_in_table_raw_integration(monkeypatch):
    """
    - Charge .env_test
    - Vérifie Kafka & Postgres (FAIL si KO)
    - Crée un topic temporaire (suffix) et garantit la table depuis .env_test
    - Envoie 3 messages JSON sur le topic
    - Appelle consume_kafka_to_table(run_id=…)
    - Vérifie 3 lignes en base
    - Cleanup : DELETE ciblé + suppression du topic
    """
    # 1) ENV de tests
    tests_env = os.path.join(os.getcwd(), "tests", ".env_test")
    monkeypatch.setenv("ENV_FILE", tests_env)
    load_env_file()

    # --- Postgres (.env_test) ---
    host = os.getenv("POSTGRES_HOST")
    dbname = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    port = os.getenv("POSTGRES_PORT")
    table = os.getenv("TABLE_AQI_RAW") or "aqi_raw"
    assert host and dbname and user and port, "Variables POSTGRES_* manquantes dans tests/.env_test"

    _ensure_database_exists(host, dbname, user, password, port)
    db_params = {"host": host, "database": dbname, "user": user, "password": password, "port": port}
    try:
        with psycopg2.connect(**db_params):
            pass
    except Exception as e:
        pytest.fail(f"Postgres injoignable: {e}")

    # --- Kafka (.env_test) ---
    broker = os.getenv("KAFKA_BROKER")
    assert broker, "KAFKA_BROKER manquant dans tests/.env_test"
    h, p = broker.split(":")
    try:
        with socket.create_connection((h, int(p)), timeout=5):
            pass
    except Exception as e:
        pytest.fail(f"Kafka broker injoignable ({broker}) : {e}")

    # 2) Ressources temporaires : topic suffixé ; table depuis .env_test (pas de DROP)
    suffix = uuid.uuid4().hex[:8]
    base_topic = os.getenv("TOPIC_AQI_RAW") or "aqi_current"
    topic = f"{base_topic}_{suffix}"

    # Propager ces noms (le module lit l'ENV à l'import)
    monkeypatch.setenv("TOPIC_AQI_RAW", topic)
    monkeypatch.setenv("TABLE_AQI_RAW", table)

    # 3) Créer le topic
    admin = KafkaAdminClient(bootstrap_servers=broker, client_id="it_insert_raw_admin")
    try:
        existing = set(admin.list_topics())
        if topic not in existing:
            admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
            for _ in range(20):
                if topic in set(admin.list_topics()):
                    break
                time.sleep(0.25)
    finally:
        admin.close()

    # 4) Garantir la table (via votre module)
    mod_tables = importlib.import_module("scripts.etl.create_tables")
    mod_tables = importlib.reload(mod_tables)
    mod_tables.create_tables()

    # 5) Importer le module consommateur après paramétrage ENV
    mod = importlib.import_module("scripts.etl.insert_in_table_raw")
    mod = importlib.reload(mod)
    # ⚠️ Sécuriser contre un éventuel re-load d'ENV dans le module : on force ses globals
    mod.KAFKA_BROKER = broker
    mod.TOPIC_AQI = topic
    mod.TABLE_RAW = table

    # 6) Produire 3 messages valides sur le topic
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    run_id = f"run_{suffix}"
    ts_iso = "2025-01-01T00:00:00+00:00"
    cities = [f"CityA_{suffix}", f"CityB_{suffix}", f"CityC_{suffix}"]

    for city in cities:
        payload = {
            "run_id": run_id,
            "time": ts_iso,
            "city": city,
            "latitude": 48.0,
            "longitude": 2.0,
            "interval": 3600,
            "european_aqi": 42,
            "pm10": 10.5,
            "pm2_5": 5.2,
            "carbon_monoxide": 0.1,
            "nitrogen_dioxide": 12.3,
            "sulphur_dioxide": 1.1,
            "ozone": 30.0,
            "ammonia": 0.05,
            "uv_index_clear_sky": 2.0,
            "uv_index": 1.8,
            "dust": 0.2,
            "aerosol_optical_depth": 0.01,
            "ragweed_pollen": 0.0,
            "olive_pollen": 0.0,
            "mugwort_pollen": 0.0,
            "grass_pollen": 0.0,
            "birch_pollen": 0.0,
            "alder_pollen": 0.0,
        }
        producer.send(topic, value=payload)
    producer.flush()
    producer.close()
    time.sleep(0.5)  # petit délai pour les environnements lents

    # 7) Consommer & insérer
    inserted = mod.consume_kafka_to_table(run_id=run_id, max_records=None)
    assert inserted == 3, f"Nombre de lignes insérées inattendu: {inserted}"

    # 8) Vérifier en base
    with psycopg2.connect(**db_params) as c, c.cursor() as cur:
        q = sql.SQL("SELECT count(*) FROM {} WHERE city = ANY(%s) AND time = %s").format(sql.Identifier(table))
        cur.execute(q, (cities, ts_iso))
        count = cur.fetchone()[0]
    assert count == 3, f"Lignes retrouvées en base: {count} (attendu 3)"

    # 9) Cleanup : DELETE ciblé (on garde la table pour les autres tests) + suppression topic
    with psycopg2.connect(**db_params) as c, c.cursor() as cur:
        cur.execute(
            sql.SQL("DELETE FROM {} WHERE city = ANY(%s) AND time = %s").format(sql.Identifier(table)),
            (cities, ts_iso),
        )
        c.commit()

    admin = KafkaAdminClient(bootstrap_servers=broker, client_id="it_insert_raw_admin_cleanup")
    try:
        try:
            admin.delete_topics([topic])
        finally:
            for _ in range(20):
                if topic not in set(admin.list_topics()):
                    break
                time.sleep(0.25)
    finally:
        admin.close()
