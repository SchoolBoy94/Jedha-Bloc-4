from __future__ import annotations
import os, json, uuid, socket, time, importlib
from datetime import datetime, timezone
import pytest
import psycopg2
from psycopg2 import sql
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from scripts.utils.env_loader import load_env_file


def _ensure_database_exists(host: str, dbname: str, user: str, password: str | None, port: str | int) -> None:
    """Crée la base POSTGRES_DB si absente (connexion via DB 'postgres')."""
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
def test_insert_in_table_transform_integration(monkeypatch):
    """
    - Charge .env_test
    - Vérifie la reachability Kafka & Postgres (FAIL si KO)
    - Crée un topic Kafka temporaire (suffix)
    - Garantit la table cible (via create_tables) en lisant son nom depuis .env_test
    - Publie 3 messages déjà « transformés » (avec run_id)
    - Appelle insert_from_kafka(run_id)
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
    table = os.getenv("TABLE_AQI_TRANSFORM") or "aqi_transform"

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

    # 2) Topic temporaire dérivé du nom de base
    suffix = uuid.uuid4().hex[:8]
    base_topic = os.getenv("TOPIC_AQI_TRANSFORMED") or "aqi_transformed"
    topic = f"{base_topic}_{suffix}"

    # Propager au module (il lit l'ENV à l'import)
    monkeypatch.setenv("TOPIC_AQI_TRANSFORMED", topic)
    monkeypatch.setenv("TABLE_AQI_TRANSFORM", table)

    # 3) Créer le topic
    admin = KafkaAdminClient(bootstrap_servers=broker, client_id="it_insert_trans_admin")
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

    # 4) Garantir la table via votre module create_tables
    mod_tables = importlib.import_module("scripts.etl.create_tables")
    mod_tables = importlib.reload(mod_tables)
    mod_tables.create_tables()  # doit créer/garantir TABLE_AQI_TRANSFORM

    # 5) Importer le module cible APRÈS paramétrage ENV
    mod = importlib.import_module("scripts.etl.insert_in_table_transform")
    mod = importlib.reload(mod)
    # Robustesse : s’assurer que ses globals correspondent bien à ce test
    mod.KAFKA_BROKER = broker
    mod.TOPIC_TRANS = topic
    mod.TABLE_TRANS = table

    # 6) Publier 3 messages déjà « transformés »
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    run_id = f"run_{suffix}"
    ts = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc).isoformat()
    cities = [f"TParis_{suffix}", f"TLyon_{suffix}", f"TMarseille_{suffix}"]

    base = {
        "time": ts,
        "latitude": 48.0,
        "longitude": 2.0,
        "european_aqi": 55.0,
        "pm2_5": 7.5,
        "pm10": 12.0,
        "carbon_monoxide": 0.10,
        "sulphur_dioxide": 1.2,
        "uv_index": 2.0,
    }
    for c in cities:
        msg = {"run_id": run_id, "city": c, **base}
        producer.send(topic, value=msg)
    producer.flush()
    producer.close()
    time.sleep(0.5)  # petit délai pour environnements lents

    # 7) Consommer & insérer (limité à 3 pour accélérer)
    inserted = mod.insert_from_kafka(run_id=run_id, max_records=3)
    assert inserted == 3, f"Nombre de lignes insérées inattendu: {inserted}"

    # 8) Vérifier en base
    ts_dt = datetime.fromisoformat(ts)  # tz-aware
    with psycopg2.connect(**db_params) as c, c.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {} WHERE city = ANY(%s) AND time = %s").format(sql.Identifier(table)),
            (cities, ts_dt),
        )
        count = cur.fetchone()[0]
    assert count == 3, f"Lignes retrouvées en base: {count} (attendu 3)"

    # 9) Cleanup : DELETE ciblé + suppression du topic (pas de DROP TABLE)
    with psycopg2.connect(**db_params) as c, c.cursor() as cur:
        cur.execute(
            sql.SQL("DELETE FROM {} WHERE city = ANY(%s) AND time = %s").format(sql.Identifier(table)),
            (cities, ts_dt),
        )
        c.commit()

    admin = KafkaAdminClient(bootstrap_servers=broker, client_id="it_insert_trans_admin_cleanup")
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
