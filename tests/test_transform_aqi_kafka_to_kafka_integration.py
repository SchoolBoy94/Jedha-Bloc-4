from __future__ import annotations
import os, json, socket, time, uuid, importlib
import pytest
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from scripts.utils.env_loader import load_env_file

@pytest.mark.integration
def test_transform_aqi_kafka_to_kafka_integration(monkeypatch):
    # 1) ENV de test
    tests_env = os.path.join(os.getcwd(), "tests", ".env_test")
    monkeypatch.setenv("ENV_FILE", tests_env)
    load_env_file()

    broker = os.getenv("KAFKA_BROKER")
    assert broker, "KAFKA_BROKER doit être défini dans tests/.env_test"
    host, port = broker.split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=5):
            pass
    except Exception as e:
        pytest.fail(f"Kafka broker injoignable ({broker}) : {e}")

    # 2) Topics temporaires dérivés des noms de base
    base_raw = os.getenv("TOPIC_AQI_RAW")
    base_out = os.getenv("TOPIC_AQI_TRANSFORMED")
    suffix = uuid.uuid4().hex[:8]
    topic_raw = f"{base_raw}_{suffix}"
    topic_out = f"{base_out}_{suffix}"

    # 3) Créer les topics temporaires
    admin = KafkaAdminClient(bootstrap_servers=broker, client_id="it_transform_admin")
    try:
        to_create = []
        for t in (topic_raw, topic_out):
            to_create.append(NewTopic(name=t, num_partitions=1, replication_factor=1))
        admin.create_topics(new_topics=to_create)
        # attendre leur matérialisation
        for _ in range(20):
            if {topic_raw, topic_out}.issubset(set(admin.list_topics())):
                break
            time.sleep(0.25)
    finally:
        admin.close()

    # 4) Importer puis *forcer* les constantes du module (ne pas dépendre de .env)
    mod = importlib.import_module("scripts.etl.transform_aqi_kafka_to_kafka")
    mod = importlib.reload(mod)
    mod.KAFKA_BROKER = broker
    mod.TOPIC_RAW = topic_raw
    mod.TOPIC_OUT = topic_out

    # 5) Publier 3 messages RAW pour un run_id unique
    run_id = f"run_{suffix}"
    producer = KafkaProducer(bootstrap_servers=broker,
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    cities = [f"TParis_{suffix}", f"TLyon_{suffix}", f"TMarseille_{suffix}"]
    base_msg = {
        "time": "2025-01-01T00:00:00+00:00",
        "latitude": 48.0, "longitude": 2.0,
        "european_aqi": 50.0, "pm2_5": 7.5, "pm10": 12.0,
        "carbon_monoxide": 0.1, "sulphur_dioxide": 1.2, "uv_index": 2.0,
        "nitrogen_dioxide": 22.2, "ozone": 33.3,
    }
    try:
        for c in cities:
            producer.send(topic_raw, value={"run_id": run_id, "city": c, **base_msg})
        producer.flush()
    finally:
        producer.close()

    try:
        # 6) Transformer (max_records limité pour accélérer)
        sent = mod.transform_kafka_raw_to_transformed(run_id=run_id, max_records=3)
        assert sent == 3, f"Nombre de messages transformés inattendu: {sent}"

        # 7) Vérifier la sortie
        cons_out = KafkaConsumer(
            topic_out, bootstrap_servers=broker,
            auto_offset_reset="earliest", enable_auto_commit=False,
            group_id=f"it_transform_verifier_{suffix}",
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        try:
            out_msgs = [m.value for m in cons_out]
        finally:
            cons_out.close()

        out_msgs = [m for m in out_msgs if m.get("run_id") == run_id]
        assert len(out_msgs) == 3, f"Nombre de messages en sortie inattendu: {len(out_msgs)}"

        KEEP = {
            "run_id","time","city","latitude","longitude",
            "european_aqi","pm2_5","pm10","carbon_monoxide",
            "sulphur_dioxide","uv_index",
        }
        for m in out_msgs:
            assert set(m.keys()) == KEEP, f"Clés inattendues en sortie: {set(m.keys()) ^ KEEP}"

    finally:
        # 8) Cleanup des topics même en cas d'assertion FAIL
        admin = KafkaAdminClient(bootstrap_servers=broker, client_id="it_transform_admin_cleanup")
        try:
            admin.delete_topics([topic_raw, topic_out])
            for _ in range(20):
                remaining = set(admin.list_topics())
                if topic_raw not in remaining and topic_out not in remaining:
                    break
                time.sleep(0.25)
        except Exception:
            pass
        finally:
            admin.close()
