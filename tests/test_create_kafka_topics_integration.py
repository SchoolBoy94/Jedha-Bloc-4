import os
import socket
import time
import pytest
from kafka.admin import KafkaAdminClient

from scripts.utils.env_loader import load_env_file
from scripts.etl.create_kafka_topics import create_kafka_topics


@pytest.mark.integration
def test_create_kafka_topics_integration(monkeypatch):
    """
    IT pour scripts/etl/create_kafka_topics.py en utilisant EXCLUSIVEMENT
    les variables définies dans tests/.env_test.
    """
    # 1) ENV de test
    tests_env = os.path.join(os.getcwd(), "tests", ".env_test")
    monkeypatch.setenv("ENV_FILE", tests_env)
    load_env_file()  # charge .env_test

    broker = os.getenv("KAFKA_BROKER") or "kafka:9092"
    # on lit exactement ce que la fonction utilisera
    topic_raw = os.getenv("TOPIC_AQI_RAW") or os.getenv("TO_RAW") or "aqi_current"
    topic_transf = os.getenv("TOPIC_AQI_TRANSFORMED") or os.getenv("TO_TRANSF") or "aqi_transformed"

    # Sanity checks pour éviter un faux vert si .env_test est incomplet
    assert topic_raw, "TOPIC_AQI_RAW (ou TO_RAW) doit être défini dans tests/.env_test"
    assert topic_transf, "TOPIC_AQI_TRANSFORMED (ou TO_TRANSF) doit être défini dans tests/.env_test"

    # 2) Reachability broker -> sinon skip
    host, port = broker.split(":")
    try:
        s = socket.create_connection((host, int(port)), timeout=5)
        s.close()
    except Exception as e:
        pytest.fail(f"Kafka broker non joignable sur {broker}: {e}")

    # 3) Appel de la fonction à tester (elle relira .env_test aussi)
    create_kafka_topics()

    # 4) Vérifications via l’admin client (petit polling)
    admin = KafkaAdminClient(bootstrap_servers=broker, client_id="it_topics_check")
    try:
        ok = False
        for _ in range(20):
            existing = set(admin.list_topics())
            if topic_raw in existing and topic_transf in existing:
                ok = True
                break
            time.sleep(0.5)
        assert ok, f"Topics manquants: {[t for t in (topic_raw, topic_transf) if t not in existing]}"
    finally:
        admin.close()
