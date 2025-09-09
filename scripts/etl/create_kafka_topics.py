import os
import socket
from kafka.admin import KafkaAdminClient, NewTopic
from scripts.utils.env_loader import load_env_file



def _check_tcp(host: str, port: int, timeout: float = 5.0) -> None:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return
    except Exception as e:
        raise RuntimeError(f"Kafka broker injoignable sur {host}:{port} → {e}")

def create_kafka_topics():
    load_env_file()

    broker = os.getenv("KAFKA_BROKER") or "kafka:9092"
    topic_raw = os.getenv("TOPIC_AQI_RAW") or os.getenv("TO_RAW") or "aqi_current"
    topic_transf = os.getenv("TOPIC_AQI_TRANSFORMED") or os.getenv("TO_TRANSF") or "aqi_transformed"

    host, port = broker.split(":")
    _check_tcp(host, int(port))  # lève si KO

    try:
        admin = KafkaAdminClient(bootstrap_servers=broker, client_id="topic_creator")
        try:
            existing = set(admin.list_topics())
            to_create = []
            if topic_raw not in existing:
                to_create.append(NewTopic(name=topic_raw, num_partitions=1, replication_factor=1))
            if topic_transf not in existing:
                to_create.append(NewTopic(name=topic_transf, num_partitions=1, replication_factor=1))

            if to_create:
                admin.create_topics(to_create)

            # Vérif finale : les topics doivent exister sinon on lève
            final = set(admin.list_topics())
            missing = [t for t in [topic_raw, topic_transf] if t not in final]
            if missing:
                raise RuntimeError(f"Topics non présents après création: {missing}")

            print(f"✅ Topics OK : {topic_raw}, {topic_transf}")
        finally:
            admin.close()
    except Exception as e:
        # NE PAS avaler l’erreur : laisser Airflow marquer la tâche en échec
        raise
