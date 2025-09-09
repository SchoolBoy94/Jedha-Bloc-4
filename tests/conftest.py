import pytest
from dotenv import load_dotenv
import os

@pytest.fixture(scope="session", autouse=True)
def load_test_env():
    # Charge le fichier .env_test une fois pour toute la session de tests
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env_test")
    load_dotenv(dotenv_path=dotenv_path, override=True)

































# tests/conftest.py
# import pytest
# from dotenv import load_dotenv

# @pytest.fixture(scope="session", autouse=True)
# def load_test_env():
#     dotenv_loaded = load_dotenv(dotenv_path="/app/tests/.env_test", override=True)
#     if not dotenv_loaded:
#         print("⚠️ Le fichier .env_test non trouvé !")




# import pytest
# from dotenv import load_dotenv
# import os

# @pytest.fixture(scope="session", autouse=True)
# def load_test_env():
#     dotenv_loaded = load_dotenv(dotenv_path="/app/tests/.env_test", override=True)
#     if not dotenv_loaded:
#         print("⚠️ Le fichier .env_test non trouvé !")
#     else:
#         print("📂 Fichier .env_test chargé depuis: /app/tests/.env_test")

#     # Vérification des variables d'environnement après le chargement
#     print("🚀 Vérification des variables d'environnement :")
#     print(f"KAFKA_BROKER={os.getenv('KAFKA_BROKER')}")
#     print(f"TOPIC_RAW={os.getenv('TOPIC_RAW')}")
#     print(f"TOPIC_TRANSFORMED={os.getenv('TOPIC_TRANSFORMED')}")












# import pytest

# @pytest.fixture(scope="function", autouse=True)
# def clean_env(monkeypatch):
#     monkeypatch.delenv("KAFKA_BROKER", raising=False)
#     monkeypatch.delenv("TOPIC_RAW", raising=False)
#     monkeypatch.delenv("TOPIC_TRANSFORMED", raising=False)


# """
# avant chaque test, les variables comme KAFKA_BROKER, TOPIC_RAW, etc. sont temporairement supprimées du système

# si un script dans /scripts/ tente de lire ces variables via os.getenv(...), il recevra None

# 👉 Cela peut provoquer une erreur si ces variables sont requises et non protégées par une valeur par défaut
# """