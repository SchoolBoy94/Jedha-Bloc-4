from __future__ import annotations

import os
import socket
import time
import uuid
import importlib
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta

import pytest
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
import mlflow
from mlflow.tracking import MlflowClient

from scripts.utils.env_loader import load_env_file


@pytest.mark.integration
def test_train_aqi_integration(monkeypatch):
    """
    IT pour scripts/model/train_aqi.py :

      - charge l'ENV de test (.env_test)
      - vérifie la reachability Postgres & MLflow (FAIL si KO)
      - s'assure que la table 'aqi_transform' existe
      - insère des données synthétiques propres
      - importe le module train_aqi (ce qui lance l'entraînement + log MLflow)
      - vérifie côté MLflow : run créé + modèle enregistré (et promu Staging si registry active)
      - cleanup : supprime uniquement les lignes insérées dans aqi_transform
    """
    # 1) ENV de test
    tests_env = os.path.join(os.getcwd(), "tests", ".env_test")
    monkeypatch.setenv("ENV_FILE", tests_env)
    load_env_file()

    # --- Postgres (.env_test) ---
    host = os.getenv("POSTGRES_HOST")
    dbname = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    port = os.getenv("POSTGRES_PORT")
    assert all([host, dbname, user, port]), "Variables POSTGRES_* manquantes dans tests/.env_test"
    db_params = {"host": host, "database": dbname, "user": user, "password": password, "port": port}

    # Reachability DB
    try:
        with psycopg2.connect(**db_params):
            pass
    except Exception as e:
        pytest.fail(f"Postgres injoignable: {e}")

    # --- MLflow (.env_test) ---
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    experiment_name = os.getenv("MLFLOW_EXPERIMENT")
    assert tracking_uri and experiment_name, "MLFLOW_TRACKING_URI / MLFLOW_EXPERIMENT manquants dans .env_test"

    parsed = urlparse(tracking_uri)
    if parsed.scheme in ("http", "https"):
        h, p = parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)
        try:
            with socket.create_connection((h, int(p)), timeout=5):
                pass
        except Exception as e:
            pytest.fail(f"MLflow injoignable ({tracking_uri}) : {e}")
    # Si file:// c’est supporté pour le tracking, mais pas le Model Registry.
    # On continue (les assertions registry seront alors conditionnelles).

    # 2) S'assurer que la table 'aqi_transform' existe (train_aqi lit *ce nom en dur*)
    ddl = """
    CREATE TABLE IF NOT EXISTS aqi_transform (
        id SERIAL PRIMARY KEY,
        time TIMESTAMPTZ NOT NULL,
        city TEXT NOT NULL,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        european_aqi NUMERIC,
        pm2_5 NUMERIC,
        pm10 NUMERIC,
        carbon_monoxide NUMERIC,
        sulphur_dioxide NUMERIC,
        uv_index NUMERIC
    );
    """
    with psycopg2.connect(**db_params) as c, c.cursor() as cur:
        cur.execute(ddl)
        c.commit()

    # 3) Injecter des données propres & récentes (pour que l’entraînement tienne)
    suffix = uuid.uuid4().hex[:8]
    base_ts = datetime.now(tz=timezone.utc) - timedelta(days=3)
    cities = [f"IT_PARIS_{suffix}", f"IT_LYON_{suffix}", f"IT_MARSEILLE_{suffix}"]

    rows = []
    for i in range(60):  # 60 lignes
        ts = base_ts + timedelta(hours=i % 24)
        city = cities[i % len(cities)]
        rows.append((
            ts.isoformat(),
            city,
            48.8 if "PARIS" in city else (45.76 if "LYON" in city else 43.3),
            2.35 if "PARIS" in city else (4.83 if "LYON" in city else 5.37),
            50.0 + (i % 5),
            7.5 + (i % 3) * 0.1,
            12.0 + (i % 4) * 0.2,
            0.10 + (i % 2) * 0.01,
            1.2 + (i % 3) * 0.05,
            2.0 + (i % 3) * 0.1,
        ))

    insert_cols = [
        "time", "city", "latitude", "longitude",
        "european_aqi", "pm2_5", "pm10",
        "carbon_monoxide", "sulphur_dioxide", "uv_index"
    ]
    with psycopg2.connect(**db_params) as c, c.cursor() as cur:
        insert_sql = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier("aqi_transform"),
            sql.SQL(", ").join(map(sql.Identifier, insert_cols))
        )
        execute_values(cur, insert_sql.as_string(c), rows)
        c.commit()

    # 4) Lancer l’entraînement (le script exécute au moment de l’import)
    #    → nécessite DB_URI/MLFLOW_* correctement paramétrés dans .env_test
    mod = importlib.import_module("scripts.model.train_aqi")
    mod = importlib.reload(mod)  # exécute le code d’entraînement

    # 5) Vérif côté MLflow : run créé + (optionnel) version promue
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()

    # Vérifier qu’on a au moins un run dans l’expérience
    exp = client.get_experiment_by_name(experiment_name)
    assert exp is not None, f"Expérience MLflow introuvable: {experiment_name}"
    # Petit poll pour laisser le serveur enregistrer
    ok_runs = False
    for _ in range(10):
        runs = client.search_runs(exp.experiment_id, order_by=["attributes.start_time DESC"], max_results=1)
        if runs:
            ok_runs = True
            break
        time.sleep(0.5)
    assert ok_runs, "Aucun run trouvé dans l’expérience après entraînement."

    # Si Registry supporté (URI http/https), vérifier l’enregistrement du modèle
    if parsed.scheme in ("http", "https"):
        reg_name = "lgbm_european_aqi"
        # Poll pour laisser la version se créer et être promue
        ok_registry = False
        for _ in range(20):
            try:
                # on accepte Staging ou None si la promotion est différée
                latest_staging = client.get_latest_versions(reg_name, stages=["Staging"])
                latest_none = client.get_latest_versions(reg_name, stages=["None"])
                if latest_staging or latest_none:
                    ok_registry = True
                    break
            except Exception:
                # le modèle peut ne pas exister juste après → on repoll
                pass
            time.sleep(0.5)
        assert ok_registry, "Modèle non trouvé dans le Model Registry (ni Staging, ni None)."

    # 6) Cleanup : supprimer uniquement nos lignes
    try:
        with psycopg2.connect(**db_params) as c, c.cursor() as cur:
            cur.execute(
                "DELETE FROM aqi_transform WHERE city = ANY(%s)",
                (cities,)
            )
            c.commit()
    except Exception:
        pass  # best effort
