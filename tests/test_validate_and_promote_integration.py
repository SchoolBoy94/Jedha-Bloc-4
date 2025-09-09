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
def test_validate_promote_aqi_integration(monkeypatch):
    """
    IT pour scripts/model/validate_and_promote_aqi.py :

      - charge .env_test
      - vérifie reachability Postgres & MLflow (FAIL si KO)
      - crée/garantit la table aqi_transform
      - insère des données synthétiques récentes
      - lance l'entraînement (train_aqi) pour créer une version Staging
      - exécute evaluate_on_full_data() (validation + promotion)
      - vérifie qu'une version Production existe
      - cleanup : suppression des lignes insérées en base
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
    model_name = "lgbm_european_aqi"
    assert tracking_uri and experiment_name, "MLFLOW_TRACKING_URI / MLFLOW_EXPERIMENT manquants dans .env_test"

    parsed = urlparse(tracking_uri)
    if parsed.scheme in ("http", "https"):
        h, p = parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)
        try:
            with socket.create_connection((h, int(p)), timeout=5):
                pass
        except Exception as e:
            pytest.fail(f"MLflow injoignable ({tracking_uri}) : {e}")
    else:
        # Le Model Registry n'est pas supporté en file:// → ce test nécessite http(s)
        pytest.skip("MLflow Model Registry requis (tracking http/https).")

    # 2) Créer la table aqi_transform si nécessaire (train/validate lisent ce nom en dur)
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

    # 3) Injecter des données récentes (fenêtre couverte par train/validate)
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

    # 4) Entraîner pour garantir une version en Staging
    train_mod = importlib.import_module("scripts.model.train_aqi")
    train_mod = importlib.reload(train_mod)

    # 5) Lancer la validation/promotion
    val_mod = importlib.import_module("scripts.model.validate_and_promote_aqi")
    val_mod = importlib.reload(val_mod)
    val_mod.evaluate_on_full_data()

    # 6) Vérifications côté MLflow
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()

    # Expérience présente
    exp = client.get_experiment_by_name(experiment_name)
    assert exp is not None, f"Expérience MLflow introuvable: {experiment_name}"

    # Production doit exister après exécution (si inexistante avant, la fonction promeut)
    ok_prod = False
    for _ in range(20):
        prod_vs = client.get_latest_versions(model_name, stages=["Production"])
        if prod_vs:
            ok_prod = True
            break
        time.sleep(0.5)
    assert ok_prod, "Aucune version Production trouvée après validation/promotion."

    # 7) Cleanup des lignes insérées (on garde les artefacts MLflow)
    try:
        with psycopg2.connect(**db_params) as c, c.cursor() as cur:
            cur.execute(
                "DELETE FROM aqi_transform WHERE city = ANY(%s)",
                (cities,)
            )
            c.commit()
    except Exception:
        pass  # best effort
