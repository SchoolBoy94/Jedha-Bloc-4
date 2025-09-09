from __future__ import annotations
import os, sys, argparse
from sqlalchemy import create_engine
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd
from sklearn.metrics import mean_absolute_error
import mlflow
from mlflow.tracking import MlflowClient

# ---------------------- Configuration --------------------------------------
from scripts.utils.env_loader import load_env_file

# Charge l'env approprié (.env en Airflow, tests/.env_test en Jenkins si ENV_FILE est défini)
load_env_file()

MLFLOW_URI      = os.getenv("MLFLOW_TRACKING_URI")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT")
TARGET          = "european_aqi"
MODEL_NAME      = f"lgbm_{TARGET}"
DB_CONN = os.getenv("DB_URI")
# ---------------------------------------------------------------------------

def _load_and_clean_csv(conn_str: str) -> pd.DataFrame:
    engine = create_engine(conn_str)
    df = pd.read_sql("SELECT * FROM aqi_transform", engine)
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    return df


def evaluate_on_full_data():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    client = MlflowClient()

    # ---- 1️⃣  Récupération du modèle Staging --------------------------------
    staging_vs = client.get_latest_versions(MODEL_NAME, stages=["Staging"])
    if not staging_vs:
        sys.exit(f"❌  Aucun modèle « {MODEL_NAME} » en Staging.")
    st_v = staging_vs[0]
    print(f"🔍  Staging → v{st_v.version} (run {st_v.run_id})")

    # ---- 2️⃣  Chargement des données complètes -------------------------------
    df = _load_and_clean_csv(DB_CONN)
    if df.empty:
        sys.exit("❌  Aucune donnée disponible dans la table `aqi_transform`.")
    
    y = df[TARGET]
    X = df.drop(columns=[TARGET, "time"])
    print(f"📦  Données totales utilisées pour l'évaluation : {len(df)} lignes.")

    # ---- 3️⃣  Évaluation du modèle Staging -----------------------------------
    st_model = mlflow.pyfunc.load_model(f"runs:/{st_v.run_id}/model")
    mae_st = mean_absolute_error(y, st_model.predict(X))
    print(f"• MAE Staging  v{st_v.version} = {mae_st:.4f}")

    # ---- 4️⃣  Comparaison avec Production (si elle existe) -------------------
    prod_vs = client.get_latest_versions(MODEL_NAME, stages=["Production"])
    if prod_vs:
        pr_v = prod_vs[0]
        mae_pr = client.get_run(pr_v.run_id).data.metrics.get("MAE_holdout")  # Peut être None
        print(f"• MAE Production v{pr_v.version} = {mae_pr:.4f}" if mae_pr else "• Pas de MAE pour Production.")
    else:
        mae_pr = None
        print("• Pas de version Production existante.")

    # ---- 5️⃣  Promotion si meilleur ------------------------------------------
    if mae_pr is None or mae_st < mae_pr:
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=st_v.version,
            stage="Production",
            archive_existing_versions=True,
        )
        print(f"✅  Modèle v{st_v.version} promu en Production 🚀")
    else:
        print("🚫  Pas de promotion : MAE Staging ≥ MAE Production")


# ---------------------- CLI -------------------------------------------------
if __name__ == "__main__":
    evaluate_on_full_data()
