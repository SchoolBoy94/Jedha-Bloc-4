from __future__ import annotations
import os, numpy as np, pandas as pd
from sqlalchemy import create_engine
import re
from dotenv import load_dotenv

from sklearn.model_selection import (
    train_test_split, RandomizedSearchCV, KFold
)
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import FunctionTransformer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, r2_score
from scipy.stats import randint, uniform
from lightgbm import LGBMRegressor

# 🔗----------------- MLflow -------------------------------------------------
import mlflow, mlflow.sklearn
from mlflow.tracking import MlflowClient
from pathlib import Path
from scripts.utils.env_loader import load_env_file

# Charge l'env approprié (.env en Airflow, tests/.env_test en Jenkins si ENV_FILE est défini)
load_env_file()


MLFLOW_URI      = os.getenv("MLFLOW_TRACKING_URI")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT")
TARGET_NAME     = "european_aqi"
REGISTERED_NAME = f"lgbm_{TARGET_NAME}"
DB_CONN = os.getenv("DB_URI")

# ---------------------------------------------------------------------------

engine = create_engine(DB_CONN)

df = pd.read_sql("SELECT * FROM aqi_transform", engine)
df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")

# 1. Trier par date décroissante
df = df.sort_values("time", ascending=False).reset_index(drop=True)

# 2. Définir les 7 derniers jours à partir de la date max
max_time = df["time"].max()
seven_days_ago = max_time - pd.Timedelta(days=7)

# 3. Filtrer les données des 7 derniers jours
df_recent = df[df["time"] >= seven_days_ago]

# 4. S'assurer qu'on ne dépasse pas 1500 lignes
n_recent = len(df_recent)
# n_needed = max(0, 1500 - n_recent)
n_needed = max(0, 400 - n_recent)

# 5. Prendre des lignes aléatoires en dehors des 7 derniers jours
df_other = df[df["time"] < seven_days_ago]
df_random = df_other.sample(n=min(n_needed, len(df_other)), random_state=42)

# 6. Combiner pour obtenir df1
df1 = pd.concat([df_recent, df_random]).sample(frac=1, random_state=42).reset_index(drop=True)

# 7. Troncature si > 1500 (sécurité)
df1 = df1.head(400)


# ---------- 2. Séparation features / cible ---------------------------------
y = df1[TARGET_NAME]

pollutants = ["pm2_5","pm10","carbon_monoxide","sulphur_dioxide","uv_index"]
X = df1[pollutants]

# ---------- 3. Pré‑traitement ----------------------------------------------
log_transform = FunctionTransformer(lambda x: np.log10(x + 1))

preprocess = ColumnTransformer(
    [
        ("log", Pipeline([
            ("imp", SimpleImputer(strategy="median")),
            ("log", FunctionTransformer(lambda x: np.log10(x + 1))),
        ]), pollutants),
    ],
    remainder="drop",
)


# ---------- 4. Modèle LightGBM sans recherche d’hyper‑paramètres ----------
lgbm_params = {
    "objective":          "regression",
    "metric":             "l1",          # MAE
    "n_estimators":       600,
    "learning_rate":      0.05,
    "num_leaves":         63,
    "subsample":          0.8,
    "colsample_bytree":   0.8,
    "random_state":       42,
    "verbose":            -1,
}

model = LGBMRegressor(**lgbm_params)

pipe = Pipeline([
    ("prep", preprocess),
    ("model", model),
])

# ---------- 5. Entraînement + log MLflow ----------------------------------
X_tr, X_val, y_tr, y_val = train_test_split(
    X, y, test_size=0.2, shuffle=True, random_state=42
)

mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment(EXPERIMENT_NAME)
client = MlflowClient()

with mlflow.start_run(run_name="lgbm_aqi_fixed") as run:

    # log des infos de dataset et des hyper‑paramètres fixés
    mlflow.log_param("sample_rows", len(df))
    # mlflow.log_param("sample_rows", len(df_1))
    mlflow.log_params(lgbm_params)

    pipe.fit(X_tr, y_tr)

    pred = pipe.predict(X_val)
    mae  = mean_absolute_error(y_val, pred)
    r2   = r2_score(y_val, pred)

    mlflow.log_metric("MAE_holdout", mae)
    mlflow.log_metric("R2_holdout",  r2)

    mlflow.sklearn.log_model(
        sk_model=pipe,
        artifact_path="model",
        registered_model_name=REGISTERED_NAME,
    )

    print(f"🔍 Hold‑out MAE = {mae:.4f} | R² = {r2:.3f}")

    # -------- Promotion en Staging ---------------------------------------
    versions = client.get_latest_versions(REGISTERED_NAME, stages=["None"])
    if versions:
        latest = max(versions, key=lambda v: int(v.version))
        client.transition_model_version_stage(
            name=REGISTERED_NAME,
            version=latest.version,
            stage="Staging",
            archive_existing_versions=False,
        )
        print(f"🚀 Modèle v{latest.version} promu → Staging")
    else:
        print("⚠️  Aucun nouveau modèle à promouvoir.")
