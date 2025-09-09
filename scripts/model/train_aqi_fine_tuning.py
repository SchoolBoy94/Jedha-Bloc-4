from __future__ import annotations
import os, numpy as np, pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split, RandomizedSearchCV, KFold
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import FunctionTransformer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, r2_score
from scipy.stats import randint, uniform
from lightgbm import LGBMRegressor

import mlflow, mlflow.sklearn
from mlflow.tracking import MlflowClient

from scripts.utils.env_loader import load_env_file

load_env_file()

MLFLOW_URI      = os.getenv("MLFLOW_TRACKING_URI")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT")
TARGET_NAME     = "european_aqi"
REGISTERED_NAME = f"lgbm_{TARGET_NAME}"
DB_CONN         = os.getenv("DB_URI")

engine = create_engine(DB_CONN)
df = pd.read_sql("SELECT * FROM aqi_transform", engine)



"""
Dernier 7 Jours + Données aléatoires pour compléter la Dataset :
"""

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
n_needed = max(0, 1500 - n_recent)

# 5. Prendre des lignes aléatoires en dehors des 7 derniers jours
df_other = df[df["time"] < seven_days_ago]
df_random = df_other.sample(n=min(n_needed, len(df_other)), random_state=42)

# 6. Combiner pour obtenir df1
df1 = pd.concat([df_recent, df_random]).sample(frac=1, random_state=42).reset_index(drop=True)

# 7. Troncature si > 1500 (sécurité)
df1 = df1.head(1500)


# ---------- 2. Séparation features / cible ---------------------------------
y = df1[TARGET_NAME]
pollutants = ["pm2_5","pm10","carbon_monoxide","sulphur_dioxide","uv_index"]
X = df1[pollutants]



# ---------- 3. Pré‑traitement ----------------------------------------------
preprocess = ColumnTransformer(
    [
        ("log", Pipeline([
            ("imp", SimpleImputer(strategy="median")),
            ("log", FunctionTransformer(lambda x: np.log10(x + 1))),
        ]), pollutants),
    ],
    remainder="drop",
)

# Espace des hyperparamètres à explorer
param_dist = {
    "num_leaves": randint(20, 100),
    "learning_rate": uniform(0.01, 0.3),
    "n_estimators": randint(100, 1000),
    "subsample": uniform(0.6, 0.4),
    "colsample_bytree": uniform(0.6, 0.4),
    "min_child_samples": randint(5, 50),
    "reg_alpha": uniform(0.0, 1.0),
    "reg_lambda": uniform(0.0, 1.0),
}

base_lgbm = LGBMRegressor(objective="regression", random_state=42, verbose=-1)

search = RandomizedSearchCV(
    base_lgbm,
    param_distributions=param_dist,
    # n_iter=30,
    n_iter=10,
    scoring="neg_mean_absolute_error",
    # cv=KFold(3, shuffle=True, random_state=42),
    cv=KFold(2, shuffle=True, random_state=42),
    n_jobs=-1,
    random_state=42,
    verbose=1,
)

pipe = Pipeline([
    ("prep", preprocess),
    ("model", search),
])

X_tr, X_val, y_tr, y_val = train_test_split(
    X, y, test_size=0.2, shuffle=True, random_state=42
)

mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment(EXPERIMENT_NAME)
client = MlflowClient()

with mlflow.start_run(run_name="lgbm_aqi_finetuning") as run:
    mlflow.log_param("sample_rows", len(df))

    pipe.fit(X_tr, y_tr)

    search = pipe.named_steps["model"]
    best_params = search.best_params_
    mlflow.log_params(best_params)

    pred = pipe.predict(X_val)
    mae = mean_absolute_error(y_val, pred)
    r2 = r2_score(y_val, pred)

    mlflow.log_metric("MAE_holdout", mae)
    mlflow.log_metric("R2_holdout", r2)

    mlflow.sklearn.log_model(
        sk_model=pipe,
        artifact_path="model",
        registered_model_name=REGISTERED_NAME,
    )

    print(f"🏆 Best params: {best_params}")
    print(f"🔍 Hold-out MAE = {mae:.4f} | R² = {r2:.3f}")

    versions = client.get_latest_versions(REGISTERED_NAME, stages=["None"])
    if versions:
        latest = max(versions, key=lambda v: int(v.version))
        client.transition_model_version_stage(
            name=REGISTERED_NAME,
            version=latest.version,
            stage="Staging",
            archive_existing_versions=False,
        )
        print(f"🚀 Modèle v{latest.version} promu → Staging")
    else:
        print("⚠️ Aucun nouveau modèle à promouvoir.")
