import os
from typing import Any, Dict, List, Union

import pandas as pd
import mlflow.pyfunc
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from scripts.utils.env_loader import load_env_file

# Charge l'env approprié (.env en Airflow, tests/.env_test en Jenkins si ENV_FILE est défini)
load_env_file()

# ─── Configuration ───────────────────────────────────────────────────────────
MODEL_NAME  = os.getenv("AQI_MODEL_NAME", "lgbm_european_aqi")
MODEL_STAGE = os.getenv("AQI_MODEL_STAGE", "Production")
MLFLOW_URI  = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MODEL_URI   = f"models:/{MODEL_NAME}/{MODEL_STAGE}"

mlflow.set_tracking_uri(MLFLOW_URI)

def load_model():
    try:
        return mlflow.pyfunc.load_model(MODEL_URI)
    except Exception as e:
        raise RuntimeError(f"❌ Impossible de charger le modèle {MODEL_URI}: {e}")

model = load_model()

FEATURE_COLUMNS: List[str] = [
    "pm2_5", "pm10", "carbon_monoxide", "sulphur_dioxide", "uv_index"
]

app = FastAPI(
    title="AQI Prediction API",
    description="Prédit le European AQI à partir des polluants.",
    version="1.0.0",
)

# ─── Schémas Pydantic ────────────────────────────────────────────────────────
class PredictRequest(BaseModel):
    data: Union[Dict[str, Any], List[Dict[str, Any]]]

class PredictResponseSingle(BaseModel):
    european_aqi: float

class PredictResponseBatch(BaseModel):
    predictions: List[float]

# ─── Endpoint de prédiction ──────────────────────────────────────────────────
@app.post("/predict", response_model=Union[PredictResponseSingle, PredictResponseBatch])
def predict(req: PredictRequest):
    try:
        # Gestion mono vs multi
        if isinstance(req.data, list):
            df = pd.DataFrame(req.data)
        else:
            df = pd.DataFrame([req.data])

        # Validation colonnes
        missing = [col for col in FEATURE_COLUMNS if col not in df.columns]
        if missing:
            raise HTTPException(status_code=422, detail=f"Colonnes manquantes : {missing}")

        df = df[FEATURE_COLUMNS]
        preds = model.predict(df)

        # Mono ou multi
        if len(preds) == 1:
            return PredictResponseSingle(european_aqi=float(preds[0]))
        else:
            return PredictResponseBatch(predictions=[float(p) for p in preds])

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erreur lors de la prédiction : {e}")

# ─── Health check ────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "model_uri": MODEL_URI}

@app.get("/reload")
def reload_model():
    global model
    try:
        model = load_model()
        return {"message": f"✅ Modèle rechargé depuis {MODEL_URI}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur de rechargement : {e}")
