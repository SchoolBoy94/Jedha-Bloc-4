import os
from dotenv import load_dotenv
import sys
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd
import mlflow.pyfunc
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, RegressionPreset
from evidently.ui.workspace import Workspace
from scripts.utils.env_loader import load_env_file

load_env_file()

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI")
MODEL_URI           = os.getenv("MLFLOW_MODEL_URI")
DB_CONN             = os.getenv("DB_URI")
if not DB_CONN:
    raise ValueError("❌ La variable d'environnement DB_URI est vide ou non définie.")

WORKSPACE_PATH = Path(os.getenv("EVIDENTLY_WORKSPACE_PATH"))
PROJECT_NAME   = "AQI monitoring"
HOLDOUT_DAYS   = os.getenv("EVIDENTLY_HOLDOUT_DAYS")

def load_slices():
    engine = create_engine(DB_CONN)
    df = pd.read_sql("SELECT * FROM aqi_transform", engine)
    print("Colonnes SQL chargées :", df.columns)
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"])

    today = datetime.utcnow()
    current_month = pd.Period(today.replace(day=1), freq="M")  # mois précédent
    reference_month = current_month - 1     

    df["month_period"] = df["time"].dt.to_period("M")
    current = df[df["month_period"] == current_month].sort_values("time", ascending=False)
    reference = df[df["month_period"] == reference_month].sort_values("time", ascending=False)

    return reference, current

def push_snapshot_to_ui(report: Report) -> None:
    ws = Workspace.create(path=str(WORKSPACE_PATH))
    found = ws.search_project(PROJECT_NAME)
    project = found[0] if found else ws.create_project(PROJECT_NAME)
    ws.add_report(project.id, report)
    print(f"✓ Snapshot ajouté au projet « {PROJECT_NAME} »")

def timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H-%M")

def main() -> None:
    ref_df, cur_df = load_slices()

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model = mlflow.pyfunc.load_model(MODEL_URI)

    feat_cols = ["pm2_5", "pm10", "carbon_monoxide", "sulphur_dioxide", "uv_index"]
    for df in (ref_df, cur_df):
        df["prediction"] = model.predict(df[feat_cols])
        df["target"]     = df["european_aqi"]

    report = Report(metrics=[DataDriftPreset(), RegressionPreset()])
    report.run(reference_data=ref_df, current_data=cur_df)

    push_snapshot_to_ui(report)

    ts = timestamp()
    html_path = WORKSPACE_PATH / f"drift-regression-report_{ts}.html"
    json_path = WORKSPACE_PATH / f"drift-regression-report_{ts}.json"

    report.save_html(str(html_path))
    report.save_json(str(json_path))

    print(f"✓ Rapports Evidently générés :\n   ├── {html_path}\n   └── {json_path}")

if __name__ == "__main__":
    main()
