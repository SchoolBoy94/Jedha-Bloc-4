from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import sys
import os
import requests

# Ajouter le chemin du dossier contenant ton script
sys.path.append("/opt/airflow/scripts")  # adapte ce chemin si besoin

from etl.transform_data_quality import quality_data
from scripts.notifications import notification_erreur_dag as notification
from dotenv import load_dotenv

load_dotenv()


# ──────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "retries": 0,
    'start_date': days_ago(1),
    "retry_delay": timedelta(minutes=5),
    'on_failure_callback': notification.send_discord_notification,
}

with DAG(
    dag_id="dag_Data_Quality",
    default_args=default_args,
    schedule_interval="0 0,6 * * *",
    catchup=False,
    tags=["data_quality", "aqi"],
) as dag:

    check_quality = PythonOperator(
        task_id="check_aqi_transform_quality",
        python_callable=quality_data,
    )

    check_quality