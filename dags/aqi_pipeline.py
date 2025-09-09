from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys, os
sys.path.append("/opt/airflow/scripts")   # ← Airflow voit maintenant tes scripts

from scripts.etl import current_aqi_to_kafka as produce
from scripts.etl import insert_in_table_raw as load_raw
from scripts.etl import transform_aqi_kafka_to_kafka as transform
from scripts.etl import insert_in_table_transform as load_trans
from scripts.notifications import notification_erreur_dag as notification


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': notification.send_discord_notification,
}

with DAG(
    dag_id="aqi_pipeline",
    default_args=default_args,
    schedule_interval=None,
    # start_date=datetime(2025, 7, 21),     # adapte si nécessaire
    schedule_interval="@hourly",      
    catchup=False,
    tags=["aqi", "kafka", "etl"],
) as dag:

    # 1) produit les messages AQI (current)
    current_aqi_kafka = PythonOperator(
        task_id="current_aqi_kafka",
        python_callable=produce.push_current_aqi_to_kafka,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    # 2) consomme le topic brut et insère dans table raw
    insert_table_raw = PythonOperator(
        task_id="insert_table_raw",
        python_callable=load_raw.consume_kafka_to_table,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    # 3) transforme le topic brut → topic transformé
    transform_kafka = PythonOperator(
        task_id="transform",
        python_callable=transform.transform_kafka_raw_to_transformed,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    # 4) consomme le topic transformé et insère dans table transform
    insert_table_transform = PythonOperator(
        task_id="insert_table_transform",
        python_callable=load_trans.insert_from_kafka,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    # Dépendances : current → [raw, transform] → transform load
    current_aqi_kafka >> [insert_table_raw, transform_kafka] >> insert_table_transform