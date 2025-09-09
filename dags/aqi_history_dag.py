import sys
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
# Pour que Airflow trouve vos modules dans /scripts
sys.path.append("/opt/airflow/scripts")

# Import SANS alias ambigus
from scripts.etl import create_tables as create_tables_mod
from scripts.etl import create_kafka_topics as create_kafka_mod
from scripts.etl import load_aqi_csv_to_table_raw as load_raw_mod
from scripts.etl import load_aqi_csv_to_table_transform as load_trans_mod

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}


# Dates de l’historique à récupérer (modifier au besoin)
START_DATE = "2025-07-01"
END_DATE = "2025-09-06"

# Fichier de sortie CSV
CSV_PATH = "/opt/airflow/data/aqi_2025.csv"



with DAG(
    dag_id="aqi_history_dag",
    default_args=default_args,
    description="Créer tables + charger CSV raw + charger CSV transform",
    schedule_interval="@once",  # ajustez si besoin
    catchup=False,
    tags=["aqi", "postgres", "csv"],
) as dag:



    # 1. Télécharger l’historique AQI et générer aqi_all.csv
    fetch_csv = BashOperator(
        task_id="fetch_aqi_history_to_csv",
        bash_command=(
            f"python3 /opt/airflow/scripts/etl/history_aqi_to_csv.py "
            f"--start {START_DATE} --end {END_DATE} --out {CSV_PATH}"
        )
    )

    create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables_mod.create_tables,  # lit l’ENV dans le script
    )

    creation_kafka_topic = PythonOperator(
        task_id="create_kafka",
        python_callable=create_kafka_mod.create_kafka_topics,  # lit l’ENV dans le script
    )

    load_raw = PythonOperator(
        task_id="load_csv_to_aqi_raw",
        python_callable=load_raw_mod.load_csv_to_postgres,  # lit CSV_PATH dans l’ENV
    )

    load_transform = PythonOperator(
        task_id="load_csv_to_aqi_transform",
        python_callable=load_trans_mod.load_transform_csv,  # lit CSV_PATH dans l’ENV
    )

    creation_kafka_topic >> create_tables >> fetch_csv >> load_raw >> load_transform
    