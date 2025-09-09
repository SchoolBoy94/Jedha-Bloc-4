import os
import requests
from dotenv import load_dotenv


from scripts.utils.env_loader import load_env_file

# Charge l'env approprié (.env en Airflow, tests/.env_test en Jenkins si ENV_FILE est défini)
load_env_file()

DISCORD_URL = os.getenv("DISCORD_WEBHOOK_URL")  # à définir dans l’env.




def send_discord_notification(context):
    """
    Envoie une notification Discord quand une tâche Airflow échoue.
    """
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    content = (
        f"🚨 **ALERTE DAG Airflow** 🚨\n"
        f"DAG: `{dag_id}`\n"
        f"Tâche: `{task_id}` a échoué\n"
        f"Date d'exécution: {execution_date}\n"
        f"[Voir les logs]({log_url})"
    )

    data = {"content": content}
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=data)
        if response.status_code != 204:
            print(f"Erreur Discord: {response.status_code} {response.text}")
    except Exception as e:
        print(f"Exception lors de l'envoi Discord: {e}")
