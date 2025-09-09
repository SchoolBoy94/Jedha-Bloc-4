import os
from dotenv import load_dotenv
import json
from typing import Dict, Tuple, Optional
import requests


from scripts.utils.env_loader import load_env_file

# Charge l'env approprié (.env en Airflow, tests/.env_test en Jenkins si ENV_FILE est défini)
load_env_file()

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")  # à définir dans l’env.


# seuils (min, max] – ouverts à gauche, fermés à droite
THRESHOLDS: Dict[str, Dict[str, Tuple[float, float]]] = {
    "pm2_5":          {"poor": (25, 50),  "very_poor": (50, 75),  "extremely_poor": (75, float("inf"))},
    "pm10":           {"poor": (50, 100), "very_poor": (100, 150),"extremely_poor": (150, float("inf"))},
    "nitrogen_dioxide":{"poor": (120, 230),"very_poor": (230, 340),"extremely_poor": (340, float("inf"))},
    "ozone":          {"poor": (130, 240),"very_poor": (240, 380),"extremely_poor": (380, float("inf"))},
    "sulphur_dioxide":{"poor": (350, 500),"very_poor": (500, 750),"extremely_poor": (750, float("inf"))},
}

EMOJI = {"poor": "🟥", "very_poor": "🟥 🟥", "extremely_poor": "🟥 🟥 🟥"}


def _category(value: float, pollutant: str) -> Optional[str]:
    """Retourne la catégorie ('poor', ...) ou None si acceptable."""
    for cat, (low, high) in THRESHOLDS.get(pollutant, {}).items():
        if low < value <= high:
            return cat
    return None


def notify_if_poor(record: Dict[str, object]) -> None:
    """Envoie une notification Discord si un polluant est ≥ catégorie 'Poor'."""
    if not WEBHOOK_URL:
        # Pas de webhook → on sort silencieusement pour ne pas bloquer l’app
        return

    city = record.get("city", "Unknown")
    ts   = record.get("time", "")

    for pollutant in THRESHOLDS:
        value = record.get(pollutant)
        if value is None:
            continue
        cat = _category(float(value), pollutant)
        if cat:
            msg = (
                f"{EMOJI[cat]} **Qualité de l’air – {city}**\n"
                f"> {pollutant.upper()} : **{value:.1f} µg/m³** – {cat.replace('_', ' ').title()}\n"
                f"> {ts}"
            )
            try:
                requests.post(WEBHOOK_URL, json={"content": msg}, timeout=10)
                # Log simple sur STDOUT ; à adapter si tu as un logger
            except Exception as exc:
                print(f"⚠️  Erreur Discord : {exc}")
