import pytest
from unittest import mock
import os
from scripts.notifications.notif_quality_air import notify_if_poor, _category


# Valeur fixe pour le webhook URL
os.environ["DISCORD_WEBHOOK_URL"] = "https://discord.com/api/webhooks/XXXXX/your-webhook-url"

# Test de la fonction _category
@pytest.mark.parametrize("value, pollutant, expected_category", [
    (30, "pm2_5", "poor"),
    (60, "pm2_5", "very_poor"),
    (80, "pm2_5", "extremely_poor"),
    (90, "pm10", "poor"),
    (200, "nitrogen_dioxide", "poor"),
    (1000, "ozone", "extremely_poor"),
    (10, "sulphur_dioxide", None),
])
def test_category(value, pollutant, expected_category):
    result = _category(value, pollutant)
    assert result == expected_category


# Test de la fonction notify_if_poor
@mock.patch("scripts.notifications.notif_quality_air.requests.post")
def test_notify_if_poor(mock_post):
    # Cas où un polluant dépasse le seuil
    record = {
        "city": "Paris",
        "time": "2025-08-30 12:00:00",
        "pm2_5": 30,
        "pm10": 120,
        "nitrogen_dioxide": 200,
        "ozone": 150,
        "sulphur_dioxide": 400,
    }

    # Mock de la fonction post pour vérifier qu'elle a été appelée
    mock_post.return_value.status_code = 200  # Simuler une requête réussie

    # Appeler la fonction
    notify_if_poor(record)

    # Vérifier que la fonction post a bien été appelée
    assert mock_post.call_count == 5  # Il y a 5 polluants à vérifier
    for call in mock_post.call_args_list:
        # Vérifier que le message contient bien les informations
        args, kwargs = call
        message = kwargs["json"]["content"]
        assert "Qualité de l’air – Paris" in message
        assert "PM2_5" in message or "PM10" in message  # Vérification correcte du format


# Test de la gestion d'erreur : Si la requête échoue (ex : mauvais URL Discord)
@mock.patch("scripts.notifications.notif_quality_air.requests.post")
def test_notify_if_poor_error(mock_post):
    # Cas où un polluant dépasse le seuil
    record = {
        "city": "Paris",
        "time": "2025-08-30 12:00:00",
        "pm2_5": 30,
    }

    # Simuler une erreur lors de l'envoi de la requête
    mock_post.side_effect = Exception("Discord error")

    # Appeler la fonction
    with mock.patch("builtins.print") as mock_print:  # Mock print pour vérifier les erreurs
        notify_if_poor(record)
        mock_print.assert_called_with("⚠️  Erreur Discord : Discord error")


# Test avec webhook URL non défini
@mock.patch("scripts.notifications.notif_quality_air.requests.post")
def test_notify_if_poor_no_webhook(mock_post):
    # Cas où le webhook n'est pas défini
    os.environ["DISCORD_WEBHOOK_URL"] = ""  # Simuler l'absence de webhook
    record = {
        "city": "Paris",
        "time": "2025-08-30 12:00:00",
        "pm2_5": 30,
    }

    # Appeler la fonction
    notify_if_poor(record)

    # Vérifier que la fonction post n'a pas été appelée
    mock_post.assert_not_called()
