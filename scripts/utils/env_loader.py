import os
from pathlib import Path
from dotenv import load_dotenv

def load_env_file(default=".env") -> str:
    """
    Charge un fichier d'environnement *unique* en respectant l'ordre :
    1) ENV_FILE explicite si défini (recommandé en CI/CD)
    2) Contexte tests/Jenkins -> tests/.env_test si présent
    3) Fallback -> .env (prod/dev)

    Retourne le chemin effectivement chargé.
    """
    # 1) Priorité à ENV_FILE si fourni
    explicit = os.getenv("ENV_FILE")
    if explicit and Path(explicit).exists():
        load_dotenv(dotenv_path=explicit, override=True)
        print(f"🔧 ENV chargé : {explicit}")
        return explicit

    # 2) Contexte tests/Jenkins → bascule automatique vers .env_test si dispo
    likely_test = (
        os.getenv("PYTEST_CURRENT_TEST") is not None
        or os.getenv("CI") is not None
        or os.getenv("JENKINS_HOME") is not None
    )
    if likely_test:
        # chemins classiques selon l’endroit d’où on lance
        for candidate in ["tests/.env_test", ".env_test", "/var/jenkins_home/workspace/tests/.env_test"]:
            if Path(candidate).exists():
                load_dotenv(dotenv_path=candidate, override=True)
                print(f"🧪 ENV test chargé : {candidate}")
                return candidate

    # 3) Fallback → .env
    path = default if Path(default).exists() else None
    if path:
        load_dotenv(dotenv_path=path, override=True)
        print(f"✅ ENV par défaut chargé : {path}")
        return path

    print("⚠️ Aucun fichier .env/.env_test trouvé — variables système seulement.")
    return ""
