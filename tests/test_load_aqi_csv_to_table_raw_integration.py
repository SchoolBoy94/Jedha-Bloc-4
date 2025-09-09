from __future__ import annotations
import os, uuid, tempfile, csv, importlib
import pytest
import psycopg2
from psycopg2 import sql

from scripts.utils.env_loader import load_env_file


@pytest.mark.integration
def test_load_aqi_csv_to_table_raw_integration(monkeypatch):
    # 1) ENV de test
    tests_env = os.path.join(os.getcwd(), "tests", ".env_test")
    monkeypatch.setenv("ENV_FILE", tests_env)
    load_env_file()

    db_params = {
        "host": os.getenv("POSTGRES_HOST"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "port": os.getenv("POSTGRES_PORT"),
    }
    table_raw = os.getenv("TABLE_AQI_RAW")
    assert table_raw, "TABLE_AQI_RAW doit être défini dans tests/.env_test"

    # Vérifie que la DB est joignable
    try:
        with psycopg2.connect(**db_params):
            pass
    except Exception as e:
        pytest.fail(f"Postgres injoignable: {e}")

    # 2) Prépare un CSV temporaire avec des villes uniques (pour cleanup ciblé)
    suffix = uuid.uuid4().hex[:8]
    city1 = f"IT_Paris_{suffix}"
    city2 = f"IT_Lyon_{suffix}"

    headers = ["city", "latitude", "longitude", "time"]  # colonnes minimales
    rows = [
        [city1, 48.8566, 2.3522, "2025-01-01T00:00:00Z"],
        [city2, 45.7640, 4.8357, "2025-01-01T01:00:00Z"],
    ]

    tmp_csv = tempfile.NamedTemporaryFile(mode="w", newline="", delete=False, suffix=".csv")
    try:
        w = csv.writer(tmp_csv)
        w.writerow(headers)
        w.writerows(rows)
        tmp_csv.flush()
        tmp_csv.close()

        # 3) Import/reload du module et lance l'insertion
        mod = importlib.import_module("scripts.etl.load_aqi_csv_to_table_raw")
        mod = importlib.reload(mod)

        inserted = mod.load_csv_to_postgres(csv_path=tmp_csv.name)
        assert inserted == 2, f"Nombre de lignes insérées inattendu: {inserted}"

        # 4) Vérifie en base (en utilisant le nom de table lu dans l'ENV)
        with psycopg2.connect(**db_params) as c, c.cursor() as cur:
            query = sql.SQL("SELECT count(*) FROM {} WHERE city IN (%s, %s)").format(
                sql.Identifier(table_raw)
            )
            cur.execute(query, (city1, city2))
            count = cur.fetchone()[0]
        assert count == 2, f"Lignes retrouvées en base: {count} (attendu 2)"

    finally:
        # 5) Cleanup ciblé: supprime uniquement les lignes insérées par ce test
        try:
            with psycopg2.connect(**db_params) as c, c.cursor() as cur:
                truncate_q = sql.SQL("TRUNCATE TABLE {}").format(
                    sql.Identifier(table_raw)
                )
                cur.execute(truncate_q)
                c.commit()
        except Exception:
            pass
        try:
            os.unlink(tmp_csv.name)
        except Exception:
            pass
