#!/bin/bash
set -e

DATABASES=("airflow" "aqi" "mlflow")

for db in "${DATABASES[@]}"; do
  echo "Checking if database '$db' exists…"
  if psql -U "$POSTGRES_USER" -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1; then
    echo "Database '$db' already exists, skipping."
  else
    echo "Creating database: $db"
    psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" <<-EOSQL
      CREATE DATABASE "$db";
EOSQL
  fi
done
