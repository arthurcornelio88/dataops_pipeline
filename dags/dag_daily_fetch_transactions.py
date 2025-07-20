from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
from google.cloud import bigquery
from outils import get_secret


# ========= ENV MANAGEMENT ========= #
BQ_PROJECT = get_secret("BQ_PROJECT") or "your_project"
ENV = os.getenv("ENV", "DEV")

if ENV == "PROD":
    # Fetch endpoint from Google Secret Manager
    PROD_ENDPOINT = get_secret("prod-api-url", BQ_PROJECT)
    ENDPOINTS = {
        "DEV": os.getenv("DEV_TRANSACTION_URL", "http://localhost:8001/transactions"),
        "PROD": PROD_ENDPOINT
    }
else:
    ENDPOINTS = {
        "DEV": os.getenv("DEV_TRANSACTION_URL", "http://localhost:8001/transactions"),
        "PROD": os.getenv("PROD_TRANSACTION_URL")
    }
BQ_PROJECT = os.getenv("BQ_PROJECT") or "your_project"
BQ_DATASET = os.getenv("BQ_DATASET") or "raw_api_data"
BQ_LOCATION = os.getenv("BQ_LOCATION") or "EU"
RESET_BQ = os.getenv("RESET_BQ_BEFORE_WRITE", "false").lower() == "true"

# Optional variability level (default = "high")
FETCH_VARIABILITY = os.getenv("FETCH_VARIABILITY", 0)  # 0 to 1, float value

# ========= BIGQUERY TABLE MANAGEMENT ========= #
def delete_table_if_exists(table_id):
    client = bigquery.Client()
    try:
        client.delete_table(table_id, not_found_ok=True)
        print(f"🗑️ Table BQ supprimée si existante : {table_id}")
    except Exception as e:
        print(f"❌ Erreur lors de la suppression de la table : {e}")

# ========= FETCH + STORE LOGIC ========= #
def fetch_transactions_to_bq():
    url = ENDPOINTS.get(ENV)
    if not url:
        raise ValueError(f"❌ No endpoint defined for ENV: {ENV}")

    n = 500
    full_url = f"{url}?n={n}&variability={FETCH_VARIABILITY}"
    print(f"🌐 Fetching from: {full_url}")

    response = requests.get(full_url)
    if response.status_code != 200:
        raise Exception(f"❌ Failed to fetch transactions: {response.status_code} - {response.text}")

    df = pd.DataFrame(response.json())
    df["ingestion_ts"] = datetime.utcnow().isoformat()

     
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.daily_{datetime.utcnow().strftime('%Y%m%d')}"

    # if RESET_BQ, reset table of the day BEFORE pushing
    if RESET_BQ:
        delete_table_if_exists(table_id)
    else:
        print(f"ℹ️ RESET_BQ_BEFORE_WRITE désactivé → pas de suppression de {table_id}")

    # Write to BigQuery
    df.to_gbq(destination_table=table_id, project_id=BQ_PROJECT, if_exists="replace", location=BQ_LOCATION)
    print(f"✅ {len(df)} transactions ingested into {table_id} [variability={FETCH_VARIABILITY}]")


# ========= DAG DEFINITION ========= #
def_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 7, 1),
}

with DAG(
    dag_id="daily_fetch_transactions",
    default_args=def_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["fraud", "ingestion", "bigquery"]
) as dag:

    fetch_to_bq = PythonOperator(
        task_id="fetch_transactions_to_bigquery",
        python_callable=fetch_transactions_to_bq
    )

    fetch_to_bq
