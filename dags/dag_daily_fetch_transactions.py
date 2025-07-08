from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
from google.cloud import bigquery

# ========= ENV MANAGEMENT ========= #
ENV = os.getenv("ENV", "DEV")
ENDPOINTS = {
    "DEV": os.getenv("DEV_TRANSACTION_URL", "http://localhost:8001/transactions"),
    "PROD": os.getenv("PROD_TRANSACTION_URL")  # ex: https://mock-api-service/api/transactions
}
BQ_PROJECT = os.getenv("BQ_PROJECT") or "your_project"
BQ_DATASET = os.getenv("BQ_DATASET") or "raw_api_data"
BQ_LOCATION = os.getenv("BQ_LOCATION") or "EU"

# Optional variability level (default = "high")
FETCH_VARIABILITY = os.getenv("FETCH_VARIABILITY", "medium")  # "low", "medium", "high"

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
