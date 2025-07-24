from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
from google.cloud import bigquery
from outils import get_secret


# ========= ENV MANAGEMENT ========= #
ENV = os.getenv("ENV") # Define it in .env.airflow
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") # Define it in .env.airflow

if ENV == "PROD":
    # Fetch endpoint from Google Secret Manager
    BQ_PROJECT = PROJECT
    API_URL = get_secret("prod-mock-url", PROJECT)
    BQ_RAW_DATASET = get_secret("bq-raw-dataset", PROJECT)
    BQ_LOCATION = get_secret("bq-location", PROJECT)
    RESET_BQ = get_secret("reset-bq-before-write", PROJECT)
    FETCH_VARIABILITY = get_secret("fetch-variability", PROJECT)
else:
    API_URL = os.getenv("API_URL_DEV")
    BQ_PROJECT = os.getenv("BQ_PROJECT")
    BQ_RAW_DATASET = os.getenv("BQ_RAW_DATASET")
    BQ_LOCATION = os.getenv("BQ_LOCATION")
    RESET_BQ = os.getenv("RESET_BQ_BEFORE_WRITE", "false").lower() == "true"
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
    url = API_URL
    if not url:
        raise ValueError(f"❌ No endpoint defined for ENV: {ENV}")

    n = 500
    full_url = f"{url}/transactions?n={n}&variability={FETCH_VARIABILITY}"
    print(f"🌐 Fetching from: {full_url}")

    response = requests.get(full_url)
    if response.status_code != 200:
        raise Exception(f"❌ Failed to fetch transactions: {response.status_code} - {response.text}")

    df = pd.DataFrame(response.json())
    df["ingestion_ts"] = datetime.utcnow().isoformat()

     
    table_id = f"{BQ_PROJECT}.{BQ_RAW_DATASET}.daily_{datetime.utcnow().strftime('%Y%m%d')}"

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
