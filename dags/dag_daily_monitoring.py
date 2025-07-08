from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
from google.cloud import bigquery
import pandas as pd

# ========= ENV & CONFIG ========= #
ENV = os.getenv("ENV", "DEV")
BQ_PROJECT = os.getenv("BQ_PROJECT") or "your_project"
BQ_DATASET = os.getenv("BQ_DATASET", "raw_api_data")
MONITOR_ENDPOINT = os.getenv("MONITOR_URL_DEV", "http://model-api:8000/monitor") if ENV == "DEV" else os.getenv("MONITOR_URL_PROD")
PREPROCESS_ENDPOINT = os.getenv("PREPROCESS_URL_DEV", "http://localhost:8000/preprocess")
BQ_LOCATION = os.getenv("BQ_LOCATION") or "EU"
REFERENCE_FILE = os.getenv("REFERENCE_DATA_PATH", "fraudTest.csv")

# ========= DRIFT MONITORING ========= #
def run_drift_monitoring():
    # === Config
    today = datetime.utcnow().strftime("%Y%m%d")
    shared_dir = os.path.abspath("../shared_data")
    os.makedirs(shared_dir, exist_ok=True)

    # === Paths
    curr_filename = f"current_{today}.csv"
    curr_path = os.path.join(shared_dir, curr_filename)
    output_html_name = f"data_drift_{today}.html"

    # === Load & Save and clean current data from BigQuery
    curr_table = f"{BQ_PROJECT}.{BQ_DATASET}.daily_{today}"
    df_curr = bigquery.Client().query(f"SELECT * FROM `{curr_table}`").to_dataframe()

    df_curr = df_curr.drop(columns=["ingestion_ts"], errors="ignore")
    df_curr.to_csv(curr_path, index=False)

    # === API call to /monitor
    res = requests.post(MONITOR_ENDPOINT, json={
        "reference_path": REFERENCE_FILE,
        "current_path": curr_filename,
        "output_html": output_html_name
    })

    if res.status_code != 200:
        raise Exception(f"❌ Drift check failed: {res.status_code} - {res.text}")

    result = res.json()
    if result.get("drift_summary", {}).get("drift_detected"):
        print("🚨 Data drift detected:", result["drift_summary"])
    else:
        print("✅ No significant drift detected.")


# ========= DAG ========= #
def_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 7, 1),
}

with DAG(
    dag_id="daily_monitoring",
    default_args=def_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["fraud", "monitoring", "drift"]
) as dag:

    monitor = PythonOperator(
        task_id="monitor_drift_report",
        python_callable=run_drift_monitoring
    )

    monitor
