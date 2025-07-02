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
BQ_PREDICT_DATASET = os.getenv("BQ_PREDICT_DATASET") or "predictions"
MONITOR_ENDPOINT = os.getenv("MONITOR_URL_DEV", "http://model-api:8000/monitor") if ENV == "DEV" else os.getenv("MONITOR_URL_PROD")
BQ_LOCATION = os.getenv("BQ_LOCATION") or "EU"

# ========= DRIFT MONITORING ========= #
def run_drift_monitoring():
    today = datetime.utcnow().strftime("%Y%m%d")
    pred_table = f"{BQ_PROJECT}.{BQ_PREDICT_DATASET}.daily_{today}"

    # Load reference and current data
    bq = bigquery.Client()
    ref_path = "/tmp/predict/reference.csv"
    curr_path = f"/tmp/predict/current_{today}.csv"

    os.makedirs("/tmp/predict", exist_ok=True)
    df_ref = pd.read_csv("data/reference.csv")  # Static or versioned reference file
    df_ref.to_csv(ref_path, index=False)

    df_curr = bq.query(f"SELECT * FROM `{pred_table}`").to_dataframe()
    df_curr.to_csv(curr_path, index=False)

    output_html = f"reports/data_drift_{today}.html"

    res = requests.post(MONITOR_ENDPOINT, json={
        "reference_path": ref_path,
        "current_path": curr_path,
        "output_html": output_html
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
