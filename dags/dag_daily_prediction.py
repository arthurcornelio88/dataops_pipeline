from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import pandas as pd
from pandas_gbq import to_gbq
from google.cloud import bigquery

# ========= ENV VARS ========= #
ENV = os.getenv("ENV", "DEV")
BQ_PROJECT = os.getenv("BQ_PROJECT") or "your_project"
BQ_DATASET = os.getenv("BQ_DATASET") or "raw_api_data"
BQ_PREDICT_DATASET = os.getenv("BQ_PREDICT_DATASET") or "predictions"
PREDICT_ENDPOINT = os.getenv("PREDICT_URL_DEV") if ENV == "DEV" else os.getenv("PREDICT_URL_PROD")
BQ_LOCATION = os.getenv("BQ_LOCATION") or "EU"

# ========= PREDICTION TASK ========= #
def run_daily_prediction():
    today = datetime.utcnow().strftime("%Y%m%d")
    raw_table = f"{BQ_PROJECT}.{BQ_DATASET}.daily_{today}"
    pred_table = f"{BQ_PROJECT}.{BQ_PREDICT_DATASET}.daily_{today}"

    bq = bigquery.Client()
    df = bq.query(f"SELECT * FROM `{raw_table}`").to_dataframe()

    os.makedirs("/tmp/predict", exist_ok=True)
    input_path = f"/tmp/predict/input_{today}.csv"
    output_path = f"/tmp/predict/predictions_{today}.csv"
    df.to_csv(input_path, index=False)

    res = requests.post(PREDICT_ENDPOINT, json={
        "input_path": input_path,
        "output_path": output_path
    })

    if res.status_code != 200:
        raise Exception(f"❌ Predict failed: {res.status_code} - {res.text}")

    df_pred = pd.read_csv(output_path)
    df_pred["prediction_ts"] = datetime.utcnow().isoformat()

    to_gbq(df_pred, destination_table=pred_table, project_id=BQ_PROJECT, if_exists="replace", location=BQ_LOCATION)
    print(f"✅ Predictions saved to {pred_table}")

# ========= DAG DEFINITION ========= #
def_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 7, 1),
}

with DAG(
    dag_id="daily_prediction",
    default_args=def_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["fraud", "prediction", "bigquery"]
) as dag:

    predict = PythonOperator(
        task_id="predict_on_daily_data",
        python_callable=run_daily_prediction
    )

    predict
