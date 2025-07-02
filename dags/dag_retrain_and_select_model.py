from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import json

# ========= ENV & CONFIG ========= #
ENV = os.getenv("ENV", "DEV")
TRAIN_ENDPOINT = os.getenv("TRAIN_URL_DEV", "http://model-api:8000/train") if ENV == "DEV" else os.getenv("TRAIN_URL_PROD")
VALIDATE_ENDPOINT = os.getenv("VALIDATE_URL_DEV", "http://model-api:8000/validate") if ENV == "DEV" else os.getenv("VALIDATE_URL_PROD")
SUMMARY_PATH = "models/model_summary.json"
MODEL_NAME = "catboost_model.cbm"

# ========= TRAINING & SELECTION ========= #
def retrain_and_select():
    # Step 1: Retrain model
    res_train = requests.post(TRAIN_ENDPOINT, json={
        "test": False,
        "fast": False,
        "model_name": MODEL_NAME
    })
    if res_train.status_code != 200:
        raise Exception(f"❌ Training failed: {res_train.status_code} - {res_train.text}")
    print("✅ Model retrained.")

    # Step 2: Validate model
    res_val = requests.post(VALIDATE_ENDPOINT, json={
        "model_name": MODEL_NAME
    })
    if res_val.status_code != 200:
        raise Exception(f"❌ Validation failed: {res_val.status_code} - {res_val.text}")
    result = res_val.json()
    auc = result.get("auc")
    print(f"📊 AUC after retrain: {auc}")

    # Step 3: Optional model selection logic (mocked)
    # Could call another service or evaluate local scores from previous models
    if os.path.exists(SUMMARY_PATH):
        with open(SUMMARY_PATH, "r") as f:
            summary = json.load(f)
        summary.append({"model": MODEL_NAME, "timestamp": datetime.utcnow().isoformat(), "auc": auc})
    else:
        summary = [{"model": MODEL_NAME, "timestamp": datetime.utcnow().isoformat(), "auc": auc}]

    with open(SUMMARY_PATH, "w") as f:
        json.dump(summary, f, indent=2)

    print("🏆 Updated model summary:")
    print(json.dumps(summary, indent=2))

# ========= DAG ========= #
def_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 7, 1),
}

with DAG(
    dag_id="retrain_and_select_model",
    default_args=def_args,
    schedule_interval=None,
    catchup=False,
    tags=["fraud", "training", "retrain"]
) as dag:

    retrain = PythonOperator(
        task_id="retrain_model",
        python_callable=retrain_and_select
    )

    retrain
