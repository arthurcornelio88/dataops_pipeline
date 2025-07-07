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
API_URL_DEV = os.getenv("API_URL_DEV") or "http://localhost:8000"
BQ_LOCATION = os.getenv("BQ_LOCATION") or "EU"

# ========= PREDICTION TASK ========= #
def run_daily_prediction():
    today = datetime.utcnow().strftime("%Y%m%d")
    timestamp = None
    shared_data_dir = os.path.abspath("../shared_data")
    os.makedirs(shared_data_dir, exist_ok=True)

    raw_table = f"{BQ_PROJECT}.{BQ_DATASET}.daily_{today}"
    pred_table = f"{BQ_PROJECT}.{BQ_PREDICT_DATASET}.daily_{today}"

    # 1️⃣ Lire depuis BigQuery
    print(f"📥 Reading from BigQuery table: {raw_table}")
    bq = bigquery.Client()
    df = bq.query(f"SELECT * FROM `{raw_table}`").to_dataframe()

    # 2️⃣ Appel à l'API pour preprocessing
    print(f"📡 Sending data to /preprocess_direct")
    res = requests.post(f"{API_URL_DEV}/preprocess_direct", json={
        "data": df.to_dict(orient="records"),
        "log_amt": True,
        "for_prediction": True,
        "output_dir": "/app/shared_data"  # Important pour Docker
    })
    if res.status_code != 200:
        raise Exception(f"❌ Preprocessing failed: {res.status_code} - {res.text}")

    timestamp = res.json()["timestamp"]
    X_local_path = os.path.join(shared_data_dir, f"X_pred_{timestamp}.csv")
    X_api_path = f"/app/shared_data/X_pred_{timestamp}.csv"

    if not os.path.exists(X_local_path):
        raise FileNotFoundError(f"⛔ Fichier préprocessé introuvable: {X_local_path}")

    # 3️⃣ Nettoyage des colonnes objets
    df_x = pd.read_csv(X_local_path)
    drop_cols = [col for col in df_x.columns if df_x[col].dtype == "object"]
    if drop_cols:
        print(f"🧹 Removing non-numeric columns: {drop_cols}")
        df_x.drop(columns=drop_cols).to_csv(X_local_path, index=False)

    # 4️⃣ Prédiction via API
    output_local_path = os.path.join(shared_data_dir, f"predictions_{today}.csv")
    output_api_path = output_local_path.replace(shared_data_dir, "/app/shared_data")
    os.makedirs(os.path.dirname(output_local_path), exist_ok=True)

    print(f"🤖 Sending data to /predict")
    res = requests.post(PREDICT_ENDPOINT, json={
        "input_path": X_api_path,           # 🟢 Chemin vers X_pred...
        "output_path": output_api_path      # 🟢 Chemin vers predictions...
    })
    if res.status_code != 200:
        raise Exception(f"❌ Predict failed: {res.status_code} - {res.text}")

    # 5️⃣ Upload prédictions dans BigQuery
    print(f"📤 Uploading predictions to: {pred_table}")
    df_pred = pd.read_csv(output_local_path)
    df_pred["prediction_ts"] = datetime.utcnow().isoformat()

    to_gbq(df_pred, destination_table=pred_table, project_id=BQ_PROJECT, if_exists="append", location=BQ_LOCATION)
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
