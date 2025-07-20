from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from outils import get_storage_path, host_to_docker_path, get_secret
import requests
import pandas as pd
from google.cloud import bigquery

# ========= ENV VARS ========= #
ENV = os.getenv("ENV") # Define it in .env.airflow
PROJECT = os.getenv("PROJECT")  # Define it in .env.airflow

if ENV == "PROD":
    # Fetch endpoint from Google Secret Manager
    BQ_PROJECT = PROJECT
    API_URL = get_secret("prod-api-url", PROJECT)
    BQ_RAW_DATASET = get_secret("bq-raw-dataset", PROJECT)
    BQ_PREDICT_DATASET = get_secret("bq-predict-dataset", PROJECT)
    BQ_LOCATION = get_secret("bq-location", PROJECT)
else:
    API_URL = os.getenv("API_URL_DEV")
    BQ_PROJECT = os.getenv("BQ_PROJECT")
    BQ_RAW_DATASET = os.getenv("BQ_RAW_DATASET")
    BQ_PREDICT_DATASET = os.getenv("BQ_PREDICT_DATASET")
    BQ_LOCATION = os.getenv("BQ_LOCATION")

# ========= PREDICTION TASK ========= #
def run_daily_prediction():
    today = datetime.utcnow().strftime("%Y%m%d")
    timestamp = None
    preprocessed_dir = get_storage_path("preprocessed", None)
    predictions_dir = get_storage_path("predictions", None)
    # Only create local dirs in DEV
    if ENV == "DEV":
        os.makedirs(preprocessed_dir, exist_ok=True)
        os.makedirs(predictions_dir, exist_ok=True)

    raw_table = f"{BQ_PROJECT}.{BQ_RAW_DATASET}.daily_{today}"
    pred_table = f"{BQ_PROJECT}.{BQ_PREDICT_DATASET}.daily_{today}"

    # 1️⃣ Lire depuis BigQuery
    print(f"📥 Reading from BigQuery table: {raw_table}")
    bq = bigquery.Client()
    df = bq.query(f"SELECT * FROM `{raw_table}`").to_dataframe()

    # 2️⃣ Appel à l'API pour preprocessing
    print(f"📡 Sending data to /preprocess_direct")
    # Set output_dir for preprocessing (env-aware)
    output_dir = get_storage_path("preprocessed", None)
    if output_dir.endswith("/"):
        output_dir = output_dir[:-1]
    res = requests.post(f"{API_URL}/preprocess_direct", json={
        "data": df.to_dict(orient="records"),
        "log_amt": True,
        "for_prediction": True,
        "output_dir": output_dir
    })
    print(f"🔍 /preprocess_direct response: status={res.status_code}, text={res.text}")
    if res.status_code != 200:
        raise Exception(f"❌ Preprocessing failed: {res.status_code} - {res.text}")
    try:
        timestamp = res.json()["timestamp"]
    except Exception as e:
        raise Exception(f"❌ Could not extract timestamp from /preprocess_direct response: {res.text}")
    # Path to preprocessed features
    X_local_path = get_storage_path("preprocessed", f"X_pred_{timestamp}.csv")
    X_api_path = get_storage_path("preprocessed", f"X_pred_{timestamp}.csv")

    if ENV == "DEV" and not os.path.exists(X_local_path):
        raise FileNotFoundError(f"⛔ Fichier préprocessé introuvable: {X_local_path}")

    # 3️⃣ Nettoyage des colonnes objets
    df_x = pd.read_csv(X_local_path)
    drop_cols = [col for col in df_x.columns if df_x[col].dtype == "object"]
    cleaned_pred_path = get_storage_path("predictions", f"X_pred_{timestamp}.csv")
    if drop_cols:
        print(f"🧹 Removing non-numeric columns: {drop_cols}")
        df_x = df_x.drop(columns=drop_cols)
    df_x.to_csv(cleaned_pred_path, index=False)
    X_api_path = cleaned_pred_path

    # 4️⃣ Prédiction via API
    output_local_path = get_storage_path("predictions", f"predictions_{today}.csv")
    output_api_path = get_storage_path("predictions", f"predictions_{today}.csv")
    if ENV == "DEV":
        os.makedirs(predictions_dir, exist_ok=True)

    # Conversion host->docker uniquement pour l'appel API en DEV
    if ENV == "DEV":
        X_api_path_api = host_to_docker_path(X_api_path)
        output_api_path_api = host_to_docker_path(output_api_path)
    else:
        X_api_path_api = X_api_path
        output_api_path_api = output_api_path

    pred_api_url = f"{API_URL}/predict"
    print(f"🤖 Sending data to /predict")
    res_pred = requests.post(pred_api_url, json={
        "input_path": X_api_path_api,           # 🟢 Chemin vers X_pred...
        "output_path": output_api_path_api      # 🟢 Chemin vers predictions...
    })
    print(f"🔍 /predict response: status={res_pred.status_code}, text={res_pred.text}")
    if res_pred.status_code != 200:
        raise Exception(f"❌ Predict failed: {res_pred.status_code} - {res_pred.text}")

    # 5️⃣ Upload prédictions dans BigQuery
    print(f"📤 Uploading predictions to: {pred_table}")
    if ENV == "DEV":
        df_pred = pd.read_csv(output_local_path)
    else:
        # In PROD, ensure predictions file exists in GCS before uploading to BQ
        from google.cloud import storage
        bucket_name = os.getenv("GCS_BUCKET") or os.getenv("GCP_BUCKET", "fraud-detection-jedha2024")
        storage_client = storage.Client()
        blob_path = output_local_path.replace(f"gs://{bucket_name}/", "")
        temp_path = f"/tmp/predictions_{today}.csv"
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        if not blob.exists():
            raise FileNotFoundError(f"⛔ Prediction file not found in GCS: {output_local_path}. API->GCS step failed.")
        blob.download_to_filename(temp_path)
        df_pred = pd.read_csv(temp_path)
    df_pred["prediction_ts"] = datetime.utcnow().isoformat()

    df_pred.to_gbq(destination_table=pred_table, project_id=BQ_PROJECT, if_exists="append", location=BQ_LOCATION)
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
