# fetch_raw_sample.py

import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# === Load ENV ===
ENV = os.getenv("ENV")
PROJECT = os.getenv("BQ_PROJECT")
BQ_RAW_DATASET = os.getenv("BQ_RAW_DATASET")
BQ_LOCATION = os.getenv("BQ_LOCATION")

# === Output config ===
TODAY = datetime.utcnow().strftime("%Y%m%d")
OUTPUT_FILE = f"raw_sample_{TODAY}.csv"
OUTPUT_PATH = f"gs://fraud-detection-jedha2024/shared_data/tmp/{OUTPUT_FILE}" if ENV == "PROD" else f"/app/shared_data/tmp/{OUTPUT_FILE}"

# === Query last 500 records ===
def fetch_latest_raw_sample(n_rows=500):
    table = f"{PROJECT}.{BQ_RAW_DATASET}.daily_{TODAY}"
    query = f"""
        SELECT * 
        FROM `{table}` 
        ORDER BY ingestion_ts DESC 
        LIMIT {n_rows}
    """

    print(f"🔍 Running query:\n{query}")
    client = bigquery.Client()
    df = client.query(query).to_dataframe()

    print(f"✅ Retrieved {len(df)} rows from BigQuery")

    # Save locally or to GCS
    if ENV == "PROD":
        from google.cloud import storage
        import io

        csv_bytes = df.to_csv(index=False).encode("utf-8")
        bucket_name = os.getenv("GCS_BUCKET", "fraud-detection-jedha2024")
        bucket = storage.Client().bucket(bucket_name)
        blob = bucket.blob(f"shared_data/tmp/{OUTPUT_FILE}")
        blob.upload_from_string(csv_bytes, content_type="text/csv")
        print(f"📁 Saved to GCS: {OUTPUT_PATH}")
    else:
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        df.to_csv(OUTPUT_PATH, index=False)
        print(f"📁 Saved locally to: {OUTPUT_PATH}")

    return OUTPUT_PATH

if __name__ == "__main__":
    fetch_latest_raw_sample()
