import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# === Config
BQ_PROJECT = os.getenv("BQ_PROJECT", "your_project")
BQ_DATASET = os.getenv("BQ_DATASET", "raw_api_data")
TABLE_NAME = f"daily_{datetime.utcnow().strftime('%Y%m%d')}"
BQ_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.{TABLE_NAME}"
REF_PATH = "../shared_data/fraudTest.csv"

# === Charger les données
bq_client = bigquery.Client()
df_bq = bq_client.query(f"SELECT * FROM `{BQ_TABLE}`").to_dataframe()
df_ref = pd.read_csv(REF_PATH).drop(columns=["Unnamed: 0"], errors="ignore")

# === Comparaison
cols_bq = set(df_bq.columns)
cols_ref = set(df_ref.columns)

print("✅ Colonnes communes:")
print(sorted(cols_bq & cols_ref))
print("\n❌ Présentes uniquement dans la référence:")
print(sorted(cols_ref - cols_bq))
print("\n❌ Présentes uniquement dans BigQuery:")
print(sorted(cols_bq - cols_ref))
