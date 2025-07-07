# 🔧 BigQuery Configuration (Local Setup)

This guide explains how to configure local access to Google BigQuery from your Airflow DAGs and Python scripts using a service account key.

---

## ✅ 1. Prerequisites

- A GCP project (e.g. `jedha2024`)
- Access to [Google Cloud Console](https://console.cloud.google.com)
- `bigquery`, `pandas-gbq`, and `google-cloud-bigquery` Python packages installed

---

## 🔐 2. Create a Service Account + JSON Key

1. Go to **IAM & Admin → Service Accounts**
2. Click **"Create Service Account"**
   - Name: `fraud-airflow-bq`
3. Once created, go to the account → **"Keys" tab**
4. Click **"Add Key → Create new key"**, select **JSON**
5. Download the `.json` file (e.g. `fraud_airflow_bq.json`)
6. Move it to the **root directory of your local project**

> ⚠️ Never commit this file to Git. Add it to your `.gitignore`.

---

## 🛂 3. Assign Required IAM Roles

Go to **IAM** and find your service account (`fraud-airflow-bq@...`).  
Assign it the following **roles**:

| Role Name               | Purpose                             |
|------------------------|-------------------------------------|
| `BigQuery Data Editor` | Read/write table access             |
| `BigQuery Job User`    | Submit jobs (queries, uploads, etc) |
| `BigQuery Metadata Viewer` (optional) | View table schemas/info |

---

## 🌍 4. Configure Your `.env.airflow` (or `.env`)

Add the following environment variables:

```bash
# BigQuery credentials
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/fraud_airflow_bq.json"

# BigQuery config
export BQ_PROJECT="jedha2024"
export BQ_DATASET="raw_api_data"
export BQ_PREDICT_DATASET="predictions"
export BQ_LOCATION="EU"
