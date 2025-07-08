# test dag
allumer les deux api mock-api and model-api
> docker compose up mock-api model-api
gcloud auth config
source env.airtable
source .env

airflow tasks test daily_fetch_transactions fetch_transactions_to_bigquery 2025-07-08
airflow tasks test daily_prediction predict_on_daily_data 2025-07-08
airflow tasks test daily_monitoring monitor_drift_report 2025-07-08