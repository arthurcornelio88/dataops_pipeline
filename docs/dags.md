# test dag
allumer les deux api mock-api and model-api
> docker compose up mock-api model-api
gcloud auth login
source env.airtable
source .env

fetch
- airflow tasks test daily_fetch_transactions fetch_transactions_to_bigquery 2025-07-08
predict
- airflow tasks test daily_prediction predict_on_daily_data 2025-07-08
monitor_and_train
- airflow tasks test daily_monitoring monitor_drift_report 2025-07-12
- airflow tasks test daily_monitoring validate_model 2025-07-12
- airflow tasks test daily_monitoring decide_if_retrain 2025-07-12