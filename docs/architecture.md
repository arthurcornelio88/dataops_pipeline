## 📊 Architecture Pipeline - Fraud Detection

### 🧱 Core Components

| Component   | Description                                                               |
| ----------- | ------------------------------------------------------------------------- |
| `mock-api`  | Generates synthetic transactions via `/transactions` (GET)                |
| `model-api` | Exposes full ML logic: preprocessing, prediction, validation, drift check |
| `dataops`   | Airflow project: orchestrates DAGs                                        |
| `BigQuery`  | Main storage for raw data, predictions, monitoring, and history           |
| `MLflow`    | Tracks model training and validation experiments                          |
| `Evidently` | Generates data drift reports (HTML/JSON) and alerts                       |

---

### 🔄 Data Flow (Airflow Orchestration)

#### **DAG 1 – Transaction Ingestion**

* 🔄 Call `/transactions?n=500` from `mock-api`
* ⬇️ Store into BigQuery → table `raw_api_data.daily_YYYYMMDD`

#### **DAG 2 – Daily Prediction**

* 🔄 Read `raw_api_data.daily_YYYYMMDD`
* ⬇️ Call `/predict` from `model-api`
* ⬇️ Store predictions into `predictions.daily_YYYYMMDD`

#### **DAG 3 – Monitoring & Drift Detection**

* 🔄 Compare `data/predictions.csv` with reference data `data/reference.csv`
* ⬇️ Call `/monitor` on `model-api`
* 📄 Generate Evidently report (.html/.json)
* 🔔 If drift detected → trigger alert (email/Slack)

#### **DAG 4 – Retraining & Model Selection**

* 🔄 Triggered if drift OR poor model performance (AUC/F1 below threshold)
* ⬇️ Call `/train` (fast or full mode)
* 📊 Evaluate all candidate models
* 🏆 Select best model (based on summary/metrics)
* 📅 Save final model to GCS or `models/`
* 🔄 Update prediction endpoint with selected model

---

### 📦 Key BigQuery Tables

* `raw_api_data.daily_YYYYMMDD`
* `predictions.daily_YYYYMMDD`
* `drift_reports` (optional)
* `model_summary` (metadata and evaluation of past training runs)

---

### 📩 Notifications / Alerting

* Data drift detected → ✅ Email or Slack alert
* Poor performance (e.g. AUC < 0.85 or F1 < 0.60) → ⚠️ Trigger retraining
* DAG failures → 🔴 Default Airflow email alerting

---

### ✅ Next Steps

* [ ] Implement DAG 1 – ingestion
* [ ] Implement DAG 2 – prediction
* [ ] Implement DAG 3 – monitoring
* [ ] Connect alerts + email integration
* [ ] Implement DAG 4 – automatic retrain + model selection