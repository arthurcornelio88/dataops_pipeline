# ⚙️ `dataops_pipeline` — Airflow Orchestration for Fraud Detection (GCP Ready)

This repository orchestrates the fraud detection pipeline using **Apache Airflow**, deployed on a **GCP VM**. It connects with the `model-api`, automates data ingestion from the mock API, prediction jobs, and fine-tuning based on fraud drift.

---

## 🚀 Key Features

- Apache Airflow 2.8+ with GCP integration
- Real-time prediction + retraining logic
- Orchestration for `/preprocess`, `/predict`, `/train`, `/monitor`
- GCS + BigQuery + Secret Manager
- Discord alerting on fraud spikes

---

## 🧰 Quickstart

### 1. Clone the Repository

```bash
git clone https://gitlab.com/automatic_fraud_detection_b3/dataops_pipeline.git
cd dataops_pipeline
````

### 2. Provision the VM (One Time)

Use [`docs/bigquery_setup.md`](docs/bigquery_setup.md) to:

* Create service account + IAM roles

Then, run:

```bash
export ENV=PROD
export GOOGLE_CLOUD_PROJECT=jedha2024
export REFERENCE_PATH=fraudTest.csv

chmod +x setup_vm_airflow.sh
./setup_vm_airflow.sh
```

---

## 🌪️ Launch Airflow

```bash
source .venv/bin/activate
source .env.airflow

airflow webserver --port 8080 &
airflow scheduler &
```

Web UI → `http://<YOUR_VM_IP>:8080`

> Get external IP with `curl ifconfig.me`

---

## 📂 DAG Overview

Once Airflow is live, activate:

| DAG ID             | Description                                 |
| ------------------ | ------------------------------------------- |
| `fetch_api_data`   | Pulls synthetic transactions every minute   |
| `predict_payments` | Batch predicts fraud on fresh data          |
| `monitor_fraud`    | Monitors prediction volume + retrains model |
| `retrain_model`    | Fine-tunes CatBoost when drift is detected  |

🧠 Full explanations in [`docs/dags.md`](docs/dags.md)

---

## 📚 Documentation

| Topic                     | Path                                               |
| ------------------------- | -------------------------------------------------- |
| 🌐 Airflow Setup          | [`docs/airflow_setup.md`](docs/airflow_setup.md)   |
| 🗂️ BigQuery Tables       | [`docs/bigquery_setup.md`](docs/bigquery_setup.md) |
| 📊 Drift Testing (manual) | [`docs/drift_testing.md`](docs/drift_testing.md)   |
| 💬 Discord Alerts         | [`docs/discord.md`](docs/discord.md)               |
| 🧪 Manual DAG Testing     | [`docs/instructions.md`](docs/instructions.md)     |
| 🔁 DAG Logic Explained    | [`docs/dags.md`](docs/dags.md)                     |

---

## ⚠️ Prerequisites

* GCS Bucket for models + preprocessed data
* BigQuery Datasets:

  * `raw_api_data` (ingested)
  * `predictions` (fraud scores)
* Secret Manager:

  * `DISCORD_WEBHOOK`
  * GCP Service account JSON (`GOOGLE_APPLICATION_CREDENTIALS`)
* `.env.airflow` properly configured

---

## 🧠 Related Projects

| Component       | Description                        |
| --------------- | ---------------------------------- |
| `model-api`     | FastAPI backend for ML logic       |
| `mock-api`      | Fake transaction stream generator  |
| `mlflow_server` | Optional experiment tracking stack |

---

## 🧼 Logs and Data

* Logs: `logs/`
* Shared preprocessed data: `shared_data/`

---

## 🛟 Troubleshooting

* MLflow not reachable? Check port or tracking URI
* DAG not running? Validate your `.env.airflow` + secrets
* Model missing? Ensure `catboost_model_latest.cbm` exists in GCS

---

Built with ❤️ by Arthur Cornélio — Jedha 2024
