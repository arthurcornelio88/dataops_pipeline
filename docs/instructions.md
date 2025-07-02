# Block 3 – Automatic Fraud Detection 🥷 with Airflow

## ⏱ Duration
**360 minutes**

---

## 🔔 Important Note

> 💡 This project is handled **by a single person (myself)** to validate Block 3. There is no team division.

---

## 🎯 Project Goals

- [x] Expose a complete ML inference API (prediction, preprocessing, training, validation)
- [x] Simulate real-time data flow with a mock transaction API
- [ ] Generate a daily report of all payments and frauds from the previous day
- [ ] Monitor prediction drift and model degradation over time

---

## 📇 Context

Fraud is a huge issue for financial institutions. In 2019, the European Central Bank estimated that over **€1 billion** was lost to card fraud across the EU. 😮

AI has proven effective in detecting fraud with high accuracy. But the real challenge now lies in **productionizing these models**, so they can predict fraudulent activity in **real-time**, adapt to drift, and trigger proper actions.

---

## 📦 Data Sources

- [x] **Fraudulent Payments Dataset**
  - A large static dataset of labeled (fraud/not fraud) transactions
  - ✅ Used for training and evaluating the model

- [x] **Mock Real-time Transaction API**
  - Simulates streaming transaction data via REST
  - ✅ Implemented using FastAPI + Faker

---

## 📬 Deliverables

- [x] 📌 A clean **microservice-based architecture** exposing model endpoints
- [x] 📦 All FastAPI source code for `model_training_api` and `mock_realtime_api`
- [ ] 🎥 A **demo video** showing predictions and monitoring in action (e.g. with Vidyard)
- [ ] ✅ A DAG-based prediction workflow in Airflow (separate repo `dataops`)

---

## 🧱 Repositories

### `model_training/`
> The core ML project with source code, inference and training API, preprocessing, and model registry

- ✔️ `train.py`: ML training pipeline integrated with MLflow
- ✔️ `predict.py`: Batch inference pipeline
- ✔️ `validate_model.py`: Evaluate model on test set
- ✔️ `preprocessing.py`: Feature engineering and encoding
- ✔️ `main.py`: Unified FastAPI entrypoint exposing `/predict`, `/train`, `/validate`, `/preprocess`, `/monitor`, etc.

Subfolders:
```bash
📂 model_training/
├── model_training_api/         # API logic
├── mock_realtime_api/          # Fake transaction generator (FastAPI)
├── src/                        # Core ML scripts
├── Dockerfile + docker-compose.yml
```

---

### `dataops/` *(separate repo)*
> Will host the orchestration and automation layer (Airflow)

- [ ] DAGs that interact with `model-api` and `mock-api`
- [ ] Task for periodic prediction and drift evaluation
- [ ] Logging predictions and metrics to storage

---

## 🔌 Architecture Overview

- `mock-api` generates fake transactions
- `model-api` exposes full ML logic via REST
- `dataops` (Airflow) orchestrates ingestion and retraining

Each module is containerized and connected via `docker-compose` (dev), and designed for modular deployment (prod).

---

## ✅ Project Checklist

### Phase 1 – ML Module

* [x] Prepare and explore dataset
* [x] Feature engineering + preprocessing script
* [x] `train.py` – model training with MLflow logging
* [x] `predict.py` – batch prediction
* [x] `validate_model.py` – evaluate model performance
* [x] `main.py` – FastAPI server with modular endpoints

### Phase 2 – API & Simulation

* [x] Expose `/preprocess`, `/train`, `/predict`, `/validate`, `/monitor`
* [x] Dockerize APIs and run via `docker-compose`
* [x] Testing API endpoints
  *  [x] `preprocess` ok in dev
  *  [x] `train` ok in dev
  *  [x] `predict` ok in dev
  *  [x] `validate` ok in dev
  *  [x] `monitor` ok in dev
* [x] Build `mock_realtime_api` with realistic transaction format
* [x] Dockerize APIs and run via `docker-compose`

### Phase 3 – Production & Automation

* [x] Build Airflow DAGs in `dataops`
* [ ] Periodic ingestion from mock API
* [ ] Store new data + run predictions via API
* [ ] Trigger drift detection via `/monitor`
* [ ] Retrain model if drift or performance drop detected
* [ ] Report & notify on frauds

---

## 💡 Tips & Constraints

### 🔧 ML Frameworks
- `catboost`, `scikit-learn`, `mlflow`, `pandas`, `evidently`

### 🔌 Deployment Style
- REST-first, fully modular API
- All business logic is callable from other services

---

## 👤 Solo Project Note

This full-stack MLops pipeline is implemented **entirely solo**, covering all aspects:
- ML dev
- API design
- Data simulation
- Containerization
- Infrastructure orchestration

🧑‍🚀 Let’s ship it!
