# ⚙️ `dataops_pipeline` — Airflow Setup on GCP VM

This repository contains the production orchestration for fraud detection using Apache Airflow. The setup is designed to be run manually on a clean **Debian-based VM on Google Cloud Platform**.

---

## ✅ Step 1 – Create the Service Account

```bash
gcloud iam service-accounts create fraud-b3 \
  --description="Airflow fraud detection service account" \
  --display-name="fraud-b3"
```

---

## ✅ Step 2 – Assign the Required IAM Roles

```bash
PROJECT_ID=$(gcloud config get-value project)

# BigQuery
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:fraud-b3@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:fraud-b3@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# GCS
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:fraud-b3@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.admin"

# Secret Manager
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:fraud-b3@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

---

## ✅ Step 3 – Create a VM with the Service Account

```bash
gcloud compute instances create airflow-prod-b3 \
  --zone=europe-west1-d \
  --machine-type=e2-medium \
  --service-account=fraud-b3@${PROJECT_ID}.iam.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/cloud-platform \
  --image-family=debian-11 \
  --image-project=debian-cloud \
  --boot-disk-size=20GB \
  --boot-disk-type=pd-balanced \
  --boot-disk-device-name=airflow-prod-b3
```

---

## 📌 Result

- **Service account**: `fraud-b3@<project>.iam.gserviceaccount.com`
- **Access:**
  - ✅ BigQuery read/write
  - ✅ GCS full access
  - ✅ Secret Manager access
- VM is ready to receive your `setup_vm_airflow.sh`

---

## ✅ 1. System Setup (One-Time, on a Fresh VM)

After creating the VM, run:

```bash
sudo apt-get update -y
sudo apt-get install -y git curl python3-pip
```

---

## 📦 2. Clone the Project

```bash
git clone https://gitlab.com/automatic_fraud_detection_b3/dataops_pipeline.git
cd dataops_pipeline
```

---

## 🚀 3. Run the Airflow Environment Setup

This script installs:
- `uv` (dependency manager)
- Apache Airflow `2.8.4`
- Google provider `10.1.1`
- Sets up the local environment

```bash
export ENV="PROD"
export REFERENCE_DATA_PATH="fraudTest.csv"
export PROJECT="jedha2024"

chmod +x setup_vm_airflow.sh
./setup_vm_airflow.sh
```

---

## 🌀 4. Launch Airflow

```bash
source .venv/bin/activate
source .env.airflow

# Start the webserver and scheduler
airflow webserver --port 8080 &
airflow scheduler &
```

Airflow UI: `http://<YOUR_VM_PUBLIC_IP>:8080`

> To kill and restart Airflow: `pkill airflow && pkill gunicorn`, then relaunch.

---

## 🌐 5. Get Your IP & Open Port 8080 in GCP

- **From the VM:**

  ```bash
  curl ifconfig.me
  ```

- **In the GCP Console:**
  Go to **VPC > Firewall rules**, and create a rule:

  | Field            | Value                                 |
  | ---------------- | ------------------------------------- |
  | Name             | `allow-airflow-8080`                  |
  | Targets          | All instances in the network          |
  | Protocols/Ports  | TCP: `8080`                           |
  | Source IP Ranges | `0.0.0.0/0` *(or restrict as needed)* |

---

## 📂 6. Enable DAGs in the Airflow UI

Once Airflow is running, enable the following DAGs:

- `fetch_api_data` — fetches real-time payments every minute
- `predict_payments` — runs fraud prediction batch jobs
- `monitor_fraud` — monitors predictions and triggers alerts

For a detailed explanation of each DAG and its role in the workflow, see [docs/dags](docs/dags.md).

---

## ⚠️ Requirements

Ensure the following components are configured:

- ✅ GCP **storage bucket**
- ✅ GCP **BigQuery datasets** (`raw_api_data`, `predictions`)
- ✅ GCP **service account key** (`GOOGLE_APPLICATION_CREDENTIALS`)
- ✅ **Discord webhook** for alert notifications

---