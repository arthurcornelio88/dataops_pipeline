## 🚀 Local Airflow Setup — One Script to Rule Them All

> Reliable, repeatable setup for dev — pure CLI, zero Docker.

---

### ✅ 1. Setup in One Command

```bash
chmod +x setup_airflow.sh && ./setup_airflow.sh
```

This script will:

✅ Create project folders
✅ Generate `.env.airflow`
✅ Clean previous Airflow metadata
✅ Reset & migrate the DB
✅ Create `admin` user
✅ Sync roles/permissions
✅ Add `.airflowignore`
✅ Print next steps

---

### ✅ 2. Project Structure

```
dataops_pipeline/
├── dags/
│   └── .airflowignore      # auto-generated
├── logs/
├── airflow_home/
├── .env.airflow            # auto-generated
└── setup_airflow.sh
```

`.env.airflow`:

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__LOGGING__BASE_LOG_FOLDER=$(pwd)/logs
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
```

---

### ✅ 3. Run Airflow

```bash
pkill -f "airflow webserver"
pkill -f "airflow scheduler"
pkill -f gunicorn
rm airflow-webserver.pid

export ENV="PROD" 
export GOOGLE_CLOUD_PROJECT="jedha2024"
export REFERENCE_PATH="fraudTest.csv"

source .env.airflow
airflow scheduler &
airflow webserver --port 8080
```

→ Open [http://localhost:8080](http://localhost:8080)
→ Login: `admin` / `admin`

---

### ✅ 4. Useful CLI Commands

List DAGs:

```bash
airflow dags list
```

Trigger a DAG:

```bash
airflow dags trigger your_dag_id
```

Test a task:

```bash
airflow tasks test your_dag_id your_task_id 2024-01-01
```

View DAG runs:

```bash
airflow dags list-runs -d your_dag_id
```

---

### 🧹 Reset Everything

If anything goes wrong or you want a fresh start:

```bash
rm -rf airflow_home/ logs/ dags/__pycache__/
./setup_airflow.sh
```

---
