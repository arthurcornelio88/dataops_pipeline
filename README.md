uv init 

uv venv

uv pip install "apache-airflow[google]==2.8.4" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.11.txt"

uv pip install "apache-airflow==3.0.2" apache-airflow-providers-google==10.1.1

uv sync

source .venv/bin/activate

---

airflow setup

```bash
sa
chmod +x setup_airflow.sh && ./setup_airflow.sh
```

===

# In production

- evidently cloud : https://app.jedha.co/course/continuous-monitoring-lds/real-time-monitoring-lds

🚨 ONE THING THAT YOU NEED TO MAKE SURE IS TO ADD THE ENVIRONMENT VARIABLE IN THE AIRFLOW VARIABLES (Admins > Variables) 🚨 You have EVIDENTLY_CLOUD_TOKEN & EVIDENTLY_CLOUD_PROJECT_ID.


===

Create VM (e2-medium)
sudo apt-get update
sudo apt-get install -y git
sudo apt-get install -y python3-pip
git clone https://gitlab.com/automatic_fraud_detection_b3/dataops_pipeline.git

===

curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.local/bin/env

cd dataops_pipeline

uv init 

uv venv

AIRFLOW_VERSION=3.0.3

# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 3.0.0 with python 3.9: https://raw.githubusercontent.com/apache/airflow/constraints-3.0.3/constraints-3.9.txt

uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

uv pip install --upgrade apache-airflow apache-airflow-providers-google

uv sync

source .venv/bin/activate

==

===

# 1. Mise à jour et outils de base (une seule fois)
sudo apt-get update -y
sudo apt-get install -y git curl python3-pip

# 2. Cloner le projet
git clone https://gitlab.com/automatic_fraud_detection_b3/dataops_pipeline.git
cd dataops_pipeline

# 3. Lancer le script
chmod +x setup_vm_airflow.sh
./setup_vm_airflow.sh
