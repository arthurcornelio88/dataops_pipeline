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
