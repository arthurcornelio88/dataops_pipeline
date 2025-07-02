uv init 

uv venv

uv pip install "apache-airflow[google]==2.8.4" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.11.txt"
uv pip install "apache-airflow==3.0.2" apache-airflow-providers-google==10.1.1

uv sync

source .venv/bin/activate
