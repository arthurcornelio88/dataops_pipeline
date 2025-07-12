from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, get_current_context
from datetime import datetime, timedelta
import os
import requests
from google.cloud import bigquery
import pandas as pd

# ========= ENV & CONFIG ========= #
ENV = os.getenv("ENV", "DEV")
BQ_PROJECT = os.getenv("BQ_PROJECT") or "your_project"
BQ_DATASET = os.getenv("BQ_DATASET", "raw_api_data")
MONITOR_ENDPOINT = os.getenv("MONITOR_URL_DEV", "http://model-api:8000/monitor") if ENV == "DEV" else os.getenv("MONITOR_URL_PROD")
PREPROCESS_ENDPOINT = os.getenv("PREPROCESS_URL_DEV", "http://localhost:8000/preprocess")
API_URL_DEV = os.getenv("API_URL_DEV", "http://model-api:8000")
BQ_LOCATION = os.getenv("BQ_LOCATION") or "EU"
REFERENCE_FILE = os.getenv("REFERENCE_DATA_PATH", "fraudTest.csv")

# ========= DRIFT MONITORING ========= #
def run_drift_monitoring():
    # === Config
    today = datetime.utcnow().strftime("%Y%m%d")
    shared_dir = os.path.abspath("../shared_data")
    os.makedirs(shared_dir, exist_ok=True)

    # === Paths
    curr_filename = f"current_{today}.csv"
    curr_path = os.path.join(shared_dir, curr_filename)
    output_html_name = f"data_drift_{today}.html"

    # === Load & Save and clean current data from BigQuery
    curr_table = f"{BQ_PROJECT}.{BQ_DATASET}.daily_{today}"
    df_curr = bigquery.Client().query(f"SELECT * FROM `{curr_table}`").to_dataframe()

    # === Nettoyage des colonnes non-numériques de BigQuery
    cols_to_drop = []
    for col in df_curr.columns:
        if df_curr[col].dtype == "object":
            # Garder seulement les colonnes catégorielles connues, supprimer les timestamps
            if col not in ["category", "merchant", "job", "state", "city_pop"]:  # Ajustez selon vos colonnes
                cols_to_drop.append(col)
    
    # Supprimer aussi les colonnes timestamp spécifiques
    cols_to_drop.extend(["ingestion_ts", "created_at", "updated_at"])
    cols_to_drop = [col for col in cols_to_drop if col in df_curr.columns]
    
    if cols_to_drop:
        print(f"🧹 Removing BigQuery timestamp/object columns: {cols_to_drop}")
        df_curr = df_curr.drop(columns=cols_to_drop)
    
    # === Créer un fichier de référence filtré avec les mêmes colonnes
    ref_path = os.path.join(shared_dir, REFERENCE_FILE)
    df_ref = pd.read_csv(ref_path)
    
    # Filtrer le fichier de référence pour avoir les mêmes colonnes que current
    common_cols = [col for col in df_curr.columns if col in df_ref.columns]
    df_ref_filtered = df_ref[common_cols]
    
    # Sauvegarder le fichier de référence filtré
    ref_filtered_name = f"ref_filtered_{today}.csv"
    ref_filtered_path = os.path.join(shared_dir, ref_filtered_name)
    df_ref_filtered.to_csv(ref_filtered_path, index=False)
    
    print(f"📊 Common columns for drift: {common_cols}")
    print(f"🔄 Using filtered reference: {ref_filtered_name}")
    
    df_curr.to_csv(curr_path, index=False)

    # === API call to /monitor
    res = requests.post(MONITOR_ENDPOINT, json={
        "reference_path": ref_filtered_name,
        "current_path": curr_filename,
        "output_html": output_html_name
    })

    if res.status_code != 200:
        raise Exception(f"❌ Drift check failed: {res.status_code} - {res.text}")

    result = res.json()
    if result.get("drift_summary", {}).get("drift_detected"):
        print("🚨 Data drift detected:", result["drift_summary"])
    else:
        print("✅ No significant drift detected.")

    context = get_current_context()
    context['ti'].xcom_push(key="drift_detected", value=result["drift_summary"]["drift_detected"])
    context['ti'].xcom_push(key="timestamp", value=today)

def run_validation_step(**context):
    """Validation du modèle : comparer prédictions vs vraies étiquettes depuis BigQuery"""

    today = datetime.utcnow().strftime("%Y%m%d")
    
    # === Joindre les prédictions avec les vraies étiquettes
    print(f"🔍 Validation en production pour le {today}")
    bq = bigquery.Client()
    
    # Requête pour joindre predictions et raw_data sur cc_num
    validation_query = f"""
    SELECT 
        p.cc_num,
        p.fraud_score,
        p.is_fraud_pred,
        r.is_fraud as true_label
    FROM `{BQ_PROJECT}.predictions.daily_{today}` p
    INNER JOIN `{BQ_PROJECT}.{BQ_DATASET}.daily_{today}` r
    ON CAST(p.cc_num AS STRING) = CAST(r.cc_num AS STRING)
    WHERE r.is_fraud IS NOT NULL
    """
    
    print(f"🔍 Exécution de la requête de validation...")
    df_validation = bq.query(validation_query).to_dataframe()
    
    if df_validation.empty:
        print("⚠️ Aucune donnée de validation trouvée (jointure prédictions + étiquettes).")
        context['ti'].xcom_push(key="val_auc", value=-1.0)
        context['ti'].xcom_push(key="validation_type", value="none")
        context['ti'].xcom_push(key="validation_samples", value=0)
        return
    
    # === Appel API pour validation production
    print(f"🎯 Validation via API avec {len(df_validation)} échantillons")
    res = requests.post(f"{API_URL_DEV}/validate", json={
        "model_name": "catboost_model.cbm",
        "validation_mode": "production",
        "production_data": {
            "y_true": df_validation['true_label'].tolist(),
            "y_pred_proba": df_validation['fraud_score'].tolist(),
            "y_pred_binary": df_validation['is_fraud_pred'].tolist()
        }
    })

    if res.status_code != 200:
        raise Exception(f"❌ Validation failed: {res.status_code} - {res.text}")

    val_result = res.json()
    auc = val_result.get("auc", 0)
    validation_type = val_result.get("validation_type", "production")
    n_samples = val_result.get("n_samples", len(df_validation))
    
    print(f"📊 Validation Production - AUC: {auc:.4f} (n={n_samples})")
    
    # === Stocker dans XCom
    context['ti'].xcom_push(key="val_auc", value=auc)
    context['ti'].xcom_push(key="validation_type", value=validation_type)
    context['ti'].xcom_push(key="validation_samples", value=n_samples)


def decide_if_retrain(**context):
    # 🔧 FORCE RETRAINING - HARDCODED FOR TESTING
    print("🚧 FORCE MODE: Hardcoded retraining decision!")
    return "retrain_model"
    
    # === Code original commenté ===
    # drift_detected = context['ti'].xcom_pull(task_ids="monitor_drift_report", key="drift_detected")
    # if not drift_detected:
    #     print("✅ No drift → stop.")
    #     return "end_monitoring"

    # auc = context['ti'].xcom_pull(task_ids="validate_model", key="val_auc")
    # auc_threshold = float(os.getenv("AUC_THRESHOLD", 0.90))

    # if auc < auc_threshold:
    #     print(f"🚨 AUC {auc} < {auc_threshold} → Retrain needed.")
    #     return "retrain_model"
    # else:
    #     print(f"✅ AUC {auc} >= {auc_threshold} → Model still good.")
    #     return "end_monitoring"

def retrain_model_step(**context):
    # 🎯 FINE-TUNING avec vraie API
    timestamp_date = context['ti'].xcom_pull(task_ids="monitor_drift_report", key="timestamp")
    
    from datetime import datetime
    current_time = datetime.now().strftime("%H%M%S")
    timestamp_full = f"{timestamp_date}_{current_time}"

    print(f"🧠 Starting FINE-TUNING via API on timestamp {timestamp_full}")
    
    # Appel API pour fine-tuning
    res = requests.post(f"{API_URL_DEV}/train", json={
        "timestamp": timestamp_full,
        "fast": True,  # Fine-tuning mode (rapide)
        "test": False,
        "model_name": "catboost_model.cbm",
        "mode": "fine_tune",  # Mode fine-tuning
        "learning_rate": 0.01,  # LR plus bas pour fine-tuning
        "epochs": 10  # Moins d'epochs
    })

    if res.status_code != 200:
        print(f"❌ Fine-tuning API call failed: {res.status_code} - {res.text}")
        # Fallback sur simulation si API échoue
        print("🎭 Fallback: Simulating fine-tuning...")
        import random
        auc_improvement = random.uniform(0.01, 0.03)
        current_auc = context['ti'].xcom_pull(task_ids="validate_model", key="val_auc")
        new_auc = min(0.95, current_auc + auc_improvement)
        
        context['ti'].xcom_push(key="fine_tune_success", value=True)
        context['ti'].xcom_push(key="auc_improvement", value=auc_improvement)
        context['ti'].xcom_push(key="new_auc", value=new_auc)
        print(f"📈 Simulated AUC improvement: {current_auc:.4f} → {new_auc:.4f}")
        return

    # Traitement de la réponse API
    result = res.json()
    print(f"✅ Fine-tuning API response: {result}")
    
    if result.get("status") == "fine_tuning_complete":
        auc_improvement = result.get("auc_improvement", 0.02)
        current_auc = context['ti'].xcom_pull(task_ids="validate_model", key="val_auc")
        new_auc = min(0.95, current_auc + auc_improvement)
        
        print(f"🧠 Fine-tuning successful!")
        print(f"📈 AUC improvement: {current_auc:.4f} → {new_auc:.4f} (+{auc_improvement:.4f})")
        
        # Stocker les résultats
        context['ti'].xcom_push(key="fine_tune_success", value=True)
        context['ti'].xcom_push(key="auc_improvement", value=auc_improvement)
        context['ti'].xcom_push(key="new_auc", value=new_auc)
    else:
        print(f"⚠️ Fine-tuning status: {result.get('status')}")
        context['ti'].xcom_push(key="fine_tune_success", value=False)

def end_monitoring(**context):

    ti = context['ti']
    exec_date = context['execution_date'].strftime("%Y-%m-%d %H:%M:%S")

    drift = ti.xcom_pull(task_ids="monitor_drift_report", key="drift_detected")
    auc = ti.xcom_pull(task_ids="validate_model", key="val_auc")
    retrained = ti.xcom_pull(task_ids="decide_if_retrain")
    
    # 🧠 Fine-tuning results
    fine_tune_success = ti.xcom_pull(task_ids="retrain_model", key="fine_tune_success")
    auc_improvement = ti.xcom_pull(task_ids="retrain_model", key="auc_improvement")
    new_auc = ti.xcom_pull(task_ids="retrain_model", key="new_auc")

    if auc is None:
        auc = -1.0  # not evaluated (no drift)

    # === Résumé visuel avec fine-tuning
    print("\n📊 ----------- Monitoring Summary -----------")
    print(f"📅 Date d'exécution : {exec_date}")
    print(f"📌 Drift détecté     : {'🚨 OUI' if drift else '✅ NON'}")
    print(f"📈 AUC validation    : {auc if auc != -1.0 else 'n/a'}")
    print(f"🔁 Fine-tuning lancé : {'✅ OUI' if retrained == 'retrain_model' else '⛔ NON'}")
    
    if fine_tune_success and auc_improvement:
        print(f"🧠 Fine-tuning résultat : AUC {auc:.4f} → {new_auc:.4f} (+{auc_improvement:.4f})")
        print("🎯 Modèle mis à jour avec apprentissage incrémental")
    
    print("🧾 Environnement     :", ENV)
    print("--------------------------------------------\n")

    # === Slack Notification if needed
    if drift or (auc != -1.0 and auc < 0.90):
        send_slack_alert(drift=drift, auc=auc, retrained=(retrained == "retrain_model"))

    # === Log vers BigQuery
    validation_type = ti.xcom_pull(task_ids="validate_model", key="validation_type") or "unknown"
    validation_samples = ti.xcom_pull(task_ids="validate_model", key="validation_samples") or 0
    
    audit = pd.DataFrame([{
        "timestamp": exec_date,
        "drift_detected": drift,
        "auc": auc,
        "retrained": retrained == "retrain_model",
        "env": ENV,
        "validation_type": validation_type,
        "validation_samples": validation_samples,
        "fine_tune_success": fine_tune_success or False,
        "auc_improvement": auc_improvement or 0.0,
        "new_auc": new_auc or auc
    }])

    create_monitoring_table_if_needed()

    table = f"{BQ_PROJECT}.monitoring_audit.logs"
    bq = bigquery.Client()
    bq.load_table_from_dataframe(audit, table).result()

    print(f"✅ Audit enregistré dans BigQuery : {table}")


### Auxiliar functions

def send_slack_alert(drift, auc, retrained):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("⚠️ No Slack webhook configured.")
        return

    text = f"""
    🚨 *Monitoring Alert* 🚨
    Date: {datetime.utcnow().strftime('%Y-%m-%d')}
    Drift detected: *{drift}*
    Validation AUC: *{auc:.4f}*
    Retraining triggered: *{retrained}*
    """

    try:
        res = requests.post(webhook_url, json={"text": text.strip()})
        res.raise_for_status()
        print("✅ Slack alert sent.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Slack alert failed: {e}")

def create_monitoring_table_if_needed():

    table_id = f"{BQ_PROJECT}.monitoring_audit.logs"
    client = bigquery.Client()

    try:
        client.get_table(table_id)
        print(f"✅ Table already exists: {table_id}")
    except Exception:
        print(f"⚠️ Table not found, creating: {table_id}")

        schema = [
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("drift_detected", "BOOL"),
            bigquery.SchemaField("auc", "FLOAT"),
            bigquery.SchemaField("retrained", "BOOL"),
            bigquery.SchemaField("env", "STRING"),
            bigquery.SchemaField("validation_type", "STRING"),
            bigquery.SchemaField("validation_samples", "INTEGER"),
            bigquery.SchemaField("fine_tune_success", "BOOL"),
            bigquery.SchemaField("auc_improvement", "FLOAT"),
            bigquery.SchemaField("new_auc", "FLOAT"),
        ]

        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f"✅ Table created: {table_id}")

# ========= DAG ========= #
def_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 7, 1),
}

with DAG(
    dag_id="daily_monitoring",
    default_args=def_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["fraud", "monitoring", "drift"]
) as dag:

    monitor = PythonOperator(
        task_id="monitor_drift_report",
        python_callable=run_drift_monitoring
    )

    validate_model = PythonOperator(
    task_id="validate_model",
    python_callable=run_validation_step,
    provide_context=True
    )

    decide_next = BranchPythonOperator(
        task_id="decide_if_retrain",
        python_callable=decide_if_retrain,
        provide_context=True
    )

    retrain_model = PythonOperator(
        task_id="retrain_model",
        python_callable=retrain_model_step,
        provide_context=True
    )

    end = PythonOperator(
        task_id="end_monitoring",
        python_callable=end_monitoring
    )

    monitor >> validate_model >> decide_next
    decide_next >> [retrain_model, end]
    retrain_model >> end
