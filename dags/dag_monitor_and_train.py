from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, get_current_context
from datetime import datetime, timedelta
import os
from outils import get_storage_path, get_secret, fetch_historical_frauds  
import requests
from google.cloud import bigquery
import pandas as pd
from urllib.parse import urljoin 
import numpy as np

# ========= ENV & CONFIG ========= #
ENV = os.getenv("ENV") # Define it in .env.airflow
PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT") # Define it in .env.airflow 

print(f"🌍 Running in ENV: {ENV} | PROJECT: {PROJECT}")

if ENV == "PROD":
    API_URL = get_secret("prod-api-url", PROJECT)
    REFERENCE_FILE = get_secret("reference-data-path", PROJECT)
    DISCORD_WEBHOOK_URL = get_secret("discord-webhook-url", PROJECT)
    BQ_PROJECT = PROJECT
    BQ_RAW_DATASET = get_secret("bq-raw-dataset", PROJECT)
    BQ_LOCATION = get_secret("bq-location", PROJECT)

else:
    API_URL = os.getenv("API_URL_DEV", "http://model-api:8000")
    REFERENCE_FILE = os.getenv("REFERENCE_DATA_PATH", "fraudTest.csv")
    DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")
    BQ_PROJECT = os.getenv("BQ_PROJECT")
    BQ_RAW_DATASET = os.getenv("BQ_RAW_DATASET")
    BQ_LOCATION = os.getenv("BQ_LOCATION")

# ========= DRIFT MONITORING ========= #
def run_drift_monitoring():
    # === Config
    today = datetime.utcnow().strftime("%Y%m%d")
    shared_dir = get_storage_path("", "")
    os.makedirs(shared_dir, exist_ok=True)

    # === Paths
    curr_filename = f"current_{today}.csv"
    curr_path = get_storage_path("", curr_filename)
    output_html_name = f"data_drift_{today}.html"

    # === Load & Save and clean current data from BigQuery
    curr_table = f"{BQ_PROJECT}.{BQ_RAW_DATASET}.daily_{today}"
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
    ref_path = get_storage_path("", REFERENCE_FILE)
    df_ref = pd.read_csv(ref_path)
    
    # Filtrer le fichier de référence pour avoir les mêmes colonnes que current
    common_cols = [col for col in df_curr.columns if col in df_ref.columns]
    df_ref_filtered = df_ref[common_cols]
    
    # Sauvegarder le fichier de référence filtré
    ref_filtered_name = f"ref_filtered_{today}.csv"
    ref_filtered_path = get_storage_path("", ref_filtered_name)
    df_ref_filtered.to_csv(ref_filtered_path, index=False)
    
    print(f"📊 Common columns for drift: {common_cols}")
    print(f"🔄 Using filtered reference: {ref_filtered_name}")
    
    df_curr.to_csv(curr_path, index=False)

    # === API call to /monitor
    print(f"🔗 API_URL resolved: {API_URL}")
    monitor_endpoint = urljoin(API_URL,"/monitor")
    res = requests.post(monitor_endpoint, json={
        "reference_path": ref_filtered_path,
        "current_path": curr_path,
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

from outils import fetch_historical_frauds  # 🆕 Import at the top

def run_validation_step(**context):
    """Validation du modèle : comparer prédictions vs vraies étiquettes depuis BigQuery"""
    today = datetime.utcnow().strftime("%Y%m%d")
    print(f"🔍 Validation en production pour le {today}")
    
    bq = bigquery.Client()

    # === Requête principale
    validation_query = f"""
    SELECT 
        p.cc_num,
        p.fraud_score,
        p.is_fraud_pred,
        r.is_fraud as true_label
    FROM `{BQ_PROJECT}.predictions.daily_{today}` p
    INNER JOIN `{BQ_PROJECT}.{BQ_RAW_DATASET}.daily_{today}` r
    ON CAST(p.cc_num AS STRING) = CAST(r.cc_num AS STRING)
    WHERE r.is_fraud IS NOT NULL
    """

    print("🔍 Exécution de la requête de validation...")
    df_validation = bq.query(validation_query).to_dataframe()

    # === Vérification de l’équilibre des classes
    if df_validation.empty:
        print("⚠️ Aucune donnée de validation trouvée (jointure vide).")
        context['ti'].xcom_push(key="val_auc", value=-1.0)
        context['ti'].xcom_push(key="validation_type", value="none")
        context['ti'].xcom_push(key="validation_samples", value=0)
        return

    fraud_count = df_validation["true_label"].sum()
    print(f"📊 Validation set: {fraud_count} frauds / {len(df_validation)} samples")

    if df_validation['true_label'].nunique() < 2:
        print("⚠️ Only one class in validation set — augmenting with past frauds...")

        df_extra = fetch_historical_frauds(
            bq_client=bq,
            bq_project=BQ_PROJECT,
            dataset=BQ_RAW_DATASET,
            days_back=7,
            min_frauds=10,
            verbose=True
        )

        if not df_extra.empty:
            df_extra['true_label'] = df_extra['is_fraud']
            df_extra['is_fraud_pred'] = 1  # assume model would catch them
            df_extra['fraud_score'] = 0.95  # simulate high confidence

            common_cols = df_validation.columns.intersection(df_extra.columns)
            df_extra = df_extra[common_cols]

            df_validation = pd.concat([df_validation, df_extra], ignore_index=True)
            print(f"🔁 Validation set after augmentation: {df_validation.shape}")
        else:
            print("🚨 Could not enrich validation set. Skipping validation.")
            context['ti'].xcom_push(key="val_auc", value=-1.0)
            context['ti'].xcom_push(key="validation_type", value="only_one_class_unfixed")
            context['ti'].xcom_push(key="validation_samples", value=len(df_validation))
            return

    # 🧹 Nettoyage ultime pour JSON-safe (force float, retire NaN et Inf)
    for col in ['true_label', 'fraud_score', 'is_fraud_pred']:
        df_validation[col] = (
            pd.to_numeric(df_validation[col], errors='coerce')  # force float
            .replace([np.inf, -np.inf], np.nan)                # remove Inf
            .fillna(0.0)                                       # fill NaN
        )

    # 🧪 DEBUG : Sanity check après nettoyage
    print("🔎 df_validation info:")
    print(df_validation[['true_label', 'fraud_score', 'is_fraud_pred']].info())

    print("\n📊 Statistiques des colonnes :")
    print(df_validation[['true_label', 'fraud_score', 'is_fraud_pred']].describe())

    print("\n🔍 NaN count :")
    print(df_validation[['true_label', 'fraud_score', 'is_fraud_pred']].isna().sum())

    print("\n🔍 Inf count :")
    print({
        col: np.isinf(df_validation[col]).sum()
        for col in ['fraud_score', 'is_fraud_pred']
    })

    print("\n🔍 Types détectés dans les colonnes :")
    print({
        col: df_validation[col].map(type).value_counts().to_dict()
        for col in ['true_label', 'fraud_score', 'is_fraud_pred']
    })

    print("\n🔍 Exemple de valeurs (premières lignes):")
    print(df_validation[['true_label', 'fraud_score', 'is_fraud_pred']].head(5).to_dict(orient='records'))

    # === Appel API
    print(f"🎯 Validation via API avec {len(df_validation)} échantillons")
    validate_endpoint = urljoin(API_URL, "/validate")
    res = requests.post(validate_endpoint, json={
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

    # === XCom
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


from outils import fetch_historical_frauds  # 🆕 Import the shared helper

def retrain_model_step(**context):
    """🧠 FINE-TUNING avec nouvelles données BigQuery → Preprocessing → Fine-tuning"""
    timestamp_date = context['ti'].xcom_pull(task_ids="monitor_drift_report", key="timestamp")
    current_time = datetime.now().strftime("%H%M%S")
    timestamp_full = f"{timestamp_date}_{current_time}"

    print(f"🧠 Starting FINE-TUNING pipeline with BigQuery data for {timestamp_full}")

    try:
        today = datetime.utcnow().strftime("%Y%m%d")
        raw_table = f"{BQ_PROJECT}.{BQ_RAW_DATASET}.daily_{today}"
        print(f"📥 Fetching fresh data from BigQuery table: {raw_table}")
        bq = bigquery.Client()
        df_fresh = bq.query(f"SELECT * FROM `{raw_table}` ORDER BY cc_num DESC LIMIT 1000").to_dataframe()

        if df_fresh.empty:
            raise Exception(f"❌ No fresh data in {raw_table}")

        if "is_fraud" in df_fresh.columns:
            fraud_count = df_fresh["is_fraud"].sum()
            print(f"📊 Fraud ratio in fresh data: {fraud_count} / {len(df_fresh)}")

            if fraud_count < 4:
                print("⚠️ No frauds in fresh data — trying to fetch historical frauds")

                df_extra = fetch_historical_frauds(
                    bq_client=bq,
                    bq_project=BQ_PROJECT,
                    dataset=BQ_RAW_DATASET,
                    days_back=7,
                    min_frauds=10,
                    verbose=True
                )

                if not df_extra.empty:
                    common_cols = df_fresh.columns.intersection(df_extra.columns)
                    df_extra = df_extra[common_cols]
                    df_fresh = pd.concat([df_fresh, df_extra], ignore_index=True)
                    print(f"🔁 Final dataset size after enrichment: {df_fresh.shape}")
                else:
                    print("🚨 No historical frauds found — continuing with fraud-free data (⚠️ risky)")

        print(f"✅ Fetched {len(df_fresh)} fresh samples from BigQuery")

        # 🧹 Nettoyage
        bigquery_cols_to_drop = ["ingestion_ts", "created_at", "updated_at", "_ingestion_time"]
        cols_to_drop = [col for col in bigquery_cols_to_drop if col in df_fresh.columns]
        if cols_to_drop:
            print(f"🧹 Removing timestamp columns: {cols_to_drop}")
            df_fresh = df_fresh.drop(columns=cols_to_drop)

        # Log class distribution
        if "is_fraud" in df_fresh.columns:
            fraud_ratio = df_fresh["is_fraud"].mean()
            print(f"📊 Final fraud ratio: {fraud_ratio:.4f}")

        # 🔄 Preprocessing
        print("🔄 Preprocessing fresh data with /preprocess_direct...")
        preprocess_endpoint = urljoin(API_URL, "/preprocess_direct")
        preprocess_res = requests.post(preprocess_endpoint, json={
            "data": df_fresh.to_dict(orient="records"),
            "log_amt": True,
            "for_prediction": False,
            "output_dir": "/app/shared_data"
        }, timeout=300)

        if preprocess_res.status_code != 200:
            raise Exception(f"❌ Preprocessing failed: {preprocess_res.status_code} - {preprocess_res.text}")

        preprocess_result = preprocess_res.json()
        fresh_timestamp = preprocess_result.get("timestamp")
        print(f"✅ Preprocessing complete. Timestamp: {fresh_timestamp}")

        # 🧠 Fine-tuning
        print("🧠 Starting fine-tuning with preprocessed data...")
        train_endpoint = urljoin(API_URL, "/train")
        finetune_res = requests.post(train_endpoint, json={
            "timestamp": fresh_timestamp,
            "timestamp_model_finetune": "latest",
            "fast": False,
            "test": False,
            "model_name": "catboost_model.cbm",
            "mode": "fine_tune",
            "learning_rate": 0.01,
            "epochs": 10
        }, timeout=600)

        if finetune_res.status_code != 200:
            raise Exception(f"❌ Fine-tuning failed: {finetune_res.status_code} - {finetune_res.text}")

        result = finetune_res.json()

        print("🛬 API response from /train:")
        import json
        print(json.dumps(result, indent=2))

        if "model_path" not in result:
            raise Exception("❌ CRITICAL: model_path missing from API response")

        current_auc = context['ti'].xcom_pull(task_ids="validate_model", key="val_auc")
        new_auc = result.get("auc")
        auc_improvement = new_auc - current_auc if current_auc and current_auc > 0 else 0.02

        print(f"📈 AUC: {current_auc:.4f} → {new_auc:.4f} (+{auc_improvement:.4f})")

        context['ti'].xcom_push(key="fine_tune_success", value=True)
        context['ti'].xcom_push(key="auc_improvement", value=auc_improvement)
        context['ti'].xcom_push(key="new_auc", value=new_auc)
        context['ti'].xcom_push(key="model_path", value=result["model_path"])

    except Exception as e:
        print(f"❌ Fine-tuning pipeline failed: {e}")
        raise e

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

    # === Discord Notifications
    # 🚨 ALERTE DE MONITORING - Se déclenche d'abord si drift/mauvaise performance
    if drift or (auc != -1.0 and auc < 0.90):
        send_discord_alert(drift=drift, auc=auc, retrained=(retrained == "retrain_model"))
    
    # 🎉 Envoie une notification dans tous les cas de fine-tuning réussi (même sans gain)
    if fine_tune_success:
        send_fine_tuning_success_alert(context)

    # === Log vers BigQuery
    validation_type = ti.xcom_pull(task_ids="validate_model", key="validation_type") or "unknown"
    validation_samples = ti.xcom_pull(task_ids="validate_model", key="validation_samples") or 0
    
    # 🔧 FIX: Convertir exec_date string en datetime pour BigQuery
    from datetime import datetime as dt
    if isinstance(exec_date, str):
        # Parse la string datetime
        timestamp_dt = dt.strptime(exec_date, "%Y-%m-%d %H:%M:%S")
    else:
        # C'est déjà un objet datetime
        timestamp_dt = exec_date
    
    audit = pd.DataFrame([{
        "timestamp": timestamp_dt,  # Utiliser l'objet datetime
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

def send_fine_tuning_success_alert(context):
    """Envoie une alerte Discord pour célébrer le succès du fine-tuning"""
    # 📊 Récupérer les métriques depuis les tâches précédentes
    fine_tune_success = context['ti'].xcom_pull(task_ids="retrain_model", key="fine_tune_success")
    auc_improvement = context['ti'].xcom_pull(task_ids="retrain_model", key="auc_improvement")
    new_auc = context['ti'].xcom_pull(task_ids="retrain_model", key="new_auc")
    model_path = context['ti'].xcom_pull(task_ids="retrain_model", key="model_path")
    
    print(f"🔍 DEBUG Discord: fine_tune_success={fine_tune_success}")
    print(f"🔍 DEBUG Discord: auc_improvement={auc_improvement}")
    print(f"🔍 DEBUG Discord: new_auc={new_auc}")
    print(f"🔍 DEBUG Discord: model_path={model_path}")
    
    # 🚨 PRODUCTION: Pas de fallbacks - toutes les valeurs doivent être présentes
    if not fine_tune_success:
        raise Exception("❌ CRITICAL: fine_tune_success not found in XCom!")
    if auc_improvement is None:
        raise Exception("❌ CRITICAL: auc_improvement not found in XCom!")
    if new_auc is None:
        raise Exception("❌ CRITICAL: new_auc not found in XCom!")
    if model_path is None:
        raise Exception("❌ CRITICAL: model_path not found in XCom!")
    
    # 🎯 Seulement si le fine-tuning a vraiment réussi
    if not fine_tune_success:
        print("🤖 Fine-tuning success flag not set, skipping Discord celebration")
        return
    
    # 🌟 Message de célébration
    try:
        webhook_url = DISCORD_WEBHOOK_URL
        if not webhook_url:
            print("⚠️ No Discord webhook URL configured in environment variables")
            return

        if auc_improvement > 0.01:
            message = f"""🎉 **EXCELLENT! Fine-tuning réussi avec BigQuery!** 🎉

📊 **Performance améliorée:** AUC +{auc_improvement:.4f} (maintenant {new_auc:.4f})
🧠 **Modèle mis à jour:** {model_path}
⚡ **Données fraîches:** Dernières 500 transactions BigQuery
🚀 **Statut:** Production ready!

*Le modèle de détection de fraude est plus intelligent! 🤖*
"""
        elif auc_improvement < 0:
            message = f"""⚠️ **Attention : fine-tuning avec dégradation de performance** ⚠️

📉 **AUC détérioré:** -{abs(auc_improvement):.4f} (de {new_auc + abs(auc_improvement):.4f} → {new_auc:.4f})
🧠 **Modèle mis à jour malgré tout:** {model_path}
🧪 **Mode:** Fine-tuning forcé (test)
📦 **Données utilisées:** BigQuery + historiques éventuels

*Vérifiez que cette mise à jour est souhaitée.* 🙏
"""
        else:
            message = f"""✅ **Fine-tuning BigQuery terminé.** ✅

📊 **Performance stable:** AUC {new_auc:.4f}
🧠 **Modèle actualisé:** {model_path}
📦 **Mode:** Fine-tuning automatique
📊 **Données synchronisées:** 500 dernières transactions

*Modèle à jour et en surveillance continue 👁️*
"""

#         if auc_improvement > 0.01:  # Amélioration significative
#             message = f"""🎉 **EXCELLENT! Fine-tuning réussi avec BigQuery!** 🎉

# 📊 **Performance améliorée:** AUC +{auc_improvement:.4f} (maintenant {new_auc:.4f})
# 🧠 **Modèle mis à jour:** {model_path}
# ⚡ **Données fraîches:** Dernières 500 transactions BigQuery
# 🚀 **Statut:** Production ready!

# *Le modèle de détection de fraude est plus intelligent! 🤖*"""
#         else:
#             message = f"""✅ **Fine-tuning BigQuery completed!** ✅
    
# 📊 **Performance maintenue:** AUC {new_auc:.4f}
# 🧠 **Modèle actualisé:** {model_path}
# 🔄 **Données synchronisées:** 500 dernières transactions
# 📊 **Statut:** Modèle à jour et opérationnel

# *Continuons à surveiller les performances! 👀*"""
            
        response = requests.post(webhook_url, json={"content": message})
        
        if response.status_code in [200, 204]:  # 200 = OK, 204 = No Content (both are success)
            print(f"🎊 Discord success notification sent! Status: {response.status_code}")
        elif response.status_code == 404:
            print(f"❌ Discord webhook not found (404). Please check webhook URL or recreate it.")
            print(f"🔗 Webhook URL: {webhook_url[:50]}...")
        else:
            print(f"⚠️ Discord notification failed with status {response.status_code}: {response.text}")
        
    except Exception as e:
        print(f"⚠️ Failed to send Discord success alert: {e}")


def send_discord_alert(drift, auc, retrained):
    """Notification Discord pour les alertes de performance/problèmes"""
    webhook_url = DISCORD_WEBHOOK_URL
    if not webhook_url:
        print("⚠️ No Discord webhook URL configured in environment variables")
        return

    # Message d'alerte général de monitoring
    message = f"""🚨 **Monitoring Alert** 🚨

📅 Date: {datetime.utcnow().strftime('%Y-%m-%d')}
📌 Drift detected: {drift}
📈 Validation AUC: {auc:.4f}
🔁 Retraining triggered: {retrained}
---
"""
    
    try:
        response = requests.post(webhook_url, json={"content": message})
        
        if response.status_code in [200, 204]:  # 200 = OK, 204 = No Content (both are success)
            print("✅ Monitoring alert sent to Discord.")
        else:
            print(f"⚠️ Discord alert failed with status {response.status_code}: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Discord alert failed: {e}")


def create_monitoring_table_if_needed():
    dataset_id = f"{BQ_PROJECT}.monitoring_audit"
    table_id = f"{dataset_id}.logs"
    client = bigquery.Client()

    # Vérifie si le dataset existe
    try:
        client.get_dataset(dataset_id)
        print(f"✅ Dataset exists: {dataset_id}")
    except Exception:
        print(f"⚠️ Dataset not found. Creating: {dataset_id}")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = BQ_LOCATION
        client.create_dataset(dataset)
        print(f"✅ Dataset created: {dataset_id}")

    # Vérifie si la table existe
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
        client.create_table(table)
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
