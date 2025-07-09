import os
import pandas as pd
import numpy as np
from datetime import datetime
from google.cloud import bigquery
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset

# === CONFIGURATION ===
BQ_PROJECT = os.getenv("BQ_PROJECT", "your_project")
BQ_DATASET = os.getenv("BQ_DATASET", "raw_api_data")
TABLE_NAME = f"daily_{datetime.utcnow().strftime('%Y%m%d')}"
BQ_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.{TABLE_NAME}"

REF_PATH = "../shared_data/fraudTest.csv"
REPORT_PATH = "shared_data/evidently_drift_report.html"
FORCE_TYPES = {"cc_num": "int64", "zip": "int64", "unix_time": "int64", "is_fraud": "int64"}

# === FONCTIONS ===

def load_and_prepare():
    print("📥 Chargement des données...")
    bq_client = bigquery.Client()
    df_bq = bq_client.query(f"SELECT * FROM `{BQ_TABLE}`").to_dataframe()
    df_ref = pd.read_csv(REF_PATH).drop(columns=["Unnamed: 0"], errors="ignore")
    return df_ref, df_bq


def align_columns(df1, df2):
    cols1, cols2 = set(df1.columns), set(df2.columns)
    common = sorted(cols1 & cols2)

    print("✅ Colonnes communes:")
    print(common)
    print("\n❌ Présentes uniquement dans la référence:")
    print(sorted(cols1 - cols2))
    print("\n❌ Présentes uniquement dans BigQuery:")
    print(sorted(cols2 - cols1))

    return df1[common].copy(), df2[common].copy(), common


def drop_empty_columns(df1, df2, cols):
    to_drop = [col for col in cols if df1[col].dropna().empty or df2[col].dropna().empty]
    if to_drop:
        print(f"⚠️ Suppression des colonnes vides: {to_drop}")
        df1 = df1.drop(columns=to_drop)
        df2 = df2.drop(columns=to_drop)
    return df1, df2


def force_column_types(df1, df2, overrides):
    failed = []
    for col, typ in overrides.items():
        if col in df1.columns and col in df2.columns:
            try:
                df1[col] = df1[col].astype(typ)
                df2[col] = df2[col].astype(typ)
            except Exception as e:
                print(f"⚠️ Échec de conversion '{col}' → {typ}: {e}")
                failed.append(col)
    return df1.drop(columns=failed, errors="ignore"), df2.drop(columns=failed, errors="ignore"), failed


def check_type_alignment(df1, df2):
    print("🔍 Vérification fine des types Evidently")
    cols_ok, cols_drop = [], []
    for col in df1.columns:
        t1 = df1[col].dropna().map(type).mode()
        t2 = df2[col].dropna().map(type).mode()

        if t1.empty or t2.empty:
            print(f"⚠️ Colonne '{col}' vide → ignorée")
            cols_drop.append(col)
        elif t1[0] != t2[0]:
            print(f"⚠️ Type mismatch '{col}': REF={t1[0].__name__}, CURR={t2[0].__name__}")
            cols_drop.append(col)
        else:
            print(f"✅ {col}: {t1[0].__name__}")
            cols_ok.append(col)
    return df1[cols_ok], df2[cols_ok]


def sanitize_types(df):
    df_clean = df.copy()
    for col in df_clean.columns:
        if pd.api.types.is_string_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].astype(str)
        elif pd.api.types.is_integer_dtype(df_clean[col]):
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("int64", errors="ignore")
        elif pd.api.types.is_float_dtype(df_clean[col]):
            df_clean[col] = df_clean[col].astype("float64")
    return df_clean


# === PIPELINE ===

df_ref, df_bq = load_and_prepare()
df_ref, df_bq, common_cols = align_columns(df_ref, df_bq)
df_ref, df_bq = drop_empty_columns(df_ref, df_bq, common_cols)

# Échantillonnage de la référence (5000 max ou 5x la taille de l’actuel)
df_ref = df_ref.sample(n=len(df_bq), random_state=42)

# Forcer typage manuel (ex: pour Evidently)
df_ref, df_bq, failed_cast = force_column_types(df_ref, df_bq, FORCE_TYPES)
if failed_cast:
    print(f"🧹 Colonnes exclues (type): {failed_cast}")

# Vérification typage croisé
df_ref, df_bq = check_type_alignment(df_ref, df_bq)

# Dernier nettoyage
df_ref = sanitize_types(df_ref)
df_bq = sanitize_types(df_bq)

print("\n🧪 Colonnes finales REF:", list(df_ref.columns))
print("🧪 Colonnes finales CURR:", list(df_bq.columns))

import hashlib

def hash_df(df):
    return pd.util.hash_pandas_object(df.astype(str), index=False).sum()

print("REF hash:", hash_df(df_ref))
print("CUR hash:", hash_df(df_bq))

# === RAPPORT EVIDENTLY ===
print("📊 Génération du rapport Evidently...")
report = Report(metrics=[DataDriftPreset(drift_share=0.2)])
report.run(reference_data=df_ref, current_data=df_bq)

os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
report.save_html(REPORT_PATH)
print(f"✅ Rapport Evidently sauvegardé : {REPORT_PATH}")
