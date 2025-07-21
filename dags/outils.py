import os
from google.cloud import secretmanager
import gcsfs
import pandas as pd
import time

def get_storage_path(subdir: str, filename: str) -> str:
    """
    Returns the environment-aware storage path for a given subdir and filename.
    DEV: Local filesystem under /app/shared_data/
    PROD: Google Cloud Storage bucket path
    """
    ENV = os.getenv("ENV", "DEV")
    PROJECT = os.getenv("PROJECT")
    GCS_BUCKET = get_secret("gcp-bucket", PROJECT) if ENV == "PROD" else os.getenv("GCS_BUCKET")
    if ENV == "PROD":
        # Use GCS path
        if subdir:
            return f"gs://{GCS_BUCKET}/shared_data/{subdir}/{filename}" if filename else f"gs://{GCS_BUCKET}/{subdir}/"
        else:
            return f"gs://{GCS_BUCKET}/shared_data/{filename}" if filename else f"gs://{GCS_BUCKET}/shared_data/"
    else:
        # Use local path (project-level shared_data)
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../shared_data'))
        if subdir:
            return os.path.join(base_dir, subdir, filename) if filename else os.path.join(base_dir, subdir) + '/'
        else:
            return os.path.join(base_dir, filename) if filename else base_dir + '/'

def host_to_docker_path(path):
    """
    Convertit un chemin absolu local (host) en chemin Docker /app/shared_data/...
    """
    return path.replace(
        "/home/arthurcornelio88/jedha/automatic_fraud_detection_b3/shared_data",
        "/app/shared_data"
    )

def get_secret(secret_id, project_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8").strip()

def write_csv(df, path):
    if path.startswith("gs://"):
        print(f"📝 Saving to GCS: {path}")
        fs = gcsfs.GCSFileSystem(skip_instance_cache=True, cache_timeout=0)

        with fs.open(path, 'w') as f:
            df.to_csv(f, index=False)
            f.flush()
        fs.invalidate_cache(path)
        # Validation immédiate
        if not fs.exists(path):
            raise RuntimeError(f"❌ GCS file not found right after saving: {path}")
        print(f"✅ File written and verified on GCS: {path}")
    else:
        print(f"📝 Saving locally: {path}")
        df.to_csv(path, index=False)


def read_gcs_csv(path: str) -> pd.DataFrame:
    """
    Lit un fichier CSV, que ce soit en local ou sur GCS (gs://...).
    
    Args:
        path (str): Le chemin vers le fichier CSV.
    
    Returns:
        pd.DataFrame: Le DataFrame chargé.
    
    Raises:
        FileNotFoundError: Si le fichier est introuvable.
    """
    if path.startswith("gs://"):
        fs = gcsfs.GCSFileSystem(skip_instance_cache=True, cache_timeout=0)

        if not fs.exists(path):
            raise FileNotFoundError(f"⛔ Fichier introuvable sur GCS: {path}")
        with fs.open(path, "r") as f:
            return pd.read_csv(f)
    else:
        if not os.path.exists(path):
            raise FileNotFoundError(f"⛔ Fichier local introuvable: {path}")
        return pd.read_csv(path)

def file_exists(path):
    if path.startswith("gs://"):
        fs = gcsfs.GCSFileSystem(skip_instance_cache=True, cache_timeout=0)

        return fs.exists(path)
    else:
        return os.path.exists(path)

def wait_for_gcs(path, timeout=30):
    """
    Attends que le fichier GCS soit visible (avec un timeout en secondes)
    """
    if not path.startswith("gs://"):
        if not os.path.exists(path):
            raise FileNotFoundError(f"❌ Local file not found: {path}")
        return

    fs = gcsfs.GCSFileSystem(skip_instance_cache=True, cache_timeout=0)
    for i in range(timeout):
        if fs.exists(path):
            print(f"✅ GCS file detected: {path}")
            return
        print(f"⏳ Waiting for GCS propagation ({i+1}/{timeout}): {path}")
        time.sleep(1)

    raise FileNotFoundError(f"⛔ File not found in GCS after {timeout}s: {path}")