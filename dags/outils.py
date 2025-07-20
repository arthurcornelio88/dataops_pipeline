import os
from google.cloud import secretmanager

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