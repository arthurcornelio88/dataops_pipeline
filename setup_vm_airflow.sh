#!/bin/bash

set -e

echo "🚀 Initialisation de la VM pour Airflow 2.8.4..."

# 1. Installer uv (si non dispo)
if ! command -v uv &> /dev/null; then
  echo "📥 Installation de uv..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
fi

# 2. Activer uv dans le shell
export PATH="$HOME/.local/bin:$PATH"

# 3. Initialiser l’environnement
echo "📁 Initialisation de l'environnement virtuel uv..."
uv init || true  # ne crash pas si déjà fait
uv venv

# 4. Installer Airflow 2.8.4 + provider GCP
echo "🐍 Installation d'Airflow 2.8.4 et des providers..."
uv pip install "apache-airflow[google]==2.8.4" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.4/constraints-3.11.txt"
uv pip install "apache-airflow-providers-google==10.1.1"

# 5. Synchroniser et activer venv
uv sync
source .venv/bin/activate

# 6. Lancer le setup airflow local
echo "⚙️ Lancement de setup_airflow.sh..."
chmod +x setup_airflow.sh
./setup_airflow.sh

echo ""
echo "✅ VM prête avec Airflow 2.8.4 🎉"
echo "👉 Pour démarrer manuellement :"
echo "   source .env.airflow"
echo "   source .venv/bin/activate"
echo "   airflow webserver --port 8080 &"
echo "   airflow scheduler &"
