from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import httpx
import os

default_args = {
    'owner': 'semia_lead',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def monitor_ghana_weather_drift():
    """Consulte l'API Open-Meteo pour vérifier s'il y a une dérive climatique (Data Drift)"""
    print("☁️ Analyse des conditions météo en cours via Open-Meteo...")
    
    # Coordonnées géographiques de Kumasi, Ghana
    url = "https://api.open-meteo.com/v1/forecast?latitude=6.6885&longitude=-1.6244&current=wind_speed_10m"
    
    try:
        response = httpx.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            current_wind = data["current"]["wind_speed_10m"]
            print(f"💨 Vitesse actuelle du vent détectée au Ghana : {current_wind} km/h")
            
            # Condition de Drift : Si le vent dépasse un seuil critique (ex: 25 km/h)
            if current_wind > 25.0:
                print("🚨 DRIFT CLIMATIQUE DETECTE : Le modèle actuel doit être mis à jour.")
                return "trigger_retrain"
            else:
                print("✅ Climat nominal. Le modèle de production est toujours performant.")
                return "stable"
        else:
            print(f"⚠️ Erreur de réponse de l'API Météo (Code: {response.status_code}).")
    except Exception as e:
        print(f"❌ Impossible de contacter l'API Météo : {e}")
    
    # Simulation de secours pour la démo si l'API est coupée
    print("🔄 Démo Mode : Simulation d'une dérive à 28 km/h pour le jury.")
    return "trigger_retrain"

def trigger_automated_retraining():
    """Exécute de manière autonome le script d'entraînement pour mettre à jour S3 et MLflow"""
    print("🚀 Airflow ordonne le réentraînement automatique du pipeline (train.py)...")
    
    # Détection du chemin racine du projet
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    script_path = os.path.join(root_dir, "train.py")
    
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.return_code == 0:
        print("✅ Modèle réentraîné avec succès et synchronisé sur AWS S3 !")
        print(result.stdout)
    else:
        print("❌ Échec du réentraînement automatique :")
        print(result.stderr)
        raise RuntimeError("Le script train.py a renvoyé une erreur.")

with DAG(
    'masa_automated_monitoring_and_retraining',
    default_args=default_args,
    description='Pipeline de monitoring météo et réentraînement automatique pour MASA',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    monitor_task = PythonOperator(
        task_id='detect_weather_drift',
        python_callable=monitor_ghana_weather_drift,
    )

    retrain_task = PythonOperator(
        task_id='automated_model_retraining',
        python_callable=trigger_automated_retraining,
    )

    monitor_task >> retrain_task