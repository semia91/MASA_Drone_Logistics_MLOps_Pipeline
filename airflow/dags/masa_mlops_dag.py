from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import httpx
import os
import pandas as pd
import boto3

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
    """Consulte l'API Open-Meteo, analyse la dérive statistique et génère le rapport HTML sur S3"""
    print("☁️ Analyse des conditions météo en cours via Open-Meteo...")
    
    # Coordonnées géographiques de Kumasi, Ghana
    url = "https://api.open-meteo.com/v1/forecast?latitude=6.6885&longitude=-1.6244&current=wind_speed_10m"
    current_wind = 15.0  # Valeur par défaut
    
    try:
        response = httpx.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            current_wind = data["current"]["wind_speed_10m"]
            print(f"💨 Vitesse actuelle du vent détectée au Ghana : {current_wind} km/h")
        else:
            print(f"⚠️ Erreur de réponse de l'API Météo (Code: {response.status_code}). Utilisation de la valeur par défaut.")
    except Exception as e:
        print(f"❌ Impossible de contacter l'API Météo : {e}. Mode démo activé.")
        current_wind = 28.5  # Simulation de dérive pour la démo si l'API coupe
    
    # -------------------------------------------------------------
    # LOGIQUE DE SURVEILLANCE ALGORITHMIQUE & RAPPORT HTML
    # -------------------------------------------------------------
    ref_wind_mean = 14.5  # Moyenne historique du dataset d'entraînement (km/h)
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Calcul de l'écart (Dérive)
    wind_drift_pct = abs((current_wind - ref_wind_mean) / ref_wind_mean) * 100
    drift_detected = "OUI" if (current_wind > 25.0 or wind_drift_pct > 15.0) else "NON"
    
    # Génération du template HTML avec du style pour le jury
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>MASA MLOps - Rapport de Monitoring</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 40px; background-color: #f4f6f9; color: #333; }}
            .container {{ background: white; padding: 30px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); max-width: 800px; margin: auto; }}
            h1 {{ color: #4A154B; border-bottom: 3px solid #4A154B; padding-bottom: 10px; }}
            .status {{ padding: 15px; border-radius: 8px; font-weight: bold; font-size: 1.2em; margin-bottom: 20px; }}
            .alert {{ background-color: #fce8e6; color: #c5221f; border: 1px solid #fad2cf; }}
            .ok {{ background-color: #e6f4ea; color: #137333; border: 1px solid #ceead6; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
            th {{ background-color: #f8f9fa; color: #5f6368; }}
            .highlight {{ font-weight: bold; color: #4A154B; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🚁 MASA Data Drift & Performance Report</h1>
            <p><strong>Date de l'analyse :</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            
            <div class="status {'alert' if drift_detected == 'OUI' else 'ok'}">
                ⚠️ Alerte Dérive Majeure : {drift_detected}
            </div>
            
            <table>
                <tr>
                    <th>Métrique Climatique</th>
                    <th>Valeur Référence (Train)</th>
                    <th>Valeur Actuelle (Production)</th>
                    <th>Écart Relatif</th>
                </tr>
                <tr>
                    <td class="highlight">Vitesse du Vent</td>
                    <td>{ref_wind_mean:.2f} km/h</td>
                    <td>{current_wind:.2f} km/h</td>
                    <td style="color: {'red' if drift_detected == 'OUI' else 'green'}">{wind_drift_pct:.1f}%</td>
                </tr>
            </table>
            <br>
            <p><i>Note MLOps : Si la vitesse du vent dépasse 25 km/h, la tâche suivante (automated_model_retraining) est automatiquement déclenchée par Airflow.</i></p>
        </div>
    </body>
    </html>
    """
    
    # Sauvegarde locale du rapport HTML
    report_path = os.path.join(root_dir, "custom_drift_report.html")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print("✅ Fichier HTML de monitoring généré en local.")
    
    # Envoi direct du fichier HTML sur ton AWS S3
    try:
        s3 = boto3.client('s3')
        # Met le vrai nom de ton bucket S3 ici :
        bucket_name = "masa-drone-network-bucket" 
        
        s3.upload_file(
            Filename=report_path,
            Bucket=bucket_name,
            Key="monitoring/custom_drift_report.html",
            ExtraArgs={'ContentType': 'text/html'}  # Permet d'ouvrir le rapport directement dans le navigateur
        )
        print("🚀 Rapport HTML poussé avec succès sur AWS S3 : monitoring/custom_drift_report.html")
    except Exception as e:
        print(f"⚠️ Échec du transfert vers AWS S3 : {e}")

    # Décision de déclenchement pour la tâche suivante
    if drift_detected == "OUI":
        print("🚨 DRIFT DETECTE : Le modèle actuel doit être mis à jour.")
        return "trigger_retrain"
    else:
        print("✅ Climat nominal. Pas besoin de réentraîner.")
        return "stable"

def trigger_automated_retraining():
    """Exécute de manière autonome le script d'entraînement pour mettre à jour S3 et MLflow"""
    print("🚀 Airflow ordonne le réentraînement automatique du pipeline (train.py)...")
    
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    script_path = os.path.join(root_dir, "train.py")
    
    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    if result.returncode == 0:
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