
import os
import httpx
import pandas as pd
import boto3
from datetime import datetime

# --- AJOUTE CES LIGNES ICI ---
from dotenv import load_dotenv
# On charge les variables du fichier .env situé dans le même dossier
load_dotenv() 
# ------------------------------

def test_monitor_ghana_weather_drift():
    print("☁️ [TEST] Analyse des conditions météo en cours via Open-Meteo...")
    
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
            print(f"⚠️ Erreur de réponse de l'API Météo. Utilisation de la valeur par défaut.")
    except Exception as e:
        print(f"❌ Impossible de contacter l'API Météo : {e}. Mode démo activé.")
        current_wind = 28.5  # Simulation de dérive pour le test
    
    # --- LOGIQUE ALGORITHMIQUE & RAPPORT HTML ---
    ref_wind_mean = 14.5  # Moyenne historique
    wind_drift_pct = abs((current_wind - ref_wind_mean) / ref_wind_mean) * 100
    drift_detected = "OUI" if (current_wind > 25.0 or wind_drift_pct > 15.0) else "NON"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>MASA MLOps - Rapport de Monitoring</title>
        <style>
            body {{ font-family: 'Segoe UI', sans-serif; margin: 40px; background-color: #f4f6f9; color: #333; }}
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
            <h1>🚁 MASA Data Drift & Performance Report (Version Algorithmique)</h1>
            <p><strong>Date de l'analyse :</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <div class="status {'alert' if drift_detected == 'OUI' else 'ok'}">
                🚨 Statut Dérive Globale : {drift_detected}
            </div>
            <table>
                <tr>
                    <th>Métrique Climatique</th>
                    <th>Moyenne Référence (Train)</th>
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
        </div>
    </body>
    </html>
    """
    
    # Sauvegarde locale
    report_path = "custom_drift_report.html"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    print("✅ Fichier HTML de monitoring généré en local.")
    
    # Envoi vers S3
    try:
        s3 = boto3.client('s3')
        # Remplace par ton vrai nom de bucket S3
        bucket_name = "masa-drone-network-bucket" 
        
        s3.upload_file(
            Filename=report_path,
            Bucket=bucket_name,
            Key="monitoring/custom_drift_report.html",
            ExtraArgs={'ContentType': 'text/html'}
        )
        print("🚀 Rapport HTML poussé avec succès sur AWS S3 : monitoring/custom_drift_report.html")
    except Exception as e:
        print(f"⚠️ Échec du transfert vers AWS S3 : {e}")

# Lancement immédiat du test
if __name__ == "__main__":
    test_monitor_ghana_weather_drift()