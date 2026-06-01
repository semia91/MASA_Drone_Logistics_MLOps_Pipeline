import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib
import mlflow
import mlflow.sklearn
import boto3
import os
from dotenv import load_dotenv

# Chargement sécurisé des credentials du fichier .env
load_dotenv()

def generate_historical_flights(n_samples=1000):
    np.random.seed(42)
    distance = np.random.uniform(5, 65, n_samples)
    weight = np.random.uniform(0.5, 4.5, n_samples)
    wind = np.random.uniform(0, 45, n_samples)
    temp = np.random.uniform(22, 39, n_samples)
    
    eta = (distance * 1.3) + (weight * 1.8) + (wind * 0.4) + np.random.normal(0, 1, n_samples)
    battery_loss = (distance * 0.8) + (wind * 0.5) + (weight * 1.1) + np.random.normal(0, 1, n_samples)
    
    return pd.DataFrame({
        'distance_km': distance, 'weight_kg': weight, 
        'wind_speed_kmh': wind, 'air_temperature_c': temp,
        'eta_minutes': eta, 'battery_loss_pct': battery_loss
    })

def train_and_upload():
    df = generate_historical_flights()
    X = df[['distance_km', 'weight_kg', 'wind_speed_kmh', 'air_temperature_c']]
    y = df[['eta_minutes', 'battery_loss_pct']]
    
    mlflow.set_experiment("MASA_Drone_Inference")
    with mlflow.start_run():
        model = RandomForestRegressor(n_estimators=50, max_depth=6, random_state=42)
        model.fit(X, y)
        
        # Enregistrement MLflow local
        mlflow.sklearn.log_model(model, "model", registered_model_name="MASA_Predict_Model")
        
        # Sauvegarde locale temporaire
        local_pkl = "app/masa_model.pkl"
        os.makedirs("app", exist_ok=True)
        joblib.dump(model, local_pkl)
        print("✅ Modèle sauvegardé localement dans app/masa_model.pkl")
        
        # Envoi sécurisé du modèle sur AWS S3 Cloud Storage
        try:
            s3 = boto3.client(
                's3',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=os.getenv("AWS_DEFAULT_REGION")
            )
            s3.upload_file(local_pkl, os.getenv("AWS_BUCKET_NAME"), "models/masa_model.pkl")
            print(f"☁️ Version du modèle poussée avec succès sur le S3 : {os.getenv('AWS_BUCKET_NAME')}")
        except Exception as e:
            print(f"⚠️ Note d'architecture : Connexion S3 non configurée ou indisponible ({e}). Le modèle reste disponible localement.")

if __name__ == "__main__":
    train_and_upload()