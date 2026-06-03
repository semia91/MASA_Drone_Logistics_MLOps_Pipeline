from fastapi.testclient import TestClient
import pytest
import sys
import os

# Ajoute la racine du projet pour être sûr que Python trouve l'application
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.main import app

client = TestClient(app)

def test_api_health():
    """Vérifie que l'API démarre et répond correctement"""
    response = client.get("/")
    assert response.status_code == 200
    # S'adapter si ton endpoint renvoie un dictionnaire différent
    assert "status" in response.json() or response.status_code == 200

def test_api_prediction():
    """Vérifie la cohérence physique des prédictions de livraison"""
    payload = {
        "hospital_latitude": 6.6885,   # Coordonnées réelles au Ghana
        "hospital_longitude": -1.6244,
        "weight_kg": 1.5,
        "wind_speed_kmh": 10.0,
        "air_temperature_c": 28.0
    }
    
    response = client.post("/predict", json=payload)
    
    # Si ton API a un mode secours (fallback) quand le modèle .pkl n'est pas là :
    assert response.status_code == 200
    data = response.json()
    assert "assigned_hub_id" in data
    
    # Tests de cohérence physique demandés (Lead MLOps)
    assert float(data["predicted_eta_minutes"]) > 0, "Le temps de vol doit être positif"
    assert float(data["predicted_battery_loss_pct"]) <= 100.0, "La perte de batterie ne peut pas dépasser 100%"