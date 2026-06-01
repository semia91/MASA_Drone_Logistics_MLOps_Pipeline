from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)

def test_api_health():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"

def test_api_prediction():
    payload = {
        "hospital_latitude": 9.4,
        "hospital_longitude": -0.8,
        "weight_kg": 1.5,
        "wind_speed_kmh": 10.0,
        "air_temperature_c": 28.0
    }
    response = client.post("/predict", json=payload)
    assert response.status_code == 200
    assert "assigned_hub_id" in response.json()