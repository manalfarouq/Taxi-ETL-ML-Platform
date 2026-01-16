"""
Tests unitaires pour l'API
"""
import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


# ===== TESTS AUTHENTIFICATION =====

def test_login_success():
    """Test login avec credentials valides"""
    response = client.post("/auth/login", json={
        "username": "admin",
        "password": "admin123"
    })
    assert response.status_code == 200
    assert "access_token" in response.json()
    assert response.json()["token_type"] == "bearer"



def test_register():
    """Test création utilisateur"""
    response = client.post("/auth/register", json={
        "username": "testuser",
        "password": "testpass"
    })
    assert response.status_code == 200
    assert "user_id" in response.json()


# ===== TESTS PREDICTION =====


def test_prediction_with_auth():
    """Test prédiction avec authentification"""
    # D'abord login
    login = client.post("/auth/login", json={
        "username": "admin",
        "password": "admin123"
    })
    token = login.json()["access_token"]
    
    # Puis prédiction
    response = client.post(
        "/predict/",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "trip_distance": 2.5,
            "fare_amount": 12.0,
            "total_amount": 16.5,
            "passenger_count": 1,
            "VendorID": 1,
            "PULocationID": 161,
            "DOLocationID": 236
        }
    )
    assert response.status_code == 200
    data = response.json()
    assert "estimated_duration" in data
    assert data["estimated_duration"] > 0
