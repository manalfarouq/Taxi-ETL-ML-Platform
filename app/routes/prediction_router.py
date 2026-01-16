from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from datetime import datetime

from app.schemas.PredictRequest_schema import PredictRequest, PredictResponse
from app.services.prediction_service import get_prediction_service
from app.models.prediction import ETAPrediction
from app.db.db_connection import get_db_session
from app.auth.token_auth import get_current_user

router = APIRouter(prefix="/predict", tags=["Prediction"])


@router.post("/", response_model=PredictResponse)
def predict_eta(
    request: PredictRequest,
    db: Session = Depends(get_db_session),
    user_id: int = Depends(get_current_user)
):
    """Prédit la durée d'un trajet de taxi"""
    try:
        service = get_prediction_service()

        # Convert request to dict
        data = request.model_dump()

        # Predict
        predicted_duration = service.predict(data)

        # Prepare features (pour avoir toutes les valeurs calculées)
        features = service.prepare_features(data)

        # Save in DB avec TOUTES les features
        prediction = ETAPrediction(
            trip_distance=features["trip_distance"],
            fare_amount=features["fare_amount"],
            tip_amount=features["tip_amount"],
            tolls_amount=features["tolls_amount"],
            total_amount=features["total_amount"],
            airport_fee=features["Airport_fee"],
            passenger_count=features["passenger_count"],
            extra=features["extra"],
            mta_tax=features["mta_tax"],
            congestion_surcharge=features["congestion_surcharge"],
            speed=features["speed"],
            ratecode_id=features["RatecodeID"],
            vendor_id=features["VendorID"],
            pu_location_id=features["PULocationID"],
            do_location_id=features["DOLocationID"],
            payment_type=features["payment_type"],
            pickup_hour=features["pickuphour"],
            day_of_week=features["dayof_week"],
            predicted_duration=predicted_duration,
            timestamp=datetime.now()
        )

        db.add(prediction)
        db.commit()
        db.refresh(prediction)

        return PredictResponse(
            estimated_duration=round(predicted_duration, 2),
            timestamp=prediction.timestamp,
            prediction_id=prediction.id
        )

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erreur: {str(e)}")
