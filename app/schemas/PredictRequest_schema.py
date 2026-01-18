from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional

class PredictRequest(BaseModel):
    # Features numériques
    trip_distance: float = Field(..., gt=0, description="Distance en miles")
    fare_amount: float = Field(..., ge=0)
    tip_amount: float = Field(default=0.0, ge=0)
    tolls_amount: float = Field(default=0.0, ge=0)
    total_amount: float = Field(..., ge=0)
    Airport_fee: float = Field(default=0.0, ge=0)
    passenger_count: int = Field(..., gt=0, le=6)
    extra: float = Field(default=0.0, ge=0)
    mta_tax: float = Field(default=0.5, ge=0)
    congestion_surcharge: float = Field(default=0.0, ge=0)
    speed: Optional[float] = Field(default=None, description="Vitesse (calculée si None)")
    
    # Features catégorielles
    RatecodeID: int = Field(default=1)
    VendorID: int = Field(default=1)
    PULocationID: int = Field(..., description="Zone de prise en charge")
    DOLocationID: int = Field(..., description="Zone de dépose")
    payment_type: int = Field(default=1)
    
    # Pour calculer pickuphour et dayof_week
    tpep_pickup_datetime: datetime = Field(default_factory=datetime.now)
    
    class Config:
        json_schema_extra = {
            "example": {
                "trip_distance": 2.5,
                "fare_amount": 12.0,
                "tip_amount": 2.0,
                "total_amount": 16.5,
                "passenger_count": 1,
                "VendorID": 1,
                "PULocationID": 161,
                "DOLocationID": 236
            }
        }

class PredictResponse(BaseModel):
    estimated_duration: float = Field(..., description="Durée estimée en minutes")
    timestamp: datetime = Field(default_factory=datetime.now)
    prediction_id: Optional[int] = None