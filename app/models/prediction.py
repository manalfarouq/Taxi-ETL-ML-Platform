from sqlqlchemy import Column, Integer, String, Float, DateTime
from ..db.db_connection import Base
import datetime

class ETAPrediction(Base):
    __tablename__ = "eta_predictions"

    id = Column(Integer, primary_key=True, index=True)
    
    # Features d'entrée
    trip_distance = Column(Float, nullable=False)
    fare_amount = Column(Float, nullable=False)
    tip_amount = Column(Float, default=0.0)
    tolls_amount = Column(Float, default=0.0)
    total_amount = Column(Float, nullable=False)
    airport_fee = Column(Float, default=0.0)
    passenger_count = Column(Integer, nullable=False)
    extra = Column(Float, default=0.0)
    mta_tax = Column(Float, default=0.5)
    congestion_surcharge = Column(Float, default=0.0)
    speed = Column(Float, nullable=True)
    
    ratecode_id = Column(Integer, default=1)
    vendor_id = Column(Integer, nullable=False)
    pu_location_id = Column(Integer, nullable=False)
    do_location_id = Column(Integer, nullable=False)
    payment_type = Column(Integer, default=1)
    
    pickup_hour = Column(Integer, nullable=False)
    day_of_week = Column(Integer, nullable=False)
    
    # Prédiction
    predicted_duration = Column(Float, nullable=False)
    
    # Métadonnées
    model_version = Column(String(50), default="v1.0")
    timestamp = Column(DateTime, default=datetime.now)