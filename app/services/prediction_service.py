from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, TimestampType
import pyspark.sql.functions as F
from datetime import datetime
from typing import Dict

class PredictionService:
    def __init__(self, model_path: str = "./models/eta_model"):
        # Initialiser Spark
        self.spark = SparkSession.builder \
            .appName("ETAPrediction") \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        
        # Charger le modèle
        self.model = PipelineModel.load(model_path)
        self.model_version = "v1.0"
    
    def prepare_features(self, data: Dict) -> Dict:
        """Prépare les features à partir des données d'entrée"""
        
        # Extraire l'heure et le jour de la semaine
        pickup_time = data.get("tpep_pickup_datetime", datetime.now())
        if isinstance(pickup_time, str):
            pickup_time = datetime.fromisoformat(pickup_time.replace('Z', '+00:00'))
        
        pickup_hour = pickup_time.hour
        day_of_week = pickup_time.weekday() + 1  # PySpark dayofweek commence à 1
        
        # Calculer la vitesse si non fournie
        speed = data.get("speed")
        if speed is None:
            # Estimation basique : vitesse moyenne NYC = 10 mph
            speed = 10.0
        
        return {
            "trip_distance": data["trip_distance"],
            "fare_amount": data["fare_amount"],
            "tip_amount": data.get("tip_amount", 0.0),
            "tolls_amount": data.get("tolls_amount", 0.0),
            "total_amount": data["total_amount"],
            "Airport_fee": data.get("Airport_fee", 0.0),
            "passenger_count": data["passenger_count"],
            "extra": data.get("extra", 0.0),
            "mta_tax": data.get("mta_tax", 0.5),
            "congestion_surcharge": data.get("congestion_surcharge", 0.0),
            "speed": speed,
            "RatecodeID": data.get("RatecodeID", 1),
            "VendorID": data["VendorID"],
            "PULocationID": data["PULocationID"],
            "DOLocationID": data["DOLocationID"],
            "payment_type": data.get("payment_type", 1),
            "pickuphour": pickup_hour,
            "dayof_week": day_of_week,
            "tpep_pickup_datetime": pickup_time
        }
    
    def predict(self, data: Dict) -> float:
        """Fait une prédiction de durée de trajet"""
        
        # Préparer les features
        features = self.prepare_features(data)
        
        # Créer le schéma Spark
        schema = StructType([
            StructField("trip_distance", DoubleType(), False),
            StructField("fare_amount", DoubleType(), False),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("total_amount", DoubleType(), False),
            StructField("Airport_fee", DoubleType(), True),
            StructField("passenger_count", IntegerType(), False),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("RatecodeID", IntegerType(), True),
            StructField("VendorID", IntegerType(), False),
            StructField("PULocationID", IntegerType(), False),
            StructField("DOLocationID", IntegerType(), False),
            StructField("payment_type", IntegerType(), True),
            StructField("pickuphour", IntegerType(), False),
            StructField("dayof_week", IntegerType(), False),
            StructField("tpep_pickup_datetime", TimestampType(), False)
        ])
        
        # Créer DataFrame Spark
        df = self.spark.createDataFrame([features], schema=schema)
        
        # Faire la prédiction
        prediction = self.model.transform(df)
        
        # Extraire la valeur
        result = prediction.select("prediction").first()[0]
        
        return float(result)
    
    def close(self):
        """Fermer la session Spark"""
        self.spark.stop()

# Instance globale
prediction_service = None

def get_prediction_service() -> PredictionService:
    """Singleton pour le service de prédiction"""
    global prediction_service
    if prediction_service is None:
        prediction_service = PredictionService()
    return prediction_service