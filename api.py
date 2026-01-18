from pyspark.sql import SparkSession
from pyspark.sql.functions import col,hour,month,dayofweek,unix_timestamp


def get_spark():
    """
    Ce qui se passe :
    - Spark démarre
    - Prépare l'exécution distribuée
    """
    return (
        SparkSession.builder
        .appName("Taxi-ETL")
        .getOrCreate()
    )


def bronze_layer(spark):
    """
    Ce qui se passe :
    - Spark lit le fichier brut
    - Aucune modification
    - Données encore sales
    """
    df = spark.read.parquet("dataset.parquet")
    return df


def silver_layer(df):
    """
    Ce qui se passe :
    - Création de la cible ML
    - Suppression des données absurdes
    - Création de features temporelles
    """
    df = df.withColumn(
        "duration_minutes",
        (
            unix_timestamp("tpep_dropoff_datetime")
            - unix_timestamp("tpep_pickup_datetime")
        ) / 60
    )

    df = df.filter(
        (col("trip_distance") > 0) &
        (col("trip_distance") <= 200) &
        (col("duration_minutes") > 0) &
        (col("passenger_count") > 0)
    )

    df = (
        df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
          .withColumn("day_of_week", dayofweek("tpep_pickup_datetime"))
          .withColumn("month", month("tpep_pickup_datetime"))
    )

    return df


def write_to_postgres(df):
    """
    Ce qui se passe :
    - Les données propres arrivent dans PostgreSQL
    - Table : silver_taxi_trips
    """
    (
        df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/taxi")
        .option("dbtable", "silver_taxi_trips")
        .option("user", "taxi")
        .option("password", "taxi")
        .mode("overwrite")
        .save()
    )


def train_model():
    """
    Ce qui se passe :
    - Données Silver → Pandas
    - Entraînement du modèle ETA
    - Sauvegarde model.pkl
    """
    import pandas as pd
    from sqlalchemy import create_engine
    from sklearn.ensemble import RandomForestRegressor
    import joblib

    engine = create_engine("postgresql://taxi:taxi@postgres:5432/taxi")
    df = pd.read_sql("SELECT * FROM silver_taxi_trips", engine)

    X = df[
        [
            "trip_distance",
            "passenger_count",
            "pickup_hour",
            "day_of_week",
            "month",
            "payment_type"
        ]
    ]
    y = df["duration_minutes"]

    model = RandomForestRegressor(n_estimators=100)
    model.fit(X, y)

    joblib.dump(model, "model.pkl")
