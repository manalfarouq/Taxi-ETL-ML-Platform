-- Créer la base de données
CREATE DATABASE taxi_nyc;

-- Créer un nouvel utilisateur
CREATE USER taxi_user WITH PASSWORD 'ton_mot_de_passe_securise';

-- Donner tous les droits sur la base taxi_nyc
GRANT ALL PRIVILEGES ON DATABASE taxi_nyc TO taxi_user;

-- Donner les droits sur le schéma public
GRANT ALL ON SCHEMA public TO taxi_user;

-- Créer la table
CREATE TABLE taxi_trips (
    trip_id SERIAL PRIMARY KEY,
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    RatecodeID INTEGER,
    store_and_fwd_flag VARCHAR(1),
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    Airport_fee DOUBLE PRECISION,
    cbd_congestion_fee DOUBLE PRECISION,
    trip_duration DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);