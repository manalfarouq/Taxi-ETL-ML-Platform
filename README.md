# 🚕 Taxi ETA ML Platform - Documentation

## Vue d'ensemble du projet

Plateforme complète de prédiction de temps d'arrivée estimé (ETA) pour les trajets de taxi urbains, combinant Big Data, Machine Learning et API REST sécurisée.

### Objectifs

- Construire un pipeline ETL distribué avec PySpark
- Nettoyer et enrichir les données de taxi NYC
- Entraîner un modèle de régression haute performance (R² = 0.98)
- Déployer une API FastAPI sécurisée par JWT
- Fournir des analytics SQL avancés
- Assurer la traçabilité des prédictions

---

## Architecture du projet

### Structure des dossiers
```
TAXI-ETL-ML-PLATFORM/
├── app/                          # Application FastAPI
│   ├── routes/                   # Endpoints API
│   │   ├── analytics_router.py   # Analytics SQL
│   │   ├── auth_router.py        # Authentification JWT
│   │   └── prediction_router.py  # Prédictions ETA
│   ├── schemas/                  # Schémas Pydantic
│   │   └── PredictRequest_schema.py
│   ├── services/                 # Logique métier
│   │   └── prediction_service.py # Service de prédiction
│   ├── models/                   # Modèles SQLAlchemy
│   │   ├── prediction.py         # Table eta_predictions
│   │   └── user.py               # Table users
│   ├── auth/                     # Sécurité
│   ├── core/                     # Configuration
│   └── db/                       # Connexion base de données
├── ML/                           # Machine Learning
│   ├── notebook/                 # Jupyter notebooks
│   │   └── main.ipynb            # Pipeline complet
│   └── models/                   # Modèles entraînés
│       └── eta_model/            # Pipeline PySpark sauvegardé
├── dags/                         # Airflow DAGs (optionnel)
├── tests/                        # Tests unitaires
└── docker-compose.yml            # Orchestration Docker
```

---

## Technologies utilisées

### Backend & API
- **FastAPI** : Framework API moderne et performant
- **SQLAlchemy** : ORM pour PostgreSQL
- **JWT** : Authentification sécurisée
- **Uvicorn** : Serveur ASGI

### Big Data & ML
- **PySpark** : Traitement distribué de données
- **MLlib** : Modèles de Machine Learning (GBTRegressor)
- **PostgreSQL** : Stockage données Silver et prédictions

### DevOps
- **Docker** : Conteneurisation
- **Airflow** : Orchestration workflows (optionnel)

---

## Pipeline de données

### 1️⃣ Zone Bronze (Données brutes)
- Ingestion du dataset taxi NYC (format Parquet)
- ~3.5M enregistrements initiaux
- Colonnes : 20 features + timestamps

### 2️⃣ Zone Silver (Données nettoyées)
**Nettoyage effectué :**
- Suppression valeurs nulles
- Filtrage durées ≤ 0 minutes
- Filtrage distances aberrantes (> 200 miles)
- Filtrage passagers ≤ 0 ou > 8
- Détection outliers (méthode IQR)
- Conservation uniquement des trajets avec ≤ 4 outliers

**Features ajoutées :**
- `trip_duration` : Durée calculée en minutes
- `pickuphour` : Heure de prise en charge (0-23)
- `dayof_week` : Jour de la semaine (1-7)
- `speed` : Vitesse moyenne (distance/temps)

**Résultat :** ~2.6M lignes propres stockées dans PostgreSQL

### 3️⃣ Machine Learning

**Features numériques (11) :**
- trip_distance, fare_amount, tip_amount, tolls_amount, total_amount
- Airport_fee, passenger_count, extra, mta_tax, congestion_surcharge, speed

**Features catégorielles (7) :**
- RatecodeID, pickuphour, dayof_week, VendorID
- PULocationID, DOLocationID, payment_type

**Pipeline ML :**
1. StringIndexer → Conversion catégories en indices
2. OneHotEncoder → Encodage one-hot
3. VectorAssembler → Assemblage features
4. GBTRegressor → Gradient Boosted Trees

**Performance :**
- **RMSE** : 1.14 minutes
- **MAE** : 0.51 minutes
- **R²** : 0.9769 (97.69% de variance expliquée)

---

## Configuration base de données

### Création base PostgreSQL

Exécuter le script SQL fourni pour créer :
- Base de données `taxi_nyc`
- Utilisateur `taxi_user`
- Tables : `users`, `taxi_trips`, `eta_predictions`

### Variables d'environnement

Créer un fichier `.env` à la racine :
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=taxi_nyc
DB_USER=taxi_user
DB_PASSWORD=ton_mot_de_passe_securise

SECRET_KEY=ta_cle_secrete_jwt_longue_et_aleatoire
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
```

---

## Démarrage de l'application

### Installation des dépendances
```bash
pip install -r requirements.txt
```

Dépendances principales :
- fastapi
- uvicorn
- pyspark
- sqlalchemy
- psycopg2-binary
- python-jose[cryptography]
- passlib
- pydantic

### Lancement serveur API
```bash
cd app
python main.py
```

L'API sera accessible sur `http://localhost:8000`

Documentation interactive : `http://localhost:8000/docs`

---

## Endpoints API

### Authentification

**POST `/auth/register`**
- Créer un nouvel utilisateur
- Body : `{"username": "user1", "password": "pass123"}`

**POST `/auth/login`**
- Obtenir un token JWT
- Body : `{"username": "user1", "password": "pass123"}`
- Retourne : `{"access_token": "...", "token_type": "bearer"}`

**GET `/auth/me`**
- Informations sur le système d'authentification

### Prédiction

**POST `/predict/`** (Protégé par JWT)
- Prédire la durée d'un trajet
- Header : `Authorization: Bearer {token}`
- Body :
```json
{
  "trip_distance": 2.5,
  "fare_amount": 12.0,
  "tip_amount": 2.0,
  "total_amount": 16.5,
  "passenger_count": 1,
  "VendorID": 1,
  "PULocationID": 161,
  "DOLocationID": 236
}
```
- Retourne : `{"estimated_duration": 12.34, "timestamp": "...", "prediction_id": 1}`

### Analytics

**GET `/analytics/avg-duration-by-hour`**
- Durée moyenne par heure de la journée
- Utilise CTE SQL
- Retourne : `[{"pickuphour": 8, "avg_duration": 18.4, "total_trips": 12500}, ...]`

**GET `/analytics/payment-analysis`**
- Analyse par type de paiement
- Statistiques complètes (min/max/avg/count)
- Retourne : `[{"payment_type": 1, "total_trips": 125430, "avg_duration": 21.6, ...}, ...]`

---

## Tests

### Test de l'API avec curl
```bash
# 1. Créer un utilisateur
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"test", "password":"test123"}'

# 2. Se connecter
TOKEN=$(curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"test", "password":"test123"}' | jq -r '.access_token')

# 3. Faire une prédiction
curl -X POST http://localhost:8000/predict/ \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "trip_distance": 2.5,
    "fare_amount": 12.0,
    "total_amount": 16.5,
    "passenger_count": 1,
    "VendorID": 1,
    "PULocationID": 161,
    "DOLocationID": 236
  }'

# 4. Analytics
curl http://localhost:8000/analytics/avg-duration-by-hour
```

---

## Améliorations du modèle

### Évolution du R²

- **v0.1** (baseline) : R² = 0.89
- **v1.0** (optimisé) : R² = 0.98

### Optimisations appliquées

1. **Ajout zones géographiques** : PULocationID et DOLocationID sont des features cruciales pour NYC
2. **Feature engineering vitesse** : Corrélation directe avec la durée
3. **Nettoyage strict outliers** : Amélioration qualité données
4. **Filtrage plages réalistes** :
   - Distance : 0.1 - 50 miles
   - Durée : 1 - 120 minutes
   - Passagers : 1 - 6
   - Vitesse : 1 - 60 mph

---

## Stockage des prédictions

Chaque prédiction est enregistrée dans `eta_predictions` avec :
- Toutes les features d'entrée
- Prédiction calculée
- Version du modèle
- Timestamp

**Utilité :**
- Suivi qualité modèle en production
- Détection drift
- Analytics historiques
- A/B testing futures versions

---

## SQL Analytics avancé

Les endpoints analytics utilisent du SQL pur exécuté via SQLAlchemy pour :
- Performance maximale (pas de post-processing Python)
- Exploitation index PostgreSQL
- CTEs pour requêtes complexes
- Scalabilité

---

## Déploiement Docker (optionnel)

Si vous utilisez Docker Compose, la stack complète peut inclure :
- PostgreSQL
- Airflow (scheduler + webserver + workers)
- PySpark standalone cluster
- FastAPI service

---

## Bonnes pratiques implémentées

✅ **Sécurité** : JWT, hachage mots de passe, validation Pydantic  
✅ **Performance** : SQL optimisé, batch predictions possibles  
✅ **Maintenabilité** : Code modulaire, typage fort  
✅ **Observabilité** : Logs prédictions, timestamps  
✅ **Scalabilité** : PySpark distribué, API stateless  


---

## Ressources

- [Documentation FastAPI](https://fastapi.tiangolo.com/)
- [PySpark MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [NYC Taxi Dataset](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

## Contribution

Ce projet est un exemple pédagogique démontrant une stack Data Engineering complète.

---

## Licence

Projet éducatif - Données NYC Taxi sous licence publique.

---

**Version** : 1.0.0  
**Dernière mise à jour** : Janvier 2025