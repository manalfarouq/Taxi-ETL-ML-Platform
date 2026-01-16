from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.routes import prediction_router, analytics_router, auth_router
from app.db.db_connection import engine, Base
from app.services.prediction_service import get_prediction_service


# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     """Gestion du cycle de vie de l'application"""
    
#     # Startup
#     print("🚀 Démarrage de l'application...")
    
#     # Créer les tables
#     Base.metadata.create_all(bind=engine)
#     print("✅ Tables créées")
    
#     # Charger le modèle
#     try:
#         service = get_prediction_service()
#         print(f"✅ Modèle chargé (version: {service.model_version})")
#     except Exception as e:
#         print(f"⚠️  Erreur chargement modèle: {e}")
    
#     yield
    
#     # Shutdown
#     print("🛑 Arrêt de l'application...")
#     try:
#         service = get_prediction_service()
#         service.close()
#         print("✅ Spark fermé")
#     except:
#         pass


# Créer l'application
app = FastAPI(
    # title="API Prédiction ETA Taxi NYC",
    # description="API de prédiction de durée de trajet",
    # version="1.0.0",
    # lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routes
# @app.get("/", tags=["Root"])
# def root():
#     return {
#         "message": "API Prédiction ETA Taxi NYC",
#         "version": "1.0.0",
#         "docs": "/docs"
#     }

# @app.get("/health", tags=["Health"])
# def health():
#     try:
#         service = get_prediction_service()
#         return {
#             "status": "healthy",
#             "model_version": service.model_version
#         }
#     except:
#         return {
#             "status": "unhealthy",
#             "model_version": None
#         }

# Inclure les routers
app.include_router(auth_router.router)
app.include_router(prediction_router.router)
app.include_router(analytics_router.router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)