from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    SK: str
    ALG: str = "HS256"
    HF_TOKEN: str
    ACCESS_TOKEN_EXPIRE_MINUTES: int

    DB_HOST: str 
    DB_PORT: int 
    DB_NAME: str 
    DB_USER: str 
    DB_PASSWORD: str 
    
    # Optionnel: pour le modèle ML
    MODEL_PATH: str = "./models/eta_model"
    MODEL_VERSION: str = "v1.0"
    
    # Optionnel: pour Spark
    SPARK_DRIVER_MEMORY: str = "2g"
    SPARK_MASTER: str = "local[*]"

    model_config = {
        "extra": "ignore",
        "env_file": ".env",
        "case_sensitive": True
    }

settings = Settings()