from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from datetime import timedelta
from app.auth.token_auth import create_access_token, Token
from app.core.config import settings

router = APIRouter(prefix="/auth", tags=["Authentication"])


class LoginRequest(BaseModel):
    """Modèle de requête pour le login"""
    username: str
    password: str


# Base de données utilisateurs simple (à remplacer par une vraie DB en production)
# ⚠️ En production: utiliser bcrypt pour hasher les mots de passe
fake_users_db = {
    "admin": {
        "user_id": 1,
        "username": "admin",
        "password": "admin123",
        "role": "admin"
    },
    "user": {
        "user_id": 2,
        "username": "user",
        "password": "user123",
        "role": "user"
    }
}


@router.post("/login", response_model=Token)
def login(request: LoginRequest):
    """
    Authentification et génération de token JWT
    
    **Credentials par défaut:**
    - username: `admin` / password: `admin123`
    - username: `user` / password: `user123`
    
    **Retour:**
    - `access_token`: Token JWT à utiliser dans le header Authorization
    - `token_type`: Type de token (bearer)
    
    **Utilisation:**
    ```
    Authorization: Bearer <access_token>
    ```
    """
    
    user = fake_users_db.get(request.username)
    
    if not user or user["password"] != request.password:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Nom d'utilisateur ou mot de passe incorrect",
            headers={"WWW-Authenticate": "Bearer"}
        )
    
    # Créer le token avec les informations de l'utilisateur
    # Utilise settings.ACCESS_TOKEN_EXPIRE_MINUTES depuis .env
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={
            "sub": user["username"],
            "user_id": user["user_id"],
            "role": user["role"]
        },
        expires_delta=access_token_expires
    )
    
    return Token(access_token=access_token, token_type="bearer")


@router.post("/register")
def register(request: LoginRequest):
    """
    Enregistrer un nouvel utilisateur
    
    **Note:** En production, les mots de passe doivent être hashés avec bcrypt
    """
    
    if request.username in fake_users_db:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cet utilisateur existe déjà"
        )
    
    # Générer un nouvel ID utilisateur
    new_user_id = max([u["user_id"] for u in fake_users_db.values()]) + 1
    
    # Ajouter l'utilisateur à la "base de données"
    fake_users_db[request.username] = {
        "user_id": new_user_id,
        "username": request.username,
        "password": request.password,  # ⚠️ À hasher en production
        "role": "user"
    }
    
    return {
        "message": "Utilisateur créé avec succès",
        "user_id": new_user_id,
        "username": request.username
    }


@router.get("/me")
def get_me():
    """
    Obtenir les informations de l'utilisateur connecté
    (endpoint de test pour vérifier l'authentification)
    """
    return {
        "message": "Pour obtenir vos infos, utilisez le token dans Authorization header",
        "users_available": list(fake_users_db.keys()),
        "token_expires_in_minutes": settings.ACCESS_TOKEN_EXPIRE_MINUTES
    }