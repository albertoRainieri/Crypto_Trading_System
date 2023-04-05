import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from fastapi import APIRouter, status
from fastapi import Depends, HTTPException, Request
from app.Controller.LoggingController import LoggingController
from database.DatabaseConnection import DatabaseConnection
from app.Controller.AuthController import Authorization
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from app.Models.Auth import Token
from passlib.context import CryptContext
from jose import JWTError, jwt
from constants.constants import *
from constants.constants import clients_oauth2
from datetime import timedelta
import json
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder



router = APIRouter()
    

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


@router.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = Authorization.authenticate_user(clients_oauth2, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = Authorization.create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}
