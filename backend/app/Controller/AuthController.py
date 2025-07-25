import os,sys
sys.path.insert(0,'../..')

from passlib.context import CryptContext
from pydantic import BaseModel
from typing import Union
#from .AuthController import User
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from app.Models.Auth import *
from constants.constants import *
from jose import JWTError, jwt
from fastapi import APIRouter
from datetime import timedelta, datetime

class Authorization:

    global pwd_context 
    pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

    def verify_password(plain_password, hashed_password):
        return pwd_context.verify(plain_password, hashed_password)

    
    def get_password_hash(password):
        return pwd_context.hash(password)

    
    def get_user(db, username: str):
        if username in db:
            user_dict = db[username]
            return UserInDB(**user_dict)

    
    def authenticate_user(fake_db, username: str, password: str):
        #print(fake_db)
        user = Authorization.get_user(fake_db, username)
        print(user)
        #print(user.hashed_password, password)
        if not user:
            return False
        if not Authorization.verify_password(password, user.hashed_password):
            return False
        return user

    def create_access_token(data: dict, expires_delta: Union[timedelta, None] = None):
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(minutes=15)
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
        return encoded_jwt

    
    async def get_current_user(token: str = Depends(oauth2_scheme)):
        credentials_exception = HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
        print(token)
        print(SECRET_KEY)
        print(ALGORITHM)
        try:
            payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            username: str = payload.get("sub")
            print(payload)
            if username is None:
                raise credentials_exception
            token_data = TokenData(username=username)
        except JWTError:
            raise credentials_exception
        user = Authorization.get_user(clients_oauth2, username=token_data.username)
        if user is None:
            raise credentials_exception
        return user

    
    async def get_current_active_user(current_user: User = Depends(get_current_user)):
        if current_user.disabled:
            raise HTTPException(status_code=400, detail="Inactive user")
        return current_user




