from fastapi import APIRouter, Depends, HTTPException
from modules.auth.repositories.user_repository import Login
from infra.database.repositories.user_repository import User
from infra.providers import token_provider
from schemas.schemas import LoginData, SuccessLogin
import httpx

router = APIRouter()

@router.post('/login', response_model=SuccessLogin)
async def login(login_data: LoginData):
    
    url = "http://127.0.0.1:3000/auth/login"
    
    payload = {
        "username": login_data.username,
        "password": login_data.password
    }
    
    async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload)
            response_data = response.json()
    
    return {
                    "access_token": response_data['access_token'],
                    "token_type": response_data['token_type']
                }
