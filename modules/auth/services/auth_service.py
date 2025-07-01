from fastapi import HTTPException
from modules.auth.repositories.user_repository import User
from schemas.schemas import LoginData, SuccessLogin
from infra.providers import token_provider

class Login():
    def user_login(login_data: LoginData):
        user = login_data.user
        password = login_data.password
        
        if user != User.user:
        
            raise HTTPException(status_code = 400, detail= "Usuário ou senha incorretos!")
    
        if password != User.password:
            
            raise HTTPException(status_code = 400, detail= "Usuário ou senha incorretos!")
        
        token = token_provider.create_access_token({'sub': User.user})
        return SuccessLogin(user=user, access_token=token)