from datetime import datetime, timedelta
from jose import jwt
import dotenv
import os


dotenv.load_dotenv(dotenv.find_dotenv())
SECRET_KEY = os.getenv('token_secret_key')
ALGORITHM = 'HS256'
EXPIRES_IN_MIN = 30

def create_access_token(data: dict):
    dados = data.copy()
    expires = datetime.utcnow() + timedelta(minutes= EXPIRES_IN_MIN)
    
    dados.update({'exp': expires})
    
    jwt_token = jwt.encode(dados, SECRET_KEY, algorithm=ALGORITHM)
    return jwt_token

def access_token_verifier(token: str):
    payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    return payload.get('sub')