from pydantic import BaseModel, constr
from datetime import date

class LoginData(BaseModel):
    username: str
    password: str
    
    class config:
        orm_mode = True

class SuccessLogin(BaseModel):    
    access_token: str
    token_type: str
    
    class config:
        orm_mode = True