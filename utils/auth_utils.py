# from infra.providers import token_provider
# from fastapi.security import OAuth2PasswordBearer
# from fastapi import Depends, HTTPException
# from jose import JWTError


# class AuthUtils():
    
#     oauth2_schema = OAuth2PasswordBearer(tokenUrl='token')
    
#     def authorization(token: str = Depends(oauth2_schema)):
        
#         exception = HTTPException(status_code=401, detail="Not Authenticated!")
        
#         try:
#             user = token_provider.access_token_verifier(token)
#         except JWTError:            
#             raise exception
        
#         if not user:
#             raise exception