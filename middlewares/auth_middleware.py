from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request, HTTPException
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from dotenv import load_dotenv
import httpx
import os

load_dotenv()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

CLIENT_ID = os.getenv("CLIENT_ID")
TENANT_ID = os.getenv("TENANT_ID")
AUTHORITY_URL = f"https://sts.windows.net/{TENANT_ID}/"
JWKS_URL = f"https://login.microsoftonline.com/{TENANT_ID}/discovery/v2.0/keys"
ALGORITHM = "RS256"

class JWTMiddleware(BaseHTTPMiddleware):
    """
    Middleware for validating JWT tokens in requests.

    This middleware checks for the presence of a JWT token in the 
    Authorization header of incoming requests and validates it against 
    the public keys provided by Azure AD. If the token is valid, 
    it extracts the payload and attaches it to the request state.

    Attributes:
        oauth2_scheme: OAuth2PasswordBearer instance to extract the token.
    """

    async def dispatch(self, request: Request, call_next):
        """
        Dispatches the request to the next middleware or endpoint.

        This method checks for a JWT token in the Authorization header,
        validates it, and attaches the decoded payload to the request state.

        Args:
            request (Request): The incoming request object.
            call_next (Callable): A callable that receives the request 
                                  as an argument and returns the response.

        Raises:
            HTTPException: If the Authorization header is missing, if the 
                           token verification fails, or if an error occurs 
                           while fetching the JWKS.
        """
        if request.url.path == "/auth/login":
            return await call_next(request)

        auth: str = request.headers.get("Authorization")
        if not auth:
            raise HTTPException(status_code=401, detail="Authorization header missing")

        token = auth.split(" ")[1]

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(JWKS_URL)
                response.raise_for_status()
                jwks = response.json()

            unverified_header = jwt.get_unverified_header(token)
            rsa_key = {}
            for key in jwks["keys"]:
                if key["kid"] == unverified_header["kid"]:
                    rsa_key = {
                        "kty": key["kty"],
                        "kid": key["kid"],
                        "use": key["use"],
                        "n": key["n"],
                        "e": key["e"]
                    }
            if not rsa_key:
                raise HTTPException(status_code=401, detail="Token has invalid signature")
            
            payload = jwt.decode(
                token,
                rsa_key,
                algorithms=[ALGORITHM],
                audience=CLIENT_ID,
                issuer=AUTHORITY_URL,
            )
            
            request.state.user = payload

        except JWTError as e:            
            raise HTTPException(status_code=401, detail="Token verification failed")
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=500, detail=f"Failed to fetch JWKS: {str(e)} - {e.response.text}")
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=f"HTTP error occurred: {str(e)}")

        response = await call_next(request)
        return response
