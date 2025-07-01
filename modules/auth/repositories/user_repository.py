from fastapi import HTTPException
import os
import httpx
from schemas.schemas import LoginData
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
AUTHORITY_URL = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPE = os.getenv("SCOPE")

class Login:
    """
    Classe responsável pela autenticação de usuários.

    Esta classe contém métodos estáticos para realizar login de usuários
    utilizando as credenciais fornecidas e o Active Directory.

    Attributes:
        CLIENT_ID (str): O ID do cliente obtido do ambiente.
        CLIENT_SECRET (str): O segredo do cliente obtido do ambiente.
        TENANT_ID (str): O ID do locatário obtido do ambiente.
        AUTHORITY_URL (str): A URL de autenticação do Active Directory.
        SCOPE (str): O escopo de permissões obtido do ambiente.
    """

    @staticmethod
    async def user_login(login_data: LoginData):
        """
        Realiza o login do usuário utilizando as credenciais fornecidas.

        Este método autentica o usuário junto ao Active Directory usando
        o fluxo de password, enviando o username e password recebidos.

        Args:
            login_data (LoginData): Um objeto contendo os dados de login
                                    (username e password).

        Returns:
            dict: Um dicionário contendo o token de acesso e o tipo do token
                  se a autenticação for bem-sucedida.

        Raises:
            HTTPException: Se ocorrer um erro HTTP durante a autenticação
                           ou se as credenciais forem inválidas.
        """
        try:
            # Autenticação com o Active Directory usando o token de client_credentials
            async with httpx.AsyncClient() as client:
                response = await client.post(AUTHORITY_URL, data={
                    'client_id': CLIENT_ID,
                    'client_secret': CLIENT_SECRET,
                    'scope': SCOPE,
                    'grant_type': 'password',
                    'username': login_data.username,  # O username recebido no payload
                    'password': login_data.password   # O password recebido no payload
                })
                
                # Verifique se a resposta foi bem-sucedida
                if response.status_code == 200:
                    # Parse da resposta JSON para extrair o token
                    token_data = response.json()
                    return {"access_token": token_data['access_token'], "token_type": "bearer"}
                else:
                    print(response.json())
                    raise HTTPException(status_code=401, detail="Credenciais inválidas, contate o NTI via chamado em https://lavronorte.milldesk.com/")

        except httpx.HTTPError as http_err:
            raise HTTPException(status_code=500, detail=f"HTTP error occurred: {http_err}")
