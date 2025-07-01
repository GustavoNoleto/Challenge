import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from utils.tasks_scheduler import TaskScheduler
from routers import dimensoes_routers
from routers import assertividade_routers
from routers import lucratividade_routers
from routers import produtividade_eficiencia_routers
from routers import overview_score_routers
from routers import auth_router
from modules.lucratividade_score.services.lucratividade_service import CustosService
from fastapi.responses import JSONResponse
from utils.logger import info_logger
from middlewares.auth_middleware import JWTMiddleware


@asynccontextmanager
async def lifespan(app):
    info_logger.info("Server startup!")
    try:
        async with asyncio.timeout(30):
            await TaskScheduler.scheduler()            
        yield        
    finally:
        async with asyncio.timeout(30):
            TaskScheduler.scheduler().close()
            info_logger.info("Server shutdown!")


app = FastAPI(lifespan=lifespan,
    title="overview_consultores_oficina")

#app.add_middleware(JWTMiddleware)

# CORS
origins = ['http://localhost:8080',
           'https://localhost:3000']

app.add_middleware(CORSMiddleware,
                   allow_origins = [origins],
                   allow_credentials = True,
                   allow_methods = ["*"],
                   allow_headers = ["*"])


# Auth router
app.include_router(auth_router.router, prefix='/auth')

# Tabelas dimensoes router
app.include_router(dimensoes_routers.router, prefix='/dimensoes')

# Assertividade Score router
app.include_router(assertividade_routers.router, prefix='/assertividade')

# Lucratividade Score router
app.include_router(lucratividade_routers.router, prefix='/lucratividade')

# Desempenho Score router
app.include_router(produtividade_eficiencia_routers.router, prefix='/desempenho')

# Overview Score Router
app.include_router(overview_score_routers.router, prefix='/consultor_score')


@app.get('/testes')
def testes():
    testes = CustosService.get_ticket_medio()
        
    testes = list(testes.iter_rows(named=True))
    response = JSONResponse(content=testes)
    
    return response


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host='0.0.0.0', port=8800, reload=False)