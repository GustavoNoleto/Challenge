from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from modules.tabelas_dimensoes.services.consultores_service import DimensaoConsultores
from modules.tabelas_dimensoes.services.filiais_service import DimensaoFiliais
from datetime import datetime
import polars as pl

router = APIRouter()

@router.get('/consultores')
async def get_consultores():
    consultores_df = DimensaoConsultores.get_dimensao_consultores()    
        
    consultores_df = list(consultores_df.iter_rows(named=True))
    response = JSONResponse(content=consultores_df)
    
    return response

@router.get('/time_now')
async def get_time_now(request: Request):    
    time_now = datetime.now()
    data = time_now.strftime("%d/%m/%Y")
    hora = time_now.strftime("%H:%M:%S")
    time_dict = {"Data": data, "Hora": hora}
    time_dict = pl.DataFrame(time_dict)
    
    time_dict = list(time_dict.iter_rows(named=True))
    response = JSONResponse(content=time_dict)
    return response

@router.get('/filiais')
async def get_filiais():
    filiais_df = DimensaoFiliais.get_dimensao_filiais()    
    
    filiais_df = list(filiais_df.iter_rows(named=True))
    response = JSONResponse(content=filiais_df)
    
    return response