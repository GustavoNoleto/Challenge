from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from modules.lucratividade_score.services.lucratividade_service import CustosService

router = APIRouter()

@router.get('/lucratividade_score')
async def get_lucratividade_score():
    lucratividade_score_12M = CustosService.get_lucratividade_score_12m()    
    
    lucratividade_score_12M = list(lucratividade_score_12M.iter_rows(named=True))
    response = JSONResponse(content=lucratividade_score_12M)
    
    return response

@router.get('/lucratividade_detalhado')
async def get_lucratividade_detalhado():
    lucratividade_detalhado = CustosService.get_custos_service()    
    
    lucratividade_detalhado = list(lucratividade_detalhado.iter_rows(named=True))
    response = JSONResponse(content=lucratividade_detalhado)
    
    return response

@router.get('/lucratividade_score_mom')
async def get_lucratividade_score_mom():
    lucratividade_score_mom = CustosService.get_lucratividade_score_mom()
    
    lucratividade_score_mom = list(lucratividade_score_mom.iter_rows(named=True))
    response = JSONResponse(content=lucratividade_score_mom)
    
    return response

@router.get('/lucratividade_historico')
async def get_lucratividade_historico():
    lucratividade_historico = CustosService.get_fator_lucratividade_historico()
    
    lucratividade_historico = list(lucratividade_historico.iter_rows(named=True))
    response = JSONResponse(content=lucratividade_historico)
    
    return response

@router.get('/ticket_medio_servicos')
async def get_ticket_medio_servicos():
    ticket_medio_servicos = CustosService.get_ticket_medio()
    
    ticket_medio_servicos = list(ticket_medio_servicos.iter_rows(named=True))
    response = JSONResponse(content=ticket_medio_servicos)
    
    return response

@router.get('/tipo_de_maquina')
async def get_tipo_de_maquina():
    tipo_de_maquina = CustosService.get_tipo_de_maquina()
    
    tipo_de_maquina = list(tipo_de_maquina.iter_rows(named=True))
    response = JSONResponse(content=tipo_de_maquina)
    
    return response