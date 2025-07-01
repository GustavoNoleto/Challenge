from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from modules.overview_score.services.overview_score_service import OverviewScore

router = APIRouter()

@router.get('/overview_detalhado')
def get_overview_detalhado():
    overview_detalhado = OverviewScore.get_overview_detalhado()
    
    overview_detalhado = list(overview_detalhado.iter_rows(named=True))
    response = JSONResponse(content=overview_detalhado)
    
    return response

@router.get('/overview_detalhado_mom')
def get_overview_detalhado_mom():
    overview_detalhado = OverviewScore.get_overview_detalhado_mom()
    
    overview_detalhado = list(overview_detalhado.iter_rows(named=True))
    response = JSONResponse(content=overview_detalhado)
    
    return response

@router.get('/overview_score')
def get_overview_score():
    overview_score = OverviewScore.get_overview_score()
    
    overview_score = list(overview_score.iter_rows(named=True))
    response = JSONResponse(content=overview_score)
    
    return response

@router.get('/overview_score_mom')
def get_overview_score_mom():
    overview_score = OverviewScore.get_overview_score_mom()
    
    overview_score = list(overview_score.iter_rows(named=True))
    response = JSONResponse(content=overview_score)
    
    return response

@router.get('/overview_atributos')
def get_overview_atributos():
    overview_atributos = OverviewScore.get_overview_atributos()
    
    overview_atributos = list(overview_atributos.iter_rows(named=True))
    response = JSONResponse(content=overview_atributos)
    
    return response
    