from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from modules.produtividade_eficiencia_score.services.produtividade_eficiencia_service import ProdutividadeEficienciaScore
from utils.preprocessing_data import PreprocessingData

router = APIRouter()

@router.get('/desempenho_detalhado')
def get_desempenho_detalhado():
    desempenho_detalhado = ProdutividadeEficienciaScore.get_produtividade_eficiencia_detalhado()  
    
    desempenho_detalhado = list(desempenho_detalhado.iter_rows(named=True))
    response = JSONResponse(content=desempenho_detalhado)
    
    return response

@router.get('/desempenho_score_mom')
def get_desempenho_score_mom():
    desempenho_score_mom = ProdutividadeEficienciaScore.get_produtividade_eficiencia_score_mom()  
    
    desempenho_score_mom = list(desempenho_score_mom.iter_rows(named=True))
    sanitized_data = PreprocessingData.sanitize_data(desempenho_score_mom)
    
    response = JSONResponse(content=sanitized_data)
    
    return response

@router.get('/desempenho_score_12m')
def get_desempenho_score_12m():
    desempenho_score_12m = ProdutividadeEficienciaScore.get_produtividade_eficiencia_score_12m()
    
    desempenho_score_12m = list(desempenho_score_12m.iter_rows(named=True))
    response = JSONResponse(content=desempenho_score_12m)
    
    return response