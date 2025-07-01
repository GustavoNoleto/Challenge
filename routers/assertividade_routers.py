from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from modules.assertividade_score.services.assertividade_score_service import AssertividadeScore
from modules.assertividade_score.repositories.assertividade_repository import AssertividadeDataFrame
from modules.assertividade_score.services.retrabalho_service import RetrabalhoService
from modules.assertividade_score.services.devolucoes_oficina_service import DevolucoesOficina
from modules.assertividade_score.services.analise_relatorios_service import AnaliseRelatorio 

router = APIRouter()

@router.get('/assertividade_detalhado')
async def get_assertividade_detalhado():
    assertividade_detalhado = AssertividadeScore.get_assertividade_detalhado()    
    
    assertividade_detalhado = list(assertividade_detalhado.iter_rows(named=True))
    response = JSONResponse(content=assertividade_detalhado)
    
    return response

@router.get('/assertividade_detalhado_mom')
async def get_assertividade_detalhado_mom():
    assertividade_detalhado_mom = AssertividadeScore.get_assertividade_detalhado_mom()    
    
    assertividade_detalhado_mom = list(assertividade_detalhado_mom.iter_rows(named=True))
    response = JSONResponse(content=assertividade_detalhado_mom)
    
    return response

@router.get('/assertividade_atributos')
async def get_assertividade_atributos():
    assertividade_atributos = AssertividadeScore.get_assertividade_atributos()
    
    assertividade_atributos = list(assertividade_atributos.iter_rows(named=True))
    response = JSONResponse(content=assertividade_atributos)
    
    return response

@router.get('/assertividade_atributos_separados')
async def get_assertividade_atributos_separados():
    assertividade_atributos_separados = AssertividadeScore.get_assertividade_atributos_separados()
    
    assertividade_atributos_separados = list(assertividade_atributos_separados.iter_rows(named=True))
    response = JSONResponse(content=assertividade_atributos_separados)
    
    return response

@router.get('/assertividade_score')
async def get_assertividade_score():
    assertividade_score = AssertividadeScore.get_assertividade_score()

    assertividade_score = list(assertividade_score.iter_rows(named=True))
    response = JSONResponse(content=assertividade_score)
    
    return response
        
@router.get('/assertividade_score_mean')
async def get_assertividade_score_mean():
    assertividade_score_mean = AssertividadeScore.get_assertividade_score_mean()    
    
    assertividade_score_mean = list(assertividade_score_mean.iter_rows(named=True))
    response = JSONResponse(content=assertividade_score_mean)
    
    return response

@router.get('/retrabalho_detalhado')
async def get_retrabalho_detalhado():
    retrabalho_detalhado = RetrabalhoService.get_retrabalho_detalhado()
    
    retrabalho_detalhado = list(retrabalho_detalhado.iter_rows(named=True))
    response = JSONResponse(content=retrabalho_detalhado)
    
    return response

@router.get('/retrabalho_detalhado_mom')
async def get_retrabalho_detalhado_mom():
    get_retrabalho_detalhado_mom = RetrabalhoService.get_retrabalho_detalhado_mom()
    
    get_retrabalho_detalhado_mom = list(get_retrabalho_detalhado_mom.iter_rows(named=True))
    response = JSONResponse(content=get_retrabalho_detalhado_mom)
    
    return response

@router.get('/retrabalho_score_mom')
async def get_retrabalho_score_mom():
    get_retrabalho_score_mom = RetrabalhoService.get_retrabalho_score_mom()
    
    get_retrabalho_score_mom = list(get_retrabalho_score_mom.iter_rows(named=True))
    response = JSONResponse(content=get_retrabalho_score_mom)
    return response

@router.get('/os_retrabalho')
async def get_os_retrabalho():
    os_retrabalho = RetrabalhoService.get_ordens_servico_retrabalho()    
    
    os_retrabalho = list(os_retrabalho.iter_rows(named=True))
    response = JSONResponse(content=os_retrabalho)
    
    return response

@router.get('/devolucoes_consultores')
async def get_devolucoes_consultores():
    devolucoes_consultores = DevolucoesOficina.get_devolucoes_oficina()    
    
    devolucoes_consultores = list(devolucoes_consultores.iter_rows(named=True))
    response = JSONResponse(content=devolucoes_consultores)
    
    return response

@router.get('/devolucoes_consultores_mom')
async def get_devolucoes_consultores_mom():
    devolucoes_consultores_mom = DevolucoesOficina.get_devolucoes_oficina_mom()    
    
    devolucoes_consultores_mom = list(devolucoes_consultores_mom.iter_rows(named=True))
    response = JSONResponse(content=devolucoes_consultores_mom)
    
    return response

@router.get('/devolucoes_score_mom')
async def get_devolucoes_oficina_score_mom():
    devolucoes_score_mom = DevolucoesOficina.get_devolucoes_oficina_score_mom()   

    devolucoes_score_mom = list(devolucoes_score_mom.iter_rows(named=True))
    response = JSONResponse(content=devolucoes_score_mom)
    
    return response

@router.get('/analise_relatorio_detalhado')
async def get_analise_relatorio_detalhado():
    analise_relatorio_detalhado = AnaliseRelatorio.get_analise_relatorio_detalhado()
    
    analise_relatorio_detalhado = list(analise_relatorio_detalhado.iter_rows(named=True))
    response = JSONResponse(content=analise_relatorio_detalhado)
    
    return response

@router.get('/analise_relatorio_detalhado_mom')
async def get_analise_relatorio_detalhado_mom():
    analise_relatorio_detalhado_mom = AnaliseRelatorio.get_analise_relatorio_detalhado_mom()
    
    analise_relatorio_detalhado_mom = list(analise_relatorio_detalhado_mom.iter_rows(named=True))
    response = JSONResponse(content=analise_relatorio_detalhado_mom)
    
    return response

@router.get('/analise_relatorio_score_mom')
async def get_analise_relatorio_score_mom():
    analise_relatorio_score_mom = AnaliseRelatorio.get_analise_relatorio_score_mom()
    
    analise_relatorio_score_mom = list(analise_relatorio_score_mom.iter_rows(named=True))
    response = JSONResponse(content=analise_relatorio_score_mom)
    
    return response


@router.get('/os_analise_relatorio')
async def get_os_analise_relatorio():
    os_analise_relatorio = AnaliseRelatorio.get_os_analise_relatorio()
    
    os_analise_relatorio = list(os_analise_relatorio.iter_rows(named=True))
    response = JSONResponse(content=os_analise_relatorio)
    
    return response

@router.get('/questoes_analise_relatorios')
async def get_questoes_analise_relatorios():
    questoes_analise = AnaliseRelatorio.get_questoes_analise_relatorio()    
        
    questoes_analise = list(questoes_analise.iter_rows(named=True))
    response = JSONResponse(content=questoes_analise)
    
    return response

@router.get('/servicos_faturados')
async def get_servicos_faturados():
    servicos_faturados = AssertividadeDataFrame.servicos_faturados_df
            
    servicos_faturados = list(servicos_faturados.iter_rows(named=True))
    response = JSONResponse(content=servicos_faturados)
    
    return response