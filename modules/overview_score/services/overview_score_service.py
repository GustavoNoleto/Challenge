from modules.assertividade_score.services.assertividade_score_service import AssertividadeScore
from modules.lucratividade_score.services.lucratividade_service import CustosService
from modules.produtividade_eficiencia_score.services.produtividade_eficiencia_service import ProdutividadeEficienciaScore
from modules.tabelas_dimensoes.services.consultores_service import DimensaoConsultores
import polars as pl

class OverviewScore():
    def get_overview_detalhado():
        consultores_df = DimensaoConsultores.get_dimensao_consultores()
        assertividade_score = AssertividadeScore.get_assertividade_score()
        lucratividade_score = CustosService.get_lucratividade_score_12m()
        desempenho_score = ProdutividadeEficienciaScore.get_produtividade_eficiencia_score_12m()
        
        assertividade_score = assertividade_score.with_columns(pl.col("ASSERTIVIDADE_SCORE").fill_null(pl.lit(0)))
        assertividade_score = assertividade_score.with_columns(            
            (pl.col("ASSERTIVIDADE_SCORE") * 0.3)
            .round(2)
            .alias("Assertividade"))
        
        lucratividade_score = lucratividade_score.with_columns(pl.col("LUCRATIVIDADE_SCORE").fill_null(pl.lit(0)))
        lucratividade_score = lucratividade_score.with_columns(
            (pl.col("LUCRATIVIDADE_SCORE") * 0.3)
            .round(2)
            .alias("Lucratividade"))
        
        desempenho_score = desempenho_score.with_columns(pl.col("DESEMPENHO_SCORE").fill_null(pl.lit(0)))
        desempenho_score = desempenho_score.with_columns(
            (pl.col("DESEMPENHO_SCORE") * 0.4)
            .round(2)
            .alias("Desempenho"))
        
        consultores_df = consultores_df.join(assertividade_score, on="CODPRO", how="left")
        consultores_df = consultores_df.join(lucratividade_score, on="CODPRO", how="left")
        consultores_df = consultores_df.join(desempenho_score, on="CODPRO", how="left")
        
        overview_detalhado = consultores_df.select("CODPRO", "Assertividade",
                                               "Lucratividade", "Desempenho")
        
        return overview_detalhado
    
    def get_overview_detalhado_mom():
        consultores_df = DimensaoConsultores.get_dimensao_consultores()
        assertividade_score = AssertividadeScore.get_assertividade_score()
        lucratividade_score = CustosService.get_lucratividade_score_mom()
        desempenho_score = ProdutividadeEficienciaScore.get_produtividade_eficiencia_score_mom()
        
        assertividade_score = assertividade_score.with_columns(pl.col("ASSERTIVIDADE_SCORE").fill_null(pl.lit(0)))
        assertividade_score = assertividade_score.with_columns(            
            (pl.col("ASSERTIVIDADE_SCORE") * 0.3)
            .round(2)
            .alias("Assertividade"))
        
        lucratividade_score = lucratividade_score.with_columns(pl.col(["LUCRATIVIDADE_SCORE","DATA"]).fill_null(pl.lit(0)))
        lucratividade_score = lucratividade_score.with_columns(
            (pl.col("LUCRATIVIDADE_SCORE") * 0.3)
            .round(2)
            .alias("Lucratividade"))
        
        desempenho_score = desempenho_score.with_columns(pl.col(["DESEMPENHO_SCORE","DATA"]).fill_null(pl.lit(0)))
        desempenho_score = desempenho_score.with_columns(
            (pl.col("DESEMPENHO_SCORE") * 0.4)
            .round(2)
            .alias("Desempenho"))
        
        consultores_df = consultores_df.join(assertividade_score, on="CODPRO", how="left")
        consultores_df = consultores_df.join(lucratividade_score, on="CODPRO", how="left")
        consultores_df = consultores_df.join(desempenho_score, on=["CODPRO", "DATA"], how="left")
        
        overview_detalhado = consultores_df.select("CODPRO","DATA","Assertividade",
                                               "Lucratividade", "Desempenho")
        
        return overview_detalhado
    
    def get_overview_score():
        overview_detalhado = OverviewScore.get_overview_detalhado()
        
        overview_score = overview_detalhado.select(
            pl.col("*"),
            (pl.col("Assertividade") + pl.col("Lucratividade") + pl.col("Desempenho"))
            .round(2)
            .alias("CONSULTOR_SCORE")
        )
        
        overview_score = overview_score.select("CODPRO", "CONSULTOR_SCORE")
        
        return overview_score
    
    def get_overview_score_mom():
        overview_detalhado = OverviewScore.get_overview_detalhado_mom()
        
        overview_score = overview_detalhado.select(
            pl.col("*"),
            (pl.col("Assertividade") + pl.col("Lucratividade") + pl.col("Desempenho"))
            .round(2)
            .alias("CONSULTOR_SCORE")
        )
        
        overview_score = overview_score.select("CODPRO", "DATA", "CONSULTOR_SCORE")
        
        return overview_score
    
    def get_overview_atributos():
        overview_detalhado = OverviewScore.get_overview_detalhado()
        
        overview_atributos = overview_detalhado.melt(id_vars=["CODPRO"], variable_name="ATTRIBUTE", value_name="SCORE")
        
        return overview_atributos
    
    def get_overview_atributos_mom():
        overview_detalhado = OverviewScore.get_overview_detalhado_mom()
        
        overview_atributos = overview_detalhado.melt(id_vars=["CODPRO"], variable_name="ATTRIBUTE", value_name="SCORE")
        
        return overview_atributos