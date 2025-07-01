from modules.tabelas_dimensoes.services.consultores_service import DimensoesDataFrames
from modules.assertividade_score.services.retrabalho_service import RetrabalhoService
from modules.assertividade_score.services.devolucoes_oficina_service import DevolucoesOficina 
from modules.assertividade_score.services.analise_relatorios_service import AnaliseRelatorio
import polars as pl

class AssertividadeScore():
    def get_assertividade_detalhado():
        retrabalho_consultores_score = RetrabalhoService.get_retrabalho_score()
        devolucoes_consultores_score = DevolucoesOficina.get_devolucoes_oficina_score()
        analise_relatorio_consultores_score = AnaliseRelatorio.get_analise_relatorio_score()
        
        # Importando os dataframes
        consultores_df = DimensoesDataFrames.consultores_df
        retrabalho_consultores_score = retrabalho_consultores_score
        devolucoes_consultores_score = devolucoes_consultores_score
        analise_relatorio_consultores_score = analise_relatorio_consultores_score
        
        # Realizando o join entre os dataframes dos scores com o dataframe dimensão de consultores
        assertividade_score = consultores_df.join(retrabalho_consultores_score, on="CODPRO", how="left")
        assertividade_score = assertividade_score.join(devolucoes_consultores_score, on="CODPRO", how="left")
        assertividade_score = assertividade_score.join(analise_relatorio_consultores_score, on="CODPRO", how="left")
        
        # Calculando o score da competência Assertividade
        assertividade_score = assertividade_score.select(
            pl.col("*"),
            ((pl.col("EFICACIA_SCORE") * 0.4) + (pl.col("DEVOLUCOES_SCORE") * 0.3) + (pl.col("QUALIDADE_RELATORIO_SCORE") * 0.3))
            .clip(0, 1000)
            .round(2)
            .alias("ASSERTIVIDADE_SCORE")
        )
        
        assertividade_detalhado = assertividade_score.select("CODPRO", "EFICACIA_SCORE", "DEVOLUCOES_SCORE",
                                                             "QUALIDADE_RELATORIO_SCORE", "ASSERTIVIDADE_SCORE")
        
        return assertividade_detalhado
    
    def get_assertividade_detalhado_mom():
        retrabalho_consultores_score_mom = RetrabalhoService.get_retrabalho_score_mom()
        devolucoes_consultores_score_mom = DevolucoesOficina.get_devolucoes_oficina_score_mom()
        analise_relatorio_consultores_score_mom = AnaliseRelatorio.get_analise_relatorio_score_mom()
        
        # Importando os dataframes
        consultores_df = DimensoesDataFrames.consultores_df
        retrabalho_consultores_score_mom = retrabalho_consultores_score_mom
        devolucoes_consultores_score_mom = devolucoes_consultores_score_mom
        analise_relatorio_consultores_score_mom = analise_relatorio_consultores_score_mom
        

        # Realizando o join entre os dataframes dos scores com o dataframe dimensão de consultores
        assertividade_score_mom = consultores_df.join(analise_relatorio_consultores_score_mom, on=["CODPRO"], how="left")
        assertividade_score_mom = assertividade_score_mom.join(devolucoes_consultores_score_mom, on=["CODPRO","DATA"], how="left")
        assertividade_score_mom = assertividade_score_mom.join(retrabalho_consultores_score_mom, on=["CODPRO","DATA"], how="left")

        # Calculando o score da competência Assertividade
        assertividade_score_mom = assertividade_score_mom.select(
            pl.col("*"),
            ((pl.col("EFICACIA_SCORE") * 0.4) + (pl.col("DEVOLUCOES_SCORE") * 0.3) + (pl.col("QUALIDADE_RELATORIO_SCORE") * 0.3))
            .clip(0, 1000)
            .round(2)
            .alias("ASSERTIVIDADE_SCORE")
        )
        
        assertividade_detalhado_mom = assertividade_score_mom.select("CODPRO","DATA","EFICACIA_SCORE", "DEVOLUCOES_SCORE",
                                                             "QUALIDADE_RELATORIO_SCORE", "ASSERTIVIDADE_SCORE")
        
        return assertividade_detalhado_mom
    
    def get_assertividade_atributos():
        assertividade_atributos = AssertividadeScore.get_assertividade_detalhado()
        
        assertividade_atributos = assertividade_atributos.select(
            pl.col("*"),
            (pl.col("EFICACIA_SCORE") * 0.4)
            .round(2)
            .alias("Eficacia"))
        
        assertividade_atributos = assertividade_atributos.select(
            pl.col("*"),
            (pl.col("DEVOLUCOES_SCORE") * 0.3)
            .round(2)
            .alias("Requisições assertivas"))
        
        assertividade_atributos = assertividade_atributos.select(
            pl.col("*"),
            (pl.col("QUALIDADE_RELATORIO_SCORE") * 0.3)
            .round(2)
            .alias("Qualidade do relatório"))
        
        # Dataframe com os scores
        assertividade_atributos = assertividade_atributos.select("CODPRO", "Eficacia", "Requisições assertivas", "Qualidade do relatório")        
        assertividade_atributos = assertividade_atributos.melt(id_vars=["CODPRO"], variable_name="ATTRIBUTE", value_name="SCORE")

        return assertividade_atributos
    
    def get_assertividade_atributos_separados():
        assertividade_atributos = AssertividadeScore.get_assertividade_detalhado()
        
        assertividade_atributos = assertividade_atributos.select(
            pl.col("*"),
            (pl.col("EFICACIA_SCORE") * 0.4)
            .round(2)
            .alias("Eficacia"))
        
        assertividade_atributos = assertividade_atributos.select(
            pl.col("*"),
            (pl.col("DEVOLUCOES_SCORE") * 0.3)
            .round(2)
            .alias("Requisições assertivas"))
        
        assertividade_atributos = assertividade_atributos.select(
            pl.col("*"),
            (pl.col("QUALIDADE_RELATORIO_SCORE") * 0.3)
            .round(2)
            .alias("Qualidade do relatório"))
        
        # Dataframe com os scores
        assertividade_atributos_separados = assertividade_atributos.select("CODPRO", "Eficacia", "Requisições assertivas", "Qualidade do relatório")
        
        return assertividade_atributos_separados
    
    def get_assertividade_score():
        assertividade_score = AssertividadeScore.get_assertividade_detalhado()
        
        assertividade_score = assertividade_score.select("CODPRO", "ASSERTIVIDADE_SCORE")
                
        return assertividade_score
    
    def get_assertividade_score_mean():
        assertividade_score = AssertividadeScore.get_assertividade_score()
        assertividade_score = assertividade_score
        
        assertividade_score_mean = assertividade_score.group_by("ATTRIBUTE").agg(pl.mean("SCORE").alias("SCORE_MEAN"))
        
        return assertividade_score_mean
