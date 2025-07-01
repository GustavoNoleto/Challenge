from modules.tabelas_dimensoes.services.consultores_service import DimensoesDataFrames
from modules.assertividade_score.repositories.assertividade_repository import AssertividadeDataFrame
import polars as pl

class AnaliseRelatorio():
    def get_analise_relatorio_detalhado():       
        # Importando os dataframes
        consultores_df = DimensoesDataFrames.consultores_df        
        analise_relatorio_df = AssertividadeDataFrame.analise_relatorio_df
                
        # Contagem de ordem de serviço
        quantidade_ordem_servico = analise_relatorio_df.group_by("CODPRO").agg(pl.n_unique("OS").alias("QTD_OS"))
        consultores_df = consultores_df.join(quantidade_ordem_servico, on="CODPRO", how="left")
                
        # Contagem da quantidade de relatórios bem preparados
        analise_relatorio_df = analise_relatorio_df.filter(pl.col("AVALIACAO_RELATORIO") == "Bem Preparado")
        quantidade_bem_preparado = analise_relatorio_df.group_by("CODPRO").agg(pl.count("AVALIACAO_RELATORIO").alias("QTD_BEM_PREPARADO"))
        consultores_df = consultores_df.join(quantidade_bem_preparado, on="CODPRO", how="left")
        consultores_df = consultores_df.with_columns(pl.col("QTD_BEM_PREPARADO").fill_null(pl.lit(0)))
        
        # Calculando a porcentagem de relatórios bem preparados por consultor
        consultores_df = consultores_df.select(
            pl.col("*"),
            ((pl.col("QTD_BEM_PREPARADO") / pl.col("QTD_OS")) * 100)
            .round(2)
            .alias("BEM_PREPARADO_PERCENT")
        )
        
        analise_relatorio_detalhado = consultores_df.select("CODPRO", "QTD_OS", "QTD_BEM_PREPARADO", "BEM_PREPARADO_PERCENT")
        
        return analise_relatorio_detalhado
    
    def get_analise_relatorio_detalhado_mom():
        consultores_df = DimensoesDataFrames.consultores_df
        analise_relatorio_df = AssertividadeDataFrame.analise_relatorio_df

        # Converte a coluna DATA para tipo Date
        analise_relatorio_df = analise_relatorio_df.with_columns([
            pl.col("DATA").str.to_datetime("%Y-%m-%d").cast(pl.Date).alias("DATA")
        ])

        # Cria coluna DATA no formato "YYYY-MM"
        analise_relatorio_df = analise_relatorio_df.with_columns([
            pl.col("DATA").dt.strftime("%Y-%m").alias("DATA")
        ])

        # Contagem de OS por consultor por mês
        quantidade_os_df = (
            analise_relatorio_df
            .group_by(["CODPRO", "DATA"])
            .agg(pl.n_unique("OS").alias("QTD_OS"))
        )

        # Contagem de relatórios "Bem Preparado" por consultor por mês
        bem_preparado_df = (
            analise_relatorio_df
            .filter(pl.col("AVALIACAO_RELATORIO") == "Bem Preparado")
            .group_by(["CODPRO", "DATA"])
            .agg(pl.count().alias("QTD_BEM_PREPARADO"))
        )

        # Junta os dois resultados
        resultado_df = quantidade_os_df.join(
            bem_preparado_df, on=["CODPRO", "DATA"], how="left"
        ).with_columns([
            pl.col("QTD_BEM_PREPARADO").fill_null(0)
        ])

        # Calcula a porcentagem
        resultado_df = resultado_df.with_columns([
            pl.when(pl.col("QTD_OS") > 0)
            .then((pl.col("QTD_BEM_PREPARADO") / pl.col("QTD_OS") * 100).round(2))
            .otherwise(0)
            .alias("BEM_PREPARADO_PERCENT")
        ])

        # Junta com nome do consultor, se desejar
        resultado_df = resultado_df.join(
            consultores_df.select(["CODPRO"]),
            on="CODPRO", how="left"
        )

        # Seleciona e ordena as colunas finais
        resultado_df = resultado_df.select([
            "CODPRO","DATA",
            "QTD_OS", "QTD_BEM_PREPARADO", "BEM_PREPARADO_PERCENT"
        ]).sort(["DATA", "CODPRO"])

        return resultado_df

        
    def get_analise_relatorio_score():
        analise_relatorio_df = AnaliseRelatorio.get_analise_relatorio_detalhado()
        
        # Calculando o score
        analise_relatorio_consultores_score = analise_relatorio_df.select(
            pl.col("*"),
            ((pl.col("BEM_PREPARADO_PERCENT") / 100) * 1000).clip(0, 1000).round(2).alias("QUALIDADE_RELATORIO_SCORE")
        )
        
        # Dataframe com o Score
        analise_relatorio_consultores_score = analise_relatorio_consultores_score.select("CODPRO", "QUALIDADE_RELATORIO_SCORE")
        
        return analise_relatorio_consultores_score
    
    def get_analise_relatorio_score_mom():
        analise_relatorio_df = AnaliseRelatorio.get_analise_relatorio_detalhado_mom()
        
        # Calculando o score
        analise_relatorio_consultores_score_mom = analise_relatorio_df.select(
            pl.col("*"),
            ((pl.col("BEM_PREPARADO_PERCENT") / 100) * 1000).clip(0, 1000).round(2).alias("QUALIDADE_RELATORIO_SCORE")
        )
    
        # Dataframe com o Score
        analise_relatorio_consultores_score_mom = analise_relatorio_consultores_score_mom.select("CODPRO","DATA","QUALIDADE_RELATORIO_SCORE")
        
        return analise_relatorio_consultores_score_mom
    
    def get_questoes_analise_relatorio():       
        questoes_analise_relatorio_df = AssertividadeDataFrame.questoes_analise_relatorio_df
        
        return questoes_analise_relatorio_df
    
    def get_os_analise_relatorio():                      
        analise_relatorio_df = AssertividadeDataFrame.analise_relatorio_df
        
        analise_relatorio_df = analise_relatorio_df.with_columns(pl.col("DATA").cast(pl.String))
        
        return analise_relatorio_df