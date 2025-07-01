from modules.tabelas_dimensoes.services.consultores_service import DimensoesDataFrames
from modules.assertividade_score.repositories.assertividade_repository import AssertividadeDataFrame
import polars as pl

class RetrabalhoService():
    def get_retrabalho_detalhado():
        # Importando os dataframes
        retrabalho_df = AssertividadeDataFrame.retrabalho_df        
        consultores_df = DimensoesDataFrames.consultores_df
        servicos_faturados_df = AssertividadeDataFrame.servicos_faturados_df

        # Criando a coluna VALID_RETRABALHO
        retrabalho_df = retrabalho_df.select(
            pl.col("*"),        
            pl.when((pl.col("STATUS") == "F") & ((pl.col("RETRABALHO") == "SIM") | (pl.col("DEPT_INTERNO") == "R")))
            .then(pl.lit("SIM"))
            .otherwise(pl.lit("NAO"))
            .alias("VALID_RETRABALHO")
        )
        
        # Criando a coluna OS_PROD_RET_CONCAT
        retrabalho_df = retrabalho_df.select(
            pl.col("*"),        
            pl.when((pl.col("VALID_RETRABALHO") == "SIM") & (pl.col("STATUS") == "F"))
            .then(pl.col("OS_CORRECAO") + pl.col("PRODUTIVO") + pl.col("OS_ORIGEM"))
            .otherwise(pl.lit(None))
            .alias("OS_PROD_RET_CONCAT")
        )
        
        # Contagem de ordem de serviço
        quantidade_ordem_servico = servicos_faturados_df.group_by("CODPRO").agg(pl.n_unique("NroOS").alias("QTD_OS"))
        consultores_df = consultores_df.join(quantidade_ordem_servico, on="CODPRO", how="left")
        
        # Contagem de retrabalho por consultor
        quantidade_retrabalho = retrabalho_df.group_by("CODPRO").agg(pl.n_unique("OS_PROD_RET_CONCAT").alias("QTD_RETRABALHO"))
        consultores_df = consultores_df.join(quantidade_retrabalho, on="CODPRO", how="left")
        
        consultores_df = consultores_df.with_columns(pl.col("QTD_RETRABALHO").fill_null(pl.lit(0)))
        
        # Calculando a porcentagem de retrabalho para cada consultor
        consultores_df = consultores_df.select(
            pl.col("*"),
            ((pl.col("QTD_RETRABALHO") / pl.col("QTD_OS")) * 100)
            .round(2)
            .alias("RETRABALHO_PERCENT")
        )
        
        retrabalho_detalhado = consultores_df.select("CODPRO","QTD_OS","QTD_RETRABALHO", "RETRABALHO_PERCENT")
        
        return retrabalho_detalhado
    
    def get_retrabalho_detalhado_mom():
        # Importando os dataframes
        retrabalho_df = AssertividadeDataFrame.retrabalho_df        
        consultores_df = DimensoesDataFrames.consultores_df
        servicos_faturados_df = AssertividadeDataFrame.servicos_faturados_df

        # Criando a coluna VALID_RETRABALHO
        retrabalho_df = retrabalho_df.select(
            pl.col("*"),        
            pl.when((pl.col("STATUS") == "F") & ((pl.col("RETRABALHO") == "SIM") | (pl.col("DEPT_INTERNO") == "R")))
            .then(pl.lit("SIM"))
            .otherwise(pl.lit("NAO"))
            .alias("VALID_RETRABALHO")
        )
        
        # Criando a coluna OS_PROD_RET_CONCAT
        retrabalho_df = retrabalho_df.select(
            pl.col("*"),        
            pl.when((pl.col("VALID_RETRABALHO") == "SIM") & (pl.col("STATUS") == "F"))
            .then(pl.col("OS_CORRECAO") + pl.col("PRODUTIVO") + pl.col("OS_ORIGEM"))
            .otherwise(pl.lit(None))
            .alias("OS_PROD_RET_CONCAT")
        )
        
          # Converte a coluna DATA para tipo Date
        retrabalho_df =  retrabalho_df.with_columns([
            pl.col("DATA").str.to_datetime("%Y-%m-%d").cast(pl.Date).alias("DATA")
        ])

        # Cria coluna DATA no formato "YYYY-MM"
        retrabalho_df  =  retrabalho_df .with_columns([
            pl.col("DATA").dt.strftime("%Y-%m").alias("DATA")
        ])


        # Contagem de ordem de serviço
        quantidade_ordem_servico = servicos_faturados_df.group_by("CODPRO").agg(pl.n_unique("NroOS").alias("QTD_OS"))
        consultores_df = consultores_df.join(quantidade_ordem_servico, on="CODPRO", how="left")
        
        # Contagem de retrabalho por consultor
        quantidade_retrabalho = retrabalho_df.group_by("CODPRO","DATA").agg(pl.n_unique("OS_PROD_RET_CONCAT").alias("QTD_RETRABALHO"))
        consultores_df = consultores_df.join(quantidade_retrabalho, on="CODPRO", how="left")
        
        consultores_df = consultores_df.with_columns(pl.col("QTD_RETRABALHO","DATA").fill_null(pl.lit(0)))
        
        # Calculando a porcentagem de retrabalho para cada consultor
        consultores_df = consultores_df.select(
            pl.col("*"),
            ((pl.col("QTD_RETRABALHO") / pl.col("QTD_OS")) * 100)
            .round(2)
            .alias("RETRABALHO_PERCENT")
        )
        
        retrabalho_detalhado = consultores_df.select("CODPRO","QTD_OS","DATA","QTD_RETRABALHO", "RETRABALHO_PERCENT")
        
        return retrabalho_detalhado
        
    def get_retrabalho_score():
        retrabalho_score = RetrabalhoService.get_retrabalho_detalhado()
        
        # Calculando o score
        retrabalho_score = retrabalho_score.select(
            pl.col("*"),
            ((1 - ((pl.col("RETRABALHO_PERCENT") * 5) / 100)) * 1000).clip(0, 1000).round(2).alias("EFICACIA_SCORE")
        )
        retrabalho_score = retrabalho_score.with_columns(pl.col("EFICACIA_SCORE").fill_null(pl.lit(0)))
         
        # Dataframe com o score
        retrabalho_consultores_score = retrabalho_score.select("CODPRO", "EFICACIA_SCORE")
        
        return retrabalho_consultores_score
    
    def get_retrabalho_score_mom():
        retrabalho_score_mom = RetrabalhoService.get_retrabalho_detalhado_mom()
        
        # Calculando o score
        retrabalho_score_mom = retrabalho_score_mom.select(
            pl.col("*"),
            ((1 - ((pl.col("RETRABALHO_PERCENT") * 5) / 100)) * 1000).clip(0, 1000).round(2).alias("EFICACIA_SCORE")
        )
        retrabalho_score_mom = retrabalho_score_mom.with_columns(pl.col("EFICACIA_SCORE").fill_null(pl.lit(0)))
         
        # Dataframe com o score
        retrabalho_consultores_score_mom = retrabalho_score_mom.select("CODPRO", "DATA", "EFICACIA_SCORE")
        
        return retrabalho_consultores_score_mom

    def get_ordens_servico_retrabalho():        
        # Importando os dataframes
        retrabalho_df = AssertividadeDataFrame.retrabalho_df
                  
        # Criando a coluna VALID_RETRABALHO
        retrabalho_df = retrabalho_df.select(
            pl.col("*"),        
            pl.when((pl.col("STATUS") == "F") & ((pl.col("RETRABALHO") == "SIM") | (pl.col("DEPT_INTERNO") == "R")))
            .then(pl.lit("SIM"))
            .otherwise(pl.lit("NAO"))
            .alias("VALID_RETRABALHO")
        ) 
        
        # Dataframe de retrabalho com filtro de retrabalho SIM
        os_retrabalho = retrabalho_df.filter(pl.col("VALID_RETRABALHO") == "SIM")
        os_retrabalho = os_retrabalho.select(pl.col("OS_CORRECAO", "PRODUTIVO", "OS_ORIGEM", "CODPRO"))
        
        return os_retrabalho