from modules.tabelas_dimensoes.services.consultores_service import DimensaoConsultores
from modules.produtividade_eficiencia_score.repositories.produtividade_eficiencia_repository import ProdutividadeEficienciaDataframe
import polars as pl
from datetime import datetime, timedelta

class ProdutividadeEficienciaScore():
    def get_produtividade_eficiencia_detalhado():
        consultores_df = DimensaoConsultores.get_dimensao_consultores()
        produtividade_eficiencia_df = ProdutividadeEficienciaDataframe.produtividade_eficiencia_df
        
        consultores_df = consultores_df.select("FILIAL", "CODPRO", "NOM_TEC", "CPF")
        
        produtividade_eficiencia_df = produtividade_eficiencia_df.with_columns(pl.col("DATA").str.to_datetime("%Y%m%d").cast(pl.Date).alias("DATA"))               
                
        produtividade_eficiencia_df = produtividade_eficiencia_df.with_columns(
            (pl.col("DATA").dt.year().cast(pl.Utf8) + '-' +
            pl.col("DATA").dt.month().cast(pl.Utf8).str.zfill(2) + '-01')
            .str.strptime(pl.Date, "%Y-%m-%d")
            .alias("DATA")
        )
        
        data_inicio = datetime.now() - timedelta(days= 30 * 13)
        
        produtividade_eficiencia_df = produtividade_eficiencia_df.filter(pl.col("DATA") >= pl.lit(data_inicio))
        
        produtividade_eficiencia_df = (produtividade_eficiencia_df.group_by("CODPRO", pl.col("DATA").dt.day())
                                       .agg(pl.sum("HRDISP", "TEMTRAB", "TEMCOB")))
        consultores_df = consultores_df.join(produtividade_eficiencia_df, on="CODPRO", how="left")
        
        consultores_df = consultores_df.with_columns(
            pl.col("TEMCOB").fill_null(pl.lit(0)),
            pl.col("TEMTRAB").fill_null(pl.lit(0)),
            pl.col("HRDISP").fill_null(pl.lit(0))
        )
        
        consultores_df = consultores_df.select(
            pl.col("*"),
            (((pl.col("TEMTRAB") / pl.col("HRDISP")) * 100)
             .round(2).alias("PRODUTIVIDADE"))
        )
        
        consultores_df = consultores_df.select(
            pl.col("*"),
            ((pl.col("TEMCOB") / pl.col("TEMTRAB")) *100)
            .round(2).alias("EFICIENCIA")
        )
        
        consultores_df = consultores_df.select(
            pl.col("*"),
            ((pl.col("PRODUTIVIDADE") * pl.col("EFICIENCIA")) / 100)
            .round(2).alias("DESEMPENHO")
        )
        
        produtividade_eficiencia_detalhado = consultores_df.with_columns(pl.col("DATA").cast(pl.String))
        produtividade_eficiencia_detalhado = produtividade_eficiencia_detalhado.with_columns(            
            pl.col("PRODUTIVIDADE").fill_nan(pl.lit(0)),
            pl.col("EFICIENCIA").fill_nan(pl.lit(0)),
            pl.col("DESEMPENHO").fill_nan(pl.lit(0))
        )       

        produtividade_eficiencia_detalhado = produtividade_eficiencia_detalhado.select("CODPRO", "HRDISP", "TEMTRAB", "TEMCOB",
                                                                                       "PRODUTIVIDADE", "EFICIENCIA", "DESEMPENHO")
        
        return produtividade_eficiencia_detalhado
    
    def get_produtividade_eficiencia_score_12m():
        produtividade_eficiencia_df = ProdutividadeEficienciaScore.get_produtividade_eficiencia_detalhado()
        
        produtividade_eficiencia_score = produtividade_eficiencia_df.select(
            pl.col("*"),
            ((pl.col("DESEMPENHO") - 15)/(52 - 15) * 1000)
            .clip(0, 1000)
            .round(2)
            .alias("DESEMPENHO_SCORE")
        )
        
        produtividade_eficiencia_score = produtividade_eficiencia_score.select("CODPRO", "DESEMPENHO_SCORE")
                
        return produtividade_eficiencia_score
                
    def get_produtividade_eficiencia_score_mom():
        consultores_df = DimensaoConsultores.get_dimensao_consultores()
        produtividade_eficiencia_df = ProdutividadeEficienciaDataframe.produtividade_eficiencia_df
        
        consultores_df = consultores_df.select("FILIAL", "CODPRO", "NOM_TEC", "CPF")
        
        produtividade_eficiencia_df = produtividade_eficiencia_df.with_columns(pl.col("DATA").str.to_datetime("%Y%m%d").cast(pl.Date).alias("DATA"))
        
        produtividade_eficiencia_df = produtividade_eficiencia_df.with_columns(
            (pl.col("DATA").dt.year().cast(pl.Utf8) + '-' +
            pl.col("DATA").dt.month().cast(pl.Utf8).str.zfill(2) + '-01')
            .str.strptime(pl.Date, "%Y-%m-%d")
            .alias("DATA")
        )        
        
        produtividade_eficiencia_df = (produtividade_eficiencia_df.group_by("CODPRO", "DATA")
                                       .agg(pl.sum("HRDISP", "TEMTRAB", "TEMCOB")))
        consultores_df = consultores_df.join(produtividade_eficiencia_df, on="CODPRO", how="left")              
                
        consultores_df = consultores_df.with_columns(
            pl.col("TEMCOB").fill_null(pl.lit(0)),
            pl.col("TEMTRAB").fill_null(pl.lit(0)),
            pl.col("HRDISP").fill_null(pl.lit(0))
        )
        
        consultores_df = consultores_df.select(
            pl.col("*"),
            (((pl.col("TEMTRAB") / pl.col("HRDISP")) * 100)
             .round(2).alias("PRODUTIVIDADE"))
        )
        
        consultores_df = consultores_df.select(
            pl.col("*"),
            ((pl.col("TEMCOB") / pl.col("TEMTRAB")) *100)
            .round(2).alias("EFICIENCIA")
        )
        
        consultores_df = consultores_df.select(
            pl.col("*"),
            ((pl.col("PRODUTIVIDADE") * pl.col("EFICIENCIA")))
            .round(2).alias("DESEMPENHO")
        )
        
        produtividade_eficiencia_score = consultores_df.with_columns(pl.col("DATA").cast(pl.String))
        produtividade_eficiencia_score = produtividade_eficiencia_score.with_columns(            
            pl.col("PRODUTIVIDADE").fill_nan(pl.lit(0)),
            pl.col("EFICIENCIA").fill_nan(pl.lit(0)),
            pl.col("DESEMPENHO").fill_nan(pl.lit(0))
        )
        
        produtividade_eficiencia_score = produtividade_eficiencia_score.select(
            pl.col("*"),
            ((pl.col("DESEMPENHO") - 15)/(52 - 15) * 1000)
            .clip(0, 1000)
            .round(2)
            .alias("DESEMPENHO_SCORE")
        )
        
        produtividade_eficiencia_score = produtividade_eficiencia_score.select("CODPRO", "DATA", "HRDISP", "TEMTRAB", "TEMCOB",
                                                                                       "PRODUTIVIDADE", "EFICIENCIA", "DESEMPENHO", "DESEMPENHO_SCORE")
        
        return produtividade_eficiencia_score