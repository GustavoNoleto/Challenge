from modules.tabelas_dimensoes.services.consultores_service import DimensoesDataFrames
from modules.assertividade_score.repositories.assertividade_repository import AssertividadeDataFrame
import polars as pl

class DevolucoesOficina():
    def get_devolucoes_oficina():
        # Importando os dataframes
        consultores_df = DimensoesDataFrames.consultores_df
        devolucoes_oficina_df = AssertividadeDataFrame.devolucoes_oficina_df
        
        # Contagem de devoluções de popularidade 0 por consultor
        devolucoes_pop0_menor3k = devolucoes_oficina_df.filter((pl.col("POPULARIDADE") == 0) & (pl.col("VALOR") < 3000))
        devolucoes_pop0_menor3k = devolucoes_pop0_menor3k.group_by("CODPRO").agg(pl.count("POPULARIDADE").alias("QTD_POP0_<3K"))
        consultores_df = consultores_df.join(devolucoes_pop0_menor3k, on="CODPRO", how="left")
        consultores_df = consultores_df.with_columns(pl.col("QTD_POP0_<3K").fill_null(pl.lit(0)))
        
        devolucoes_pop0_maior3k = devolucoes_oficina_df.filter((pl.col("POPULARIDADE") == 0) & (pl.col("VALOR") > 3000))
        devolucoes_pop0_maior3k = devolucoes_pop0_maior3k.group_by("CODPRO").agg(pl.count("POPULARIDADE").alias("QTD_POP0_>3K"))
        consultores_df = consultores_df.join(devolucoes_pop0_maior3k, on="CODPRO", how="left")
        consultores_df = consultores_df.with_columns(pl.col("QTD_POP0_>3K").fill_null(pl.lit(0)))
        
        # Contagem de devoluções de popularidade 1 por consultor
        devolucoes_pop1_menor3k = devolucoes_oficina_df.filter((pl.col("POPULARIDADE") == 1) & (pl.col("VALOR") < 3000))
        devolucoes_pop1_menor3k = devolucoes_pop1_menor3k.group_by("CODPRO").agg(pl.count("POPULARIDADE").alias("QTD_POP1_<3K"))    
        consultores_df = consultores_df.join(devolucoes_pop1_menor3k, on="CODPRO", how="left")
        consultores_df = consultores_df.with_columns(pl.col("QTD_POP1_<3K").fill_null(pl.lit(0)))
        
        devolucoes_pop1_maior3k = devolucoes_oficina_df.filter((pl.col("POPULARIDADE") == 1) & (pl.col("VALOR") > 3000))
        devolucoes_pop1_maior3k = devolucoes_pop1_maior3k.group_by("CODPRO").agg(pl.count("POPULARIDADE").alias("QTD_POP1_>3K"))    
        consultores_df = consultores_df.join(devolucoes_pop1_maior3k, on="CODPRO", how="left")
        consultores_df = consultores_df.with_columns(pl.col("QTD_POP1_>3K").fill_null(pl.lit(0)))
        
        # Contagem de devoluções de popularidade 2 por consultor
        devolucoes_pop2_menor3k = devolucoes_oficina_df.filter((pl.col("POPULARIDADE") == 2) & (pl.col("VALOR") < 3000))
        devolucoes_pop2_menor3k = devolucoes_pop2_menor3k.group_by("CODPRO").agg(pl.count("POPULARIDADE").alias("QTD_POP2_<3K"))    
        consultores_df = consultores_df.join(devolucoes_pop2_menor3k, on="CODPRO", how="left")
        consultores_df = consultores_df.with_columns(pl.col("QTD_POP2_<3K").fill_null(pl.lit(0)))
        
        devolucoes_pop2_maior3k = devolucoes_oficina_df.filter((pl.col("POPULARIDADE") == 2) & (pl.col("VALOR") > 3000))
        devolucoes_pop2_maior3k = devolucoes_pop2_maior3k.group_by("CODPRO").agg(pl.count("POPULARIDADE").alias("QTD_POP2_>3K"))    
        consultores_df = consultores_df.join(devolucoes_pop2_maior3k, on="CODPRO", how="left")
        consultores_df = consultores_df.with_columns(pl.col("QTD_POP2_>3K").fill_null(pl.lit(0)))
        
        # Contagem de devoluções de popularidade 3 por consultor
        devolucoes_pop3_menor3k = devolucoes_oficina_df.filter((pl.col("POPULARIDADE") == 3) & (pl.col("VALOR") < 3000))
        devolucoes_pop3_menor3k = devolucoes_pop3_menor3k.group_by("CODPRO").agg(pl.count("POPULARIDADE").alias("QTD_POP3_<3K"))    
        consultores_df = consultores_df.join(devolucoes_pop3_menor3k, on="CODPRO", how="left")
        consultores_df = consultores_df.with_columns(pl.col("QTD_POP3_<3K").fill_null(pl.lit(0)))
        
        devolucoes_pop3_maior3k = devolucoes_oficina_df.filter((pl.col("POPULARIDADE") == 3) & (pl.col("VALOR") > 3000))
        devolucoes_pop3_maior3k = devolucoes_pop3_maior3k.group_by("CODPRO").agg(pl.count("POPULARIDADE").alias("QTD_POP3_>3K"))    
        consultores_df = consultores_df.join(devolucoes_pop3_maior3k, on="CODPRO", how="left")
        consultores_df = consultores_df.with_columns(pl.col("QTD_POP3_>3K").fill_null(pl.lit(0)))
        
        # Contagem de devoluções de popularidade maior que 3 por consultor
        devolucoes_pop_maior3_menor3k = devolucoes_oficina_df.filter((pl.col("POPULARIDADE") > 3) & (pl.col("VALOR") < 3000))
        devolucoes_pop_maior3_menor3k = devolucoes_pop_maior3_menor3k.group_by("CODPRO").agg(pl.count("POPULARIDADE").alias("QTD_POP3+_<3K"))    
        consultores_df = consultores_df.join(devolucoes_pop_maior3_menor3k, on="CODPRO", how="left")
        consultores_df = consultores_df.with_columns(pl.col("QTD_POP3+_<3K").fill_null(pl.lit(0)))
        
        devolucoes_pop_maior3_maior3k = devolucoes_oficina_df.filter((pl.col("POPULARIDADE") > 3) & (pl.col("VALOR") > 3000))
        devolucoes_pop_maior3_maior3k = devolucoes_pop_maior3_maior3k.group_by("CODPRO").agg(pl.count("POPULARIDADE").alias("QTD_POP3+_>3K"))    
        consultores_df = consultores_df.join(devolucoes_pop_maior3_maior3k, on="CODPRO", how="left")
        consultores_df = consultores_df.with_columns(pl.col("QTD_POP3+_>3K").fill_null(pl.lit(0)))
            
        devolucoes_consultores_df = consultores_df.select("CODPRO",
                                                        pl.col("QTD_POP0_<3K", "QTD_POP0_>3K",
                                                        "QTD_POP1_<3K", "QTD_POP1_>3K",
                                                        "QTD_POP2_<3K", "QTD_POP2_>3K",
                                                        "QTD_POP3_<3K", "QTD_POP3_>3K",
                                                        "QTD_POP3+_<3K", "QTD_POP3+_>3K").cast(pl.Float64)
                                                        )
        return devolucoes_consultores_df
    
    def get_devolucoes_oficina_mom():
        # Importando os dataframes
        consultores_df = DimensoesDataFrames.consultores_df
        devolucoes_oficina_df = AssertividadeDataFrame.devolucoes_oficina_df

        # Converte a coluna DATA para tipo Date
        devolucoes_oficina_df = devolucoes_oficina_df.with_columns([
            pl.col("DATA").str.to_datetime("%Y-%m-%d").cast(pl.Date).alias("DATA")
        ])

        # Cria coluna DATA no formato "YYYY-MM"
        devolucoes_oficina_df  = devolucoes_oficina_df .with_columns([
            pl.col("DATA").dt.strftime("%Y-%m").alias("DATA")
        ])

        # Base com todos os pares únicos de consultores e meses
        codpro_data_df = devolucoes_oficina_df.select(["CODPRO", "DATA"]).unique()
        consultores_df = consultores_df.join(codpro_data_df, on="CODPRO", how="inner")

        # POPULARIDADE 0
        devolucoes_pop0_menor3k = devolucoes_oficina_df.filter(
            (pl.col("POPULARIDADE") == 0) & (pl.col("VALOR") < 3000)
        ).group_by("CODPRO", "DATA").agg(pl.count("POPULARIDADE").alias("QTD_POP0_<3K"))
        consultores_df = consultores_df.join(devolucoes_pop0_menor3k, on=["CODPRO", "DATA"], how="left").with_columns(
            pl.col("QTD_POP0_<3K").fill_null(pl.lit(0))
        )

        devolucoes_pop0_maior3k = devolucoes_oficina_df.filter(
            (pl.col("POPULARIDADE") == 0) & (pl.col("VALOR") > 3000)
        ).group_by("CODPRO", "DATA").agg(pl.count("POPULARIDADE").alias("QTD_POP0_>3K"))
        consultores_df = consultores_df.join(devolucoes_pop0_maior3k, on=["CODPRO", "DATA"], how="left").with_columns(
            pl.col("QTD_POP0_>3K").fill_null(pl.lit(0))
        )

        # POPULARIDADE 1
        devolucoes_pop1_menor3k = devolucoes_oficina_df.filter(
            (pl.col("POPULARIDADE") == 1) & (pl.col("VALOR") < 3000)
        ).group_by("CODPRO", "DATA").agg(pl.count("POPULARIDADE").alias("QTD_POP1_<3K"))
        consultores_df = consultores_df.join(devolucoes_pop1_menor3k, on=["CODPRO", "DATA"], how="left").with_columns(
            pl.col("QTD_POP1_<3K").fill_null(pl.lit(0))
        )

        devolucoes_pop1_maior3k = devolucoes_oficina_df.filter(
            (pl.col("POPULARIDADE") == 1) & (pl.col("VALOR") > 3000)
        ).group_by("CODPRO", "DATA").agg(pl.count("POPULARIDADE").alias("QTD_POP1_>3K"))
        consultores_df = consultores_df.join(devolucoes_pop1_maior3k, on=["CODPRO", "DATA"], how="left").with_columns(
            pl.col("QTD_POP1_>3K").fill_null(pl.lit(0))
        )

        # POPULARIDADE 2
        devolucoes_pop2_menor3k = devolucoes_oficina_df.filter(
            (pl.col("POPULARIDADE") == 2) & (pl.col("VALOR") < 3000)
        ).group_by("CODPRO", "DATA").agg(pl.count("POPULARIDADE").alias("QTD_POP2_<3K"))
        consultores_df = consultores_df.join(devolucoes_pop2_menor3k, on=["CODPRO", "DATA"], how="left").with_columns(
            pl.col("QTD_POP2_<3K").fill_null(pl.lit(0))
        )

        devolucoes_pop2_maior3k = devolucoes_oficina_df.filter(
            (pl.col("POPULARIDADE") == 2) & (pl.col("VALOR") > 3000)
        ).group_by("CODPRO", "DATA").agg(pl.count("POPULARIDADE").alias("QTD_POP2_>3K"))
        consultores_df = consultores_df.join(devolucoes_pop2_maior3k, on=["CODPRO", "DATA"], how="left").with_columns(
            pl.col("QTD_POP2_>3K").fill_null(pl.lit(0))
        )

        # POPULARIDADE 3
        devolucoes_pop3_menor3k = devolucoes_oficina_df.filter(
            (pl.col("POPULARIDADE") == 3) & (pl.col("VALOR") < 3000)
        ).group_by("CODPRO", "DATA").agg(pl.count("POPULARIDADE").alias("QTD_POP3_<3K"))
        consultores_df = consultores_df.join(devolucoes_pop3_menor3k, on=["CODPRO", "DATA"], how="left").with_columns(
            pl.col("QTD_POP3_<3K").fill_null(pl.lit(0))
        )

        devolucoes_pop3_maior3k = devolucoes_oficina_df.filter(
            (pl.col("POPULARIDADE") == 3) & (pl.col("VALOR") > 3000)
        ).group_by("CODPRO", "DATA").agg(pl.count("POPULARIDADE").alias("QTD_POP3_>3K"))
        consultores_df = consultores_df.join(devolucoes_pop3_maior3k, on=["CODPRO", "DATA"], how="left").with_columns(
            pl.col("QTD_POP3_>3K").fill_null(pl.lit(0))
        )

        # POPULARIDADE > 3
        devolucoes_pop_maior3_menor3k = devolucoes_oficina_df.filter(
            (pl.col("POPULARIDADE") > 3) & (pl.col("VALOR") < 3000)
        ).group_by("CODPRO", "DATA").agg(pl.count("POPULARIDADE").alias("QTD_POP3+_<3K"))
        consultores_df = consultores_df.join(devolucoes_pop_maior3_menor3k, on=["CODPRO", "DATA"], how="left").with_columns(
            pl.col("QTD_POP3+_<3K").fill_null(pl.lit(0))
        )

        devolucoes_pop_maior3_maior3k = devolucoes_oficina_df.filter(
            (pl.col("POPULARIDADE") > 3) & (pl.col("VALOR") > 3000)
        ).group_by("CODPRO", "DATA").agg(pl.count("POPULARIDADE").alias("QTD_POP3+_>3K"))
        consultores_df = consultores_df.join(devolucoes_pop_maior3_maior3k, on=["CODPRO", "DATA"], how="left").with_columns(
            pl.col("QTD_POP3+_>3K").fill_null(pl.lit(0))
        )

        # Seleção final
        devolucoes_consultores_df = consultores_df.select(
            "CODPRO", "DATA",
            pl.col(
                "QTD_POP0_<3K", "QTD_POP0_>3K",
                "QTD_POP1_<3K", "QTD_POP1_>3K",
                "QTD_POP2_<3K", "QTD_POP2_>3K",
                "QTD_POP3_<3K", "QTD_POP3_>3K",
                "QTD_POP3+_<3K", "QTD_POP3+_>3K"
            ).cast(pl.Float64)
        )

        return devolucoes_consultores_df


    
    def get_devolucoes_oficina_score():
        # Importando os dataframes
        devolucoes_consultores_df = DevolucoesOficina.get_devolucoes_oficina()
        devolucoes_consultores_df = devolucoes_consultores_df
        
        # Calculando o Score
        devolucoes_consultores_score = devolucoes_consultores_df.select(
            pl.col("*"),
            (
                (
                    1-(
                        (
                            (                            
                                (pl.col("QTD_POP0_<3K") * 15 + pl.col("QTD_POP0_>3K") * 30 
                                + pl.col("QTD_POP1_<3K") * 5 + pl.col("QTD_POP1_>3K") * 10
                                + pl.col("QTD_POP2_<3K") * 5 + pl.col("QTD_POP2_>3K") * 5
                                + pl.col("QTD_POP3_<3K") * 5 + pl.col("QTD_POP3_>3K") * 5
                                + pl.col("QTD_POP3+_<3K") * 15 + pl.col("QTD_POP3+_>3K") * 5)
                                - 0
                            ) / 100 - 0
                        ) / 100 - 0
                    )
                ) * 1000
            ).clip(0, 1000).round(2).alias("DEVOLUCOES_SCORE")
        )
        
        # Dataframe com o score
        devolucoes_consultores_score = devolucoes_consultores_score.select("CODPRO", "DEVOLUCOES_SCORE")
        
        return devolucoes_consultores_score
    
     
    def get_devolucoes_oficina_score_mom():
        # Importando os dataframes
        devolucoes_consultores_df = DevolucoesOficina.get_devolucoes_oficina_mom()
        devolucoes_consultores_df = devolucoes_consultores_df
        
        # Calculando o Score
        devolucoes_consultores_score_mom = devolucoes_consultores_df.select(
            pl.col("*"),
            (
                (
                    1-(
                        (
                            (                            
                                (pl.col("QTD_POP0_<3K") * 15 + pl.col("QTD_POP0_>3K") * 30 
                                + pl.col("QTD_POP1_<3K") * 5 + pl.col("QTD_POP1_>3K") * 10
                                + pl.col("QTD_POP2_<3K") * 5 + pl.col("QTD_POP2_>3K") * 5
                                + pl.col("QTD_POP3_<3K") * 5 + pl.col("QTD_POP3_>3K") * 5
                                + pl.col("QTD_POP3+_<3K") * 15 + pl.col("QTD_POP3+_>3K") * 5)
                                - 0
                            ) / 100 - 0
                        ) / 100 - 0
                    )
                ) * 1000
            ).clip(0, 1000).round(2).alias("DEVOLUCOES_SCORE")
        )
        
        # Dataframe com o score
        devolucoes_consultores_score_mom = devolucoes_consultores_score_mom.select("CODPRO","DATA","DEVOLUCOES_SCORE")
        
        return devolucoes_consultores_score_mom