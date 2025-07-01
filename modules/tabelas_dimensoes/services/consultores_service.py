from modules.tabelas_dimensoes.repositories.dimensoes_repository import DimensoesDataFrames
import polars as pl

class DimensaoConsultores():
    def get_dimensao_consultores():                
        consultores_df = DimensoesDataFrames.consultores_df
        consultores_rh_df = DimensoesDataFrames.consultores_rh_df   
        filiais_df = DimensoesDataFrames.filiais_df

        valor_hora_consultores_df = DimensoesDataFrames.valor_hora_consultores_df
        
        valor_hora_consultores_df = valor_hora_consultores_df.select("CPF", "CARGO")

        consultores_rh_df = consultores_rh_df.select("CPF", "DATA_ADMISSAO", "DATA_DEMISSAO")  
        
        valor_hora_consultores_df = valor_hora_consultores_df.unique(subset="CPF")

        consultores_rh_df = consultores_rh_df.unique(subset="CPF")

        consultores_df = consultores_df.join(valor_hora_consultores_df, on="CPF", how="left")
        
        consultores_df = consultores_df.join(filiais_df, on="FILIAL", how="left")

        consultores_df = consultores_df.join(consultores_rh_df, on="CPF", how="left")
        
        return consultores_df