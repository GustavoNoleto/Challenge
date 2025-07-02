from modules.tabelas_dimensoes.services.consultores_service import DimensoesDataFrames
from modules.lucratividade_score.repositories.lucratividade_repository import LucratividadeDataFrame
import polars as pl
from datetime import datetime, timedelta

class CustosService():
    def replace_day_with_first(df, date_column="DATA"):
        
        df = df.with_columns(
            pl.col(date_column).str.strptime(pl.Date, strict=False)
        )
        
        df = df.with_columns(
            (pl.col(date_column).dt.year().cast(pl.Utf8) + '-' +
            pl.col(date_column).dt.month().cast(pl.Utf8).str.zfill(2) + '-01')
            .str.strptime(pl.Date, "%Y-%m-%d")
            .alias(date_column)
        )
        
        df = df.with_columns(
            pl.col(date_column).dt.strftime("%d/%m/%Y").alias(date_column)
        )            
        return df
    
    def get_custos_service():
        # Importando os dataframes
        consultores_df = DimensoesDataFrames.consultores_df
        epi_consultores_df = LucratividadeDataFrame.epi_consultores_df
        valor_hora_consultores_df = LucratividadeDataFrame.valor_hora_consultores_df
        valor_comissoes_consultores_df = LucratividadeDataFrame.valor_comissoes_consultores_df
        custos_consultores_df = LucratividadeDataFrame.custos_consultores_df
        servicos_faturados_df = LucratividadeDataFrame.servicos_faturados_df
        
        consultores_df = consultores_df.select("FILIAL", "CODPRO", "NOM_TEC", "CPF")        
        custos_consultores_df = custos_consultores_df.select("CODPRO", "VALORBRUTO", "DATAEMISSAO")
        servicos_faturados_df = servicos_faturados_df.select("CODPRO", "ValorServico", "DataVenda")
        servicos_faturados_df = servicos_faturados_df.with_columns(pl.col("DataVenda").alias("DATA"))
        epi_consultores_df = epi_consultores_df.select("CPF", "VALOR", "DATA_ENTREGA")
        epi_consultores_df = epi_consultores_df.with_columns(pl.col("DATA_ENTREGA").alias("DATA"))
        valor_hora_consultores_df = valor_hora_consultores_df.select("CPF", "CARGO", "VALOR_HORA", "DATA")
        valor_comissoes_consultores_df = valor_comissoes_consultores_df.select("CPF", "VALOR", "DATA_REFERENCIA")
        valor_comissoes_consultores_df = valor_comissoes_consultores_df.with_columns(pl.col("DATA_REFERENCIA").alias("DATA"))
        custos_consultores_df = custos_consultores_df.with_columns(pl.col("DATAEMISSAO").alias("DATA"))              
           
        valor_hora_consultores_df = CustosService.replace_day_with_first(valor_hora_consultores_df)        
        servicos_faturados_df = CustosService.replace_day_with_first(servicos_faturados_df)
        epi_consultores_df = CustosService.replace_day_with_first(epi_consultores_df)
        valor_comissoes_consultores_df = CustosService.replace_day_with_first(valor_comissoes_consultores_df)
        custos_consultores_df = CustosService.replace_day_with_first(custos_consultores_df)                  
              
        consultores_df = consultores_df.join(valor_hora_consultores_df, on="CPF", how="left")
        
        consultores_df = consultores_df.with_columns(
            (pl.col("VALOR_HORA") * 176).round(2).alias("CUSTOS_ENCARGOS_MENSAL"))
        
        valor_comissoes_consultores_df = valor_comissoes_consultores_df.group_by("CPF", "VALOR", "DATA").agg(
                                                                    pl.sum("VALOR").round(2).alias("VALOR_COMISSOES"))
        
        consultores_df = consultores_df.join(valor_comissoes_consultores_df, on=["CPF", "DATA"], how="left")
        
        custos_consultores_df = (custos_consultores_df.group_by("CODPRO", "DATA")
                                 .agg(pl.sum("VALORBRUTO").round(2).alias("CUSTO_VIAGENS")))
                                                              
        consultores_df = consultores_df.join(custos_consultores_df, on=["CODPRO", "DATA"], how="left")               
                        
        epi_consultores_df = (epi_consultores_df.group_by("CPF", "DATA")
                              .agg(pl.sum("VALOR").round(2).alias("CUSTOS_EPI")))
        
        consultores_df = consultores_df.join(epi_consultores_df, on=["CPF", "DATA"], how="left")
    
        consultores_df = consultores_df.with_columns(
            pl.col("CUSTO_VIAGENS").fill_null(pl.lit(0)),
            pl.col("CUSTOS_EPI").fill_null(pl.lit(0)),
            pl.col("CUSTOS_ENCARGOS_MENSAL").fill_null(pl.lit(0)),
            pl.col("VALOR_COMISSOES").fill_null(pl.lit(0))
        )
                
        consultores_df = consultores_df.with_columns(
            (pl.col("CUSTOS_ENCARGOS_MENSAL") + pl.col("VALOR_COMISSOES"))
            .round(2).alias("CUSTO_CONSULTOR")
        )
        
        servicos_faturados_df = (servicos_faturados_df.group_by("CODPRO", "DATA")
                                 .agg(pl.sum("ValorServico").round(2).alias("FATURAMENTO")))
                
        consultores_df = consultores_df.join(servicos_faturados_df, on=["CODPRO", "DATA"], how="left")
        consultores_df = consultores_df.with_columns(pl.col("FATURAMENTO").fill_null(pl.lit(0)))                             
                
        lucratividade_df = consultores_df.select("FILIAL", "CODPRO", "DATA",
                                                 "CUSTOS_ENCARGOS_MENSAL", "VALOR_COMISSOES",
                                                 "CUSTO_CONSULTOR", "FATURAMENTO")
        
        return lucratividade_df
    
    def get_lucratividade_score_12m():
        lucratividade_df = CustosService.get_custos_service()
        
        lucratividade_df = lucratividade_df.with_columns(pl.col("DATA").str.to_datetime("%d/%m/%Y").cast(pl.Date).alias("DATA"))
        
        data_inicio = datetime.now() - timedelta(days= 30 * 13)
        
        lucratividade_df = lucratividade_df.filter(pl.col("DATA") >= pl.lit(data_inicio))
               
        lucratividade_df = lucratividade_df.group_by("FILIAL","CODPRO", pl.col("DATA").dt.day()).agg(pl.sum("CUSTOS_ENCARGOS_MENSAL", "VALOR_COMISSOES",
                                                                                 "CUSTO_CONSULTOR", "FATURAMENTO"))
        
        lucratividade_df = lucratividade_df.with_columns(
            (pl.col("FATURAMENTO").round(2) / pl.col("CUSTO_CONSULTOR"))
            .round(2).alias("FATOR_LUCRATIVIDADE")
            .fill_nan(pl.lit(0))
        )               
        
        lucratividade_df = lucratividade_df.select(
            pl.col("*"),
            ((pl.col("FATOR_LUCRATIVIDADE") - 1.2) / (3 - 1.2) * 1000)
            .clip(0, 1000)
            .round(2).alias("LUCRATIVIDADE_SCORE")
        )
        
        lucratividade_df = lucratividade_df.with_columns(pl.col("DATA").cast(pl.String))
        
        lucratividade_df = lucratividade_df.select("CODPRO", "LUCRATIVIDADE_SCORE")
        
        return lucratividade_df
    
    def get_lucratividade_score_mom():
        lucratividade_df = CustosService.get_custos_service()
        
        lucratividade_df = lucratividade_df.with_columns(pl.col("DATA").str.to_datetime("%d/%m/%Y").cast(pl.Date).alias("DATA"))
        
        lucratividade_df = lucratividade_df.filter(pl.col("DATA") >= datetime(2022, 1, 1))
                               
        lucratividade_df = lucratividade_df.group_by("FILIAL","CODPRO", pl.col("DATA")).agg(pl.sum("CUSTOS_ENCARGOS_MENSAL", "VALOR_COMISSOES",
                                                                                 "CUSTO_CONSULTOR", "FATURAMENTO"))
        
        lucratividade_df = lucratividade_df.with_columns(
            (pl.col("FATURAMENTO").round(2) / pl.col("CUSTO_CONSULTOR"))
            .round(2).alias("FATOR_LUCRATIVIDADE")
            .fill_nan(pl.lit(0))
        )
        
        lucratividade_df = lucratividade_df.select(
            pl.col("*"),
            ((pl.col("FATOR_LUCRATIVIDADE") - 1.2) / (3 - 1.2) * 1000)
            .clip(0, 1000)
            .round(2).alias("LUCRATIVIDADE_SCORE")
        )       
                
        lucratividade_df = lucratividade_df.with_columns(pl.col("DATA").cast(pl.String))
        
        lucratividade_df = lucratividade_df.select("FILIAL", "CODPRO", "DATA",
                                                   "CUSTOS_ENCARGOS_MENSAL", "VALOR_COMISSOES",
                                                   "CUSTO_CONSULTOR", "FATURAMENTO", "FATOR_LUCRATIVIDADE", "LUCRATIVIDADE_SCORE")
        
        return lucratividade_df
    
    def get_fator_lucratividade_historico():
        lucratividade_df = CustosService.get_custos_service()
        
        lucratividade_df = lucratividade_df.with_columns(pl.col("DATA").str.to_datetime("%d/%m/%Y").cast(pl.Date).alias("DATA"))
        
        data_inicio = datetime.now() - timedelta(days= 30 * 24)
        
        lucratividade_df = lucratividade_df.filter(pl.col("DATA") >= pl.lit(data_inicio))
               
        lucratividade_df = lucratividade_df.group_by("FILIAL","CODPRO", pl.col("DATA").dt.year()).agg(pl.sum("CUSTOS_ENCARGOS_MENSAL", "VALOR_COMISSOES",
                                                                                 "CUSTO_CONSULTOR", "FATURAMENTO"))
        
        lucratividade_historico = lucratividade_df.with_columns(
            (pl.col("FATURAMENTO").round(2) / pl.col("CUSTO_CONSULTOR"))
            .round(2).alias("FATOR_LUCRATIVIDADE")
            .fill_nan(pl.lit(0))
        )
        
        lucratividade_df = lucratividade_df.with_columns(pl.col("DATA").cast(pl.String))
        
        lucratividade_historico = lucratividade_historico.select("CODPRO", "DATA", "FATOR_LUCRATIVIDADE")
        
        return lucratividade_historico
        
    def get_ticket_medio():
        consultores_df = DimensoesDataFrames.consultores_df
        servicos_faturados_df = LucratividadeDataFrame.servicos_faturados_df
        
        consultores_df = consultores_df.select("FILIAL", "CODPRO", "NOM_TEC", "CPF")
        servicos_faturados_df = servicos_faturados_df.select("CODPRO","NroOS", "ValorServico", "DataVenda")
        servicos_faturados_df = servicos_faturados_df.with_columns(pl.col("DataVenda").alias("DATA"))
                
        servicos_faturados_df = servicos_faturados_df.with_columns(
            pl.col("DATA").str.strptime(pl.Date, strict=False)
        )
        
        servicos_faturados_df = servicos_faturados_df.with_columns(
            (pl.col("DATA").dt.year().cast(pl.Utf8) + '-' +
            pl.col("DATA").dt.month().cast(pl.Utf8).str.zfill(2) + '-01')
            .str.strptime(pl.Date, "%Y-%m-%d")
            .alias("DATA")
        )      
                
        data_inicio = datetime.now() - timedelta(days= 30 * 13)
        
        servicos_faturados_df = servicos_faturados_df.filter(pl.col("DATA") >= pl.lit(data_inicio))              
        
        faturamento_df = (servicos_faturados_df.group_by("CODPRO", pl.col("DATA").dt.day())
                                 .agg(pl.col("ValorServico").sum().round(2).alias("FATURAMENTO")))
        consultores_df = consultores_df.join(faturamento_df, on="CODPRO", how="left")
        
        quantidade_ordem_servico = servicos_faturados_df.group_by("CODPRO").agg(pl.n_unique("NroOS").alias("QTD_OS"))
        consultores_df = consultores_df.join(quantidade_ordem_servico, on="CODPRO", how="left")               
        
        consultores_df = consultores_df.select(
            pl.col("*"),
            (pl.col("FATURAMENTO") / pl.col("QTD_OS"))
            .round(2)
            .alias("TICKET_MEDIO_SERVICOS")
        )
        
        consultores_df = consultores_df.select("CODPRO", "QTD_OS", "FATURAMENTO", "TICKET_MEDIO_SERVICOS")
        
        return consultores_df
    
    def get_tipo_de_maquina():
        """
        Infere o modelo da máquina e o tipo de serviço em que o consultor realizou mais serviços nos últimos 12 meses
        """
        consultores_df = DimensoesDataFrames.consultores_df
        servicos_faturados_df = LucratividadeDataFrame.servicos_faturados_df
        
        consultores_df = consultores_df.select("FILIAL", "CODPRO", "NOM_TEC", "CPF")
        servicos_faturados_df = servicos_faturados_df.select("CODPRO","NroOS", "Chassi", "CodigoMarca", "Modelo", "DescricaoModelo", "TipoServico", "DataVenda")
        servicos_faturados_df = servicos_faturados_df.with_columns(pl.col("DataVenda").alias("DATA"))
                
        servicos_faturados_df = servicos_faturados_df.with_columns(
            pl.col("DATA").str.strptime(pl.Date, strict=False)
        )
        
        servicos_faturados_df = servicos_faturados_df.with_columns(
            (pl.col("DATA").dt.year().cast(pl.Utf8) + '-' +
            pl.col("DATA").dt.month().cast(pl.Utf8).str.zfill(2) + '-01')
            .str.strptime(pl.Date, "%Y-%m-%d")
            .alias("DATA")
        )      
                
        data_inicio = datetime.now() - timedelta(days= 30 * 13)
        
        servicos_faturados_df = servicos_faturados_df.filter(pl.col("DATA") >= pl.lit(data_inicio))
        
        servicos_faturados_df = servicos_faturados_df.filter(pl.col("CodigoMarca") == "JD ")
                
        servicos_faturados_df = servicos_faturados_df.with_columns(
                                                                    pl.concat_str([
                                                                    pl.col("DescricaoModelo").str.slice(0, 2),
                                                                    pl.lit(" "),
                                                                    pl.col("Chassi").str.extract(r"1?(\w{2})", 1)
                                                                ]).alias("modelo_concat_result")
                                                            )
        
        mapeamento = {
            "TR": "Trator",
            "CO": "Colheitadeira",
            "PU": "Pulverizador",
            "PV": "Pulverizador",
            "PLANT": "Plantadeira",
            "PLAT": "Plataforma",
            "DI": "DN"
        }
        
        cond = pl.lit("Desconhecido")
        for key, value in mapeamento.items():
            cond = pl.when(pl.col("DescricaoModelo").str.contains(key)).then(pl.lit(value)).otherwise(cond)          
                        
        servicos_faturados_df = servicos_faturados_df.with_columns(
            cond.alias("TipoEquipamento")
        )
        
        servicos_faturados_df = servicos_faturados_df.with_columns([
            pl.col(col_name).str.strip_chars().alias(col_name)
            for col_name in servicos_faturados_df.columns if servicos_faturados_df[col_name].dtype == pl.Utf8
        ])
        
        modelo_maquina_df = servicos_faturados_df.select("CODPRO", "NroOS", "Chassi", "Modelo",
                                                         "modelo_concat_result", "DescricaoModelo",
                                                         "TipoEquipamento", "TipoServico").unique(subset="NroOS")
        
        return modelo_maquina_df