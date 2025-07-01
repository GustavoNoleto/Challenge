from utils.get_dataframe import CreateDataFrame
from schemas.polars_schemas import PolarsSchemas
import polars as pl
import tracemalloc
import schedule
import asyncio
from utils.logger import info_logger, error_logger, debug_logger

class LucratividadeDataFrame():
    
    @staticmethod
    async def job_epi_consultores_df():
        """
        Atualiza o DataFrame da dimensão EPI dos consultores e salva no formato JSON.
        """
        try:
            info_logger.info("Iniciando a atualização do DataFrame EPI consultores.")
            epi_consultores_df = CreateDataFrame.get_data_bd2(CreateDataFrame.EPICONSULTORES, PolarsSchemas.epi_consultores_schema)
            epi_consultores_df.write_json('modules/lucratividade_score/cache/epi_consultores_cache.json')
            info_logger.info("DataFrame EPI consultores atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame EPI consultores: %s", str(e))
            raise e

    @staticmethod
    async def job_valor_hora_consultores_df():
        """
        Atualiza o DataFrame da dimensão valor hora dos consultores e salva no formato JSON.
        """
        try:
            info_logger.info("Iniciando a atualização do DataFrame valor hora consultores.")
            valor_hora_consultores_df = CreateDataFrame.get_data_bd2(CreateDataFrame.VALORHORACONSULTORES, PolarsSchemas.valor_hora_consultores_schema)
            valor_hora_consultores_df.write_json('modules/lucratividade_score/cache/valor_hora_consultores_cache.json')
            info_logger.info("DataFrame valor hora consultores atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame valor hora consultores: %s", str(e))
            raise e

    @staticmethod
    async def job_custos_consultores_df():
        """
        Atualiza o DataFrame da dimensão custos dos consultores e salva no formato JSON.
        """
        try:
            info_logger.info("Iniciando a atualização do DataFrame custos consultores.")
            custos_consultores_df = CreateDataFrame.get_data(CreateDataFrame.CUSTOSCONSULTORES)
            custos_consultores_df.write_json('modules/lucratividade_score/cache/custos_consultores_cache.json')
            info_logger.info("DataFrame custos consultores atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame custos consultores: %s", str(e))
            raise e

    @staticmethod
    async def job_servicos_faturados_df():
        """
        Atualiza o DataFrame da dimensão serviços faturados e salva no formato JSON.
        """
        try:
            info_logger.info("Iniciando a atualização do DataFrame serviços faturados.")
            servicos_faturados_df = CreateDataFrame.get_data(CreateDataFrame.SERVICOSFATURADOSLUCRATIVIDADE)
            servicos_faturados_df.write_json('modules/lucratividade_score/cache/servicos_faturados_cache.json')
            info_logger.info("DataFrame serviços faturados atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame serviços faturados: %s", str(e))
            raise e

    @staticmethod
    async def job_valor_comissoes_consultores_df():
        """
        Atualiza o DataFrame da dimensão valor das comissões dos consultores e salva no formato JSON.
        """
        try:
            info_logger.info("Iniciando a atualização do DataFrame valor comissões consultores.")
            valor_comissoes_consultores_df = CreateDataFrame.get_data_bd2(CreateDataFrame.VALORCOMISSOESCONSULTORES, PolarsSchemas.valor_comissoes_consultores_schema)
            valor_comissoes_consultores_df.write_json('modules/lucratividade_score/cache/valor_comissoes_consultores_cache.json')
            info_logger.info("DataFrame valor comissões consultores atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame valor comissões consultores: %s", str(e))
            raise e

    # Leitura inicial dos DataFrames a partir dos arquivos JSON
    epi_consultores_df = pl.read_json('modules/lucratividade_score/cache/epi_consultores_cache.json')
    valor_hora_consultores_df = pl.read_json('modules/lucratividade_score/cache/valor_hora_consultores_cache.json')
    valor_comissoes_consultores_df = pl.read_json('modules/lucratividade_score/cache/valor_comissoes_consultores_cache.json')
    custos_consultores_df = pl.read_json('modules/lucratividade_score/cache/custos_consultores_cache.json')
    servicos_faturados_df = pl.read_json('modules/lucratividade_score/cache/servicos_faturados_cache.json')

    @staticmethod
    async def run_scheduled_jobs():
        """
        Agenda e executa os jobs para atualizar os DataFrames em horários específicos ao longo do dia.
        """
        tracemalloc.start()
        info_logger.info("Iniciando agendamento das tarefas para atualizar os DataFrames de lucratividade.")
        
        try:
            schedule.every().day.at("07:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_epi_consultores_df())
            schedule.every().day.at("07:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_hora_consultores_df())
            schedule.every().day.at("07:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_custos_consultores_df())
            schedule.every().day.at("07:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("07:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_comissoes_consultores_df())

            schedule.every().day.at("09:35").do(asyncio.ensure_future, LucratividadeDataFrame.job_epi_consultores_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_hora_consultores_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, LucratividadeDataFrame.job_custos_consultores_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, LucratividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_comissoes_consultores_df())
            
            schedule.every().day.at("09:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_epi_consultores_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_hora_consultores_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_custos_consultores_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_comissoes_consultores_df())
            
            schedule.every().day.at("11:45").do(asyncio.ensure_future, LucratividadeDataFrame.job_epi_consultores_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_hora_consultores_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, LucratividadeDataFrame.job_custos_consultores_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, LucratividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_comissoes_consultores_df())
            
            schedule.every().day.at("13:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_epi_consultores_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_hora_consultores_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_custos_consultores_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_comissoes_consultores_df())
            
            schedule.every().day.at("15:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_epi_consultores_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_hora_consultores_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_custos_consultores_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_comissoes_consultores_df())
            
            schedule.every().day.at("17:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_epi_consultores_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_hora_consultores_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_custos_consultores_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, LucratividadeDataFrame.job_valor_comissoes_consultores_df())

            while True:
                schedule.run_pending()
                await asyncio.sleep(1)
        
        except Exception as e:
            error_logger.error("Erro no agendamento das tarefas: %s", str(e))
            raise e

        finally:
            tracemalloc.stop()
            debug_logger.debug("Monitoramento de uso de memória interrompido.")