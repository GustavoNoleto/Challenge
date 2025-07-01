from utils.get_dataframe import CreateDataFrame
import polars as pl
import tracemalloc
import schedule
import asyncio
from utils.logger import info_logger, error_logger, debug_logger

class ProdutividadeEficienciaDataframe():
    """
    Classe responsável por agendar e executar a criação e cache do DataFrame de produtividade e eficiência.
    """

    @staticmethod
    async def job_produtividade_eficiencia_df():
        """
        Função que executa a criação do DataFrame de produtividade e eficiência e salva em formato JSON.
        """
        try:
            info_logger.info("Iniciando o job para atualizar o DataFrame de produtividade e eficiência.")
            produtividade_eficiencia_df = CreateDataFrame.get_data(CreateDataFrame.PRODUTIVIDADEEFICIENCIA)
            produtividade_eficiencia_df.write_json('modules/produtividade_eficiencia_score/cache/produtividade_eficiencia_cache.json')
            info_logger.info("DataFrame de produtividade e eficiência atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame de produtividade e eficiência: %s", str(e))
            raise e

    produtividade_eficiencia_df = pl.read_json('modules/produtividade_eficiencia_score/cache/produtividade_eficiencia_cache.json')

    @staticmethod
    async def run_scheduled_jobs():
        """
        Função que agenda os jobs de atualização do DataFrame de produtividade e eficiência.
        """
        tracemalloc.start()
        info_logger.info("Iniciando agendamento das tarefas para atualizar o DataFrame de produtividade e eficiência.")

        try:
            schedule.every().day.at("07:30").do(asyncio.ensure_future, ProdutividadeEficienciaDataframe.job_produtividade_eficiencia_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, ProdutividadeEficienciaDataframe.job_produtividade_eficiencia_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, ProdutividadeEficienciaDataframe.job_produtividade_eficiencia_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, ProdutividadeEficienciaDataframe.job_produtividade_eficiencia_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, ProdutividadeEficienciaDataframe.job_produtividade_eficiencia_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, ProdutividadeEficienciaDataframe.job_produtividade_eficiencia_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, ProdutividadeEficienciaDataframe.job_produtividade_eficiencia_df())
        
            info_logger.info("Tarefas agendadas com sucesso.")
            
            while True:
                schedule.run_pending()
                await asyncio.sleep(1)

        except Exception as e:
            error_logger.error("Erro no agendamento das tarefas: %s", str(e))
            raise e

        finally:
            tracemalloc.stop()
            debug_logger.debug("Monitoramento de uso de memória interrompido.")
