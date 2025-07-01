from utils.get_dataframe import CreateDataFrame
from schemas.polars_schemas import PolarsSchemas
import polars as pl
import tracemalloc
import schedule
import asyncio
from utils.logger import info_logger, error_logger, debug_logger

class AssertividadeDataFrame():
    """Classe responsável por gerenciar DataFrames relacionados à assertividade e agendar jobs para atualizações."""

    @staticmethod
    async def job_retrabalho_df():
        """Atualiza o DataFrame de retrabalho e salva em formato JSON."""
        try:
            info_logger.info("Iniciando o job para atualizar o DataFrame de retrabalho.")
            retrabalho_df = CreateDataFrame.get_data(CreateDataFrame.RETRABALHO, PolarsSchemas.retrabalho_schema)
            retrabalho_df.write_json('modules/assertividade_score/cache/retrabalho_df_cache.json')
            info_logger.info("DataFrame de retrabalho atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame de retrabalho: %s", str(e))
            raise e

    @staticmethod
    async def job_servicos_faturados_df():
        """Atualiza o DataFrame de serviços faturados e salva em formato JSON."""
        try:
            info_logger.info("Iniciando o job para atualizar o DataFrame de serviços faturados.")
            servicos_faturados_df = CreateDataFrame.get_data(CreateDataFrame.SERVICOSFATURADOS)
            servicos_faturados_df.write_json('modules/assertividade_score/cache/servicos_faturados_cache.json')
            info_logger.info("DataFrame de serviços faturados atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame de serviços faturados: %s", str(e))
            raise e

    @staticmethod
    async def job_analise_relatorio_df():
        """Atualiza o DataFrame de análise de relatórios e salva em formato JSON."""
        try:
            info_logger.info("Iniciando o job para atualizar o DataFrame de análise de relatórios.")
            analise_relatorio_df = CreateDataFrame.get_data(CreateDataFrame.ANALISERELATORIO)
            analise_relatorio_df.write_json('modules/assertividade_score/cache/analise_relatorio_cache.json')
            info_logger.info("DataFrame de análise de relatórios atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame de análise de relatórios: %s", str(e))
            raise e

    @staticmethod
    async def job_devolucoes_oficina_df():
        """Atualiza o DataFrame de devoluções de oficina e salva em formato JSON."""
        try:
            info_logger.info("Iniciando o job para atualizar o DataFrame de devoluções de oficina.")
            devolucoes_oficina_df = CreateDataFrame.get_data(CreateDataFrame.DEVOLUCOESOFICINA)
            devolucoes_oficina_df.write_json('modules/assertividade_score/cache/devolucoes_oficina_cache.json')
            info_logger.info("DataFrame de devoluções de oficina atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame de devoluções de oficina: %s", str(e))
            raise e

    @staticmethod
    async def job_questoes_analise_relatorio_df():
        """Atualiza o DataFrame de questões de análise de relatórios e salva em formato JSON."""
        try:
            info_logger.info("Iniciando o job para atualizar o DataFrame de questões de análise de relatórios.")
            questoes_analise_relatorio_df = CreateDataFrame.get_data(CreateDataFrame.QANALISERELATORIO, PolarsSchemas.questoes_analise_relatorio_schema)
            questoes_analise_relatorio_df.write_json('modules/assertividade_score/cache/questoes_analise_relatorio_cache.json')
            info_logger.info("DataFrame de questões de análise de relatórios atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame de questões de análise de relatórios: %s", str(e))
            raise e

    retrabalho_df = pl.read_json('modules/assertividade_score/cache/retrabalho_df_cache.json')
    servicos_faturados_df = pl.read_json('modules/assertividade_score/cache/servicos_faturados_cache.json')
    analise_relatorio_df = pl.read_json('modules/assertividade_score/cache/analise_relatorio_cache.json')
    devolucoes_oficina_df = pl.read_json('modules/assertividade_score/cache/devolucoes_oficina_cache.json')
    questoes_analise_relatorio_df = pl.read_json('modules/assertividade_score/cache/questoes_analise_relatorio_cache.json')

    @staticmethod
    async def run_scheduled_jobs():
        """Executa o agendamento dos jobs para atualização dos DataFrames."""
        tracemalloc.start()
        info_logger.info("Iniciando agendamento das tarefas para atualizar os DataFrames de assertividade.")

        try:
            schedule.every().day.at("07:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_retrabalho_df())
            schedule.every().day.at("07:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("07:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_analise_relatorio_df())
            schedule.every().day.at("07:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_devolucoes_oficina_df())
            schedule.every().day.at("07:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_questoes_analise_relatorio_df())

            schedule.every().day.at("09:35").do(asyncio.ensure_future, AssertividadeDataFrame.job_retrabalho_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, AssertividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, AssertividadeDataFrame.job_analise_relatorio_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, AssertividadeDataFrame.job_devolucoes_oficina_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, AssertividadeDataFrame.job_questoes_analise_relatorio_df())          
            
            schedule.every().day.at("09:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_retrabalho_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_analise_relatorio_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_devolucoes_oficina_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_questoes_analise_relatorio_df())

            schedule.every().day.at("11:45").do(asyncio.ensure_future, AssertividadeDataFrame.job_retrabalho_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, AssertividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, AssertividadeDataFrame.job_analise_relatorio_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, AssertividadeDataFrame.job_devolucoes_oficina_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, AssertividadeDataFrame.job_questoes_analise_relatorio_df())

            schedule.every().day.at("13:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_retrabalho_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_analise_relatorio_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_devolucoes_oficina_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_questoes_analise_relatorio_df())

            schedule.every().day.at("15:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_retrabalho_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_analise_relatorio_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_devolucoes_oficina_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_questoes_analise_relatorio_df())

            schedule.every().day.at("17:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_retrabalho_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_servicos_faturados_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_analise_relatorio_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_devolucoes_oficina_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, AssertividadeDataFrame.job_questoes_analise_relatorio_df())

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
