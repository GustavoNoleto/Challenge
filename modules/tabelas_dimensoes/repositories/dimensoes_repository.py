from utils.get_dataframe import CreateDataFrame
from schemas.polars_schemas import PolarsSchemas
import polars as pl
import tracemalloc
import schedule
import asyncio
from utils.logger import info_logger, error_logger, debug_logger

class DimensoesDataFrames():
    """
    Classe responsável por agendar e atualizar DataFrames de dimensões específicos, 
    como consultores, valor da hora dos consultores e filiais.
    """
    
    @staticmethod
    async def job_consultores_df():
        """
        Atualiza o DataFrame da dimensão de consultores e salva o arquivo em cache.

        O método busca os dados dos consultores usando o schema definido, processa 
        o DataFrame e salva em formato JSON no cache. Também registra logs de 
        sucesso e erros durante o processo.
        """
        try:
            info_logger.info("Iniciando o job para atualizar o DataFrame dimensão consultores.")
            consultores_df = CreateDataFrame.get_data(CreateDataFrame.CONSULTORES, PolarsSchemas.consultores_schema)
            consultores_df.write_json('modules/tabelas_dimensoes/cache/consultores_cache.json')
            info_logger.info("DataFrame dimensão consultores atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame dimensão consultores: %s", str(e))
            raise e
        
    @staticmethod
    async def job_consultores_rh_df():
        """
        Atualiza o DataFrame da dimensão consultores_rh e salva o arquivo em cache.
        
        O método busca os dados dos consultores através de consulta sql, processa em formato de dataframe 
        e salva em formato JSON no cache. Registra logs para controle de sucesso e erros.
        """
        try:
            info_logger.info("Iniciando o job para atualizar o DataFrame dimensão consultores rh.")
            consultores_rh_df = CreateDataFrame.get_data_bd2(CreateDataFrame.CONSULTORESRH)
            consultores_rh_df.write_json('modules/tabelas_dimensoes/cache/consultores_rh_cache.json')
            info_logger.info("DataFrame dimensão consultores rh atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame dimensão consultores rh: %s", str(e))
            raise e
        
    
    @staticmethod
    async def job_valor_hora_consultores_df():
        """
        Atualiza o DataFrame da dimensão de valor da hora dos consultores e salva o arquivo em cache.

        O método busca os dados de valor/hora dos consultores usando o schema específico, 
        processa o DataFrame e salva em formato JSON no cache. Registra logs para controle de sucesso e erros.
        """
        try:
            info_logger.info("Iniciando o job para atualizar o DataFrame dimensão valor_hora_consultores.")
            valor_hora_consultores_df = CreateDataFrame.get_data_bd2(CreateDataFrame.VALORHORACONSULTORES, PolarsSchemas.valor_hora_consultores_schema)
            valor_hora_consultores_df.write_json('modules/tabelas_dimensoes/cache/valor_hora_consultores_cache.json')
            info_logger.info("DataFrame dimensão valor_hora_consultores atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame dimensão valor_hora_consultores: %s", str(e))
            raise e
        
    @staticmethod
    async def job_filiais_df():
        """
        Atualiza o DataFrame da dimensão de filiais e salva o arquivo em cache.

        O método coleta os dados das filiais, processa-os no formato do DataFrame e 
        salva os resultados em cache no formato JSON. Logs de status e erros são gerados.
        """
        try:
            info_logger.info("Iniciando o job para atualizar o DataFrame dimensão filiais.")
            filiais_df = CreateDataFrame.get_data(CreateDataFrame.FILIAIS)
            filiais_df.write_json('modules/tabelas_dimensoes/cache/filiais_cache.json')
            info_logger.info("DataFrame dimensão filiais atualizado e salvo com sucesso.")
        except Exception as e:
            error_logger.error("Erro ao gerar o DataFrame dimensão filiais: %s", str(e))
            raise e
        
    # Leitura dos arquivos cache já salvos
    consultores_df = pl.read_json('modules/tabelas_dimensoes/cache/consultores_cache.json')
    consultores_rh_df = pl.read_json('modules/tabelas_dimensoes/cache/consultores_rh_cache.json')
    valor_hora_consultores_df = pl.read_json('modules/tabelas_dimensoes/cache/valor_hora_consultores_cache.json')
    filiais_df = pl.read_json('modules/tabelas_dimensoes/cache/filiais_cache.json')
    
    @staticmethod
    async def run_scheduled_jobs():
        """
        Executa o agendamento de tarefas para atualizar os DataFrames de dimensões 
        em horários pré-definidos.

        Esta função inicia o monitoramento de memória com `tracemalloc` e define as 
        tarefas agendadas para serem executadas em horários específicos durante o dia. 
        O loop é contínuo, verificando pendências no agendamento e tratando erros 
        que possam ocorrer. Logs são gerados em cada etapa para monitoramento e depuração.
        """
        tracemalloc.start()
        info_logger.info("Iniciando agendamento das tarefas para atualizar o DataFrame dimensões.")
        
        try:
            # Definindo o agendamento das tarefas em horários específicos
            schedule.every().day.at("07:30").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_df())
            schedule.every().day.at("07:30").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_rh_df())
            schedule.every().day.at("07:30").do(asyncio.ensure_future, DimensoesDataFrames.job_valor_hora_consultores_df())
            schedule.every().day.at("07:30").do(asyncio.ensure_future, DimensoesDataFrames.job_filiais_df())

            schedule.every().day.at("09:35").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_rh_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, DimensoesDataFrames.job_valor_hora_consultores_df())
            schedule.every().day.at("09:35").do(asyncio.ensure_future, DimensoesDataFrames.job_filiais_df())
            
            schedule.every().day.at("09:30").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_rh_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, DimensoesDataFrames.job_valor_hora_consultores_df())
            schedule.every().day.at("09:30").do(asyncio.ensure_future, DimensoesDataFrames.job_filiais_df())
            
            schedule.every().day.at("11:45").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_rh_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, DimensoesDataFrames.job_valor_hora_consultores_df())
            schedule.every().day.at("11:45").do(asyncio.ensure_future, DimensoesDataFrames.job_filiais_df())
            
            schedule.every().day.at("13:30").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_rh_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, DimensoesDataFrames.job_valor_hora_consultores_df())
            schedule.every().day.at("13:30").do(asyncio.ensure_future, DimensoesDataFrames.job_filiais_df())
            
            schedule.every().day.at("15:30").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_rh_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, DimensoesDataFrames.job_valor_hora_consultores_df())
            schedule.every().day.at("15:30").do(asyncio.ensure_future, DimensoesDataFrames.job_filiais_df())
            
            schedule.every().day.at("17:30").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, DimensoesDataFrames.job_consultores_rh_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, DimensoesDataFrames.job_valor_hora_consultores_df())
            schedule.every().day.at("17:30").do(asyncio.ensure_future, DimensoesDataFrames.job_filiais_df())
            
            info_logger.info("Tarefas agendadas com sucesso.")
            
            while True:
                schedule.run_pending()
                await asyncio.sleep(1)
                tracemalloc.stop()
                
        except Exception as e:
            error_logger.error("Erro no agendamento das tarefas: %s", str(e))
            raise e
        
        finally:
            tracemalloc.stop()
            debug_logger.debug("Monitoramento de uso de memória interrompido.")
