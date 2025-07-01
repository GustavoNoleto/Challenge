import polars as pl
from sqlalchemy import text
from fastapi import HTTPException
from sqlalchemy.exc import OperationalError
from infra.database.config.database_connect import session, engine, session_bd2, engine_bd2
from utils.logger import info_logger, error_logger

class CreateDataFrame():
    # SQL Query
    ANALISERELATORIO = 'infra/database/sql_query/analise_relatorio_consultor.sql'
    QANALISERELATORIO = 'infra/database/sql_query/questoes_analise_relatorio.sql'
    CENTRODECUSTO = 'infra/database/sql_query/centro_de_custo.sql'
    CONSULTORES = 'infra/database/sql_query/consultores.sql'
    CONSULTORESRH = 'infra/database/sql_query/consultores_rh.sql'
    CUSTOSCONSULTORES = 'infra/database/sql_query/custos_consultores.sql'
    DEVOLUCOESOFICINA = 'infra/database/sql_query/devolucoes_oficina.sql'
    REQUISICOESOFICINA = 'infra/database/sql_query/requisicoes_oficina.sql'
    RETRABALHO = 'infra/database/sql_query/retrabalho.sql'
    SERVICOSFATURADOS = 'infra/database/sql_query/servicos_faturados.sql'
    SERVICOSFATURADOSLUCRATIVIDADE = 'infra/database/sql_query/servicos_faturados_lucratividade.sql'
    EPICONSULTORES = 'infra/database/sql_query/registros_epis_consultores.sql'
    VALORHORACONSULTORES = 'infra/database/sql_query/valor_hora_consultores.sql'
    VALORCOMISSOESCONSULTORES = 'infra/database/sql_query/valor_comissoes_consultores.sql'
    PRODUTIVIDADEEFICIENCIA = 'infra/database/sql_query/produtividade_eficiencia.sql'
    FILIAIS = 'infra/database/sql_query/filiais.sql'

    @staticmethod
    def get_data(file='', schema=''):
        """
        Lê os dados de um arquivo SQL e retorna um DataFrame.

        Args:
            file (str): O caminho do arquivo SQL.
            schema (str): O esquema para o DataFrame.

        Returns:
            pl.DataFrame: O DataFrame com os dados retornados da consulta SQL.
        
        Raises:
            HTTPException: Se houver erros durante a execução.
        """
        info_logger.info("Iniciando a recuperação de dados do BD1 do arquivo: %s", file)  # Log de informação
        try:
            if not file:
                raise HTTPException(status_code=400, detail="SQLQuery File is empty!")

            with open(file, 'r') as data:
                query = data.read()                           
            
            stmt = str(text(query).compile(
                dialect=engine.dialect,
                compile_kwargs={"literal_binds": True}
            ))
            
            df = pl.read_database(stmt, session)
            df = pl.DataFrame(df, schema=schema)

            info_logger.info("Consulta realizada com sucesso do BD1 do arquivo: %s", file)  # Log de sucesso
            return df
            
        except OperationalError as e:
            session.rollback()
            error_logger.error("Erro de conexão: %s", str(e))  # Log de erro
            raise HTTPException(status_code=503, detail="Connection error, try again in a few seconds!")

        except Exception as e:
            session.rollback()
            error_logger.error("Erro interno: %s", str(e))  # Log de erro
            raise HTTPException(status_code=500, detail="Internal Server Error, please contact support!")

        finally:
            session.close()

    @staticmethod
    def get_data_bd2(file='', schema=''):
        """
        Lê os dados de um arquivo SQL e retorna um DataFrame a partir do banco de dados BD2.

        Args:
            file (str): O caminho do arquivo SQL.
            schema (str): O esquema para o DataFrame.

        Returns:
            pl.DataFrame: O DataFrame com os dados retornados da consulta SQL.
        
        Raises:
            HTTPException: Se houver erros durante a execução.
        """
        info_logger.info("Iniciando a recuperação de dados do BD2 do arquivo: %s", file)  # Log de informação
        try:
            if not file:
                raise HTTPException(status_code=400, detail="SQLQuery BD2 File is empty!")

            with open(file, 'r') as data:
                query = data.read()                           
            
            stmt = str(text(query).compile(
                dialect=engine_bd2.dialect,
                compile_kwargs={"literal_binds": True}
            ))
            
            df = pl.read_database(stmt, session_bd2)
            df = pl.DataFrame(df, schema=schema)

            info_logger.info("Consulta realizada com sucesso do BD2 do arquivo: %s", file)  # Log de sucesso
            return df
            
        except OperationalError as e:
            session_bd2.rollback()
            error_logger.error("Erro de conexão no BD2: %s", str(e))  # Log de erro
            raise HTTPException(status_code=503, detail="Connection error, try again in a few seconds!")

        except Exception as e:
            session_bd2.rollback()
            error_logger.error("Erro interno no BD2: %s", str(e))  # Log de erro
            raise HTTPException(status_code=500, detail="Internal Server Error, please contact support!")

        finally:
            session_bd2.close()
