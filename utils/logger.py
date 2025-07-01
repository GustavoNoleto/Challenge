from logging.handlers import RotatingFileHandler
import logging
import sys
from datetime import datetime

def setup_logger(name, log_file, level=logging.INFO, max_bytes=15 * 1024 * 1024, backup_count=5):
    """
    Configura um logger com o nome, arquivo de log e nível de log especificados.

    Args:
        name (str): O nome do logger a ser criado.
        log_file (str): O caminho do arquivo onde os logs serão gravados.
        level (int): O nível de log (ex: logging.INFO, logging.DEBUG, logging.ERROR).
        max_bytes (int): O tamanho máximo do arquivo de log em bytes antes de ser rotacionado (padrão: 15MB).
        backup_count (int): O número de arquivos de log de backup a serem mantidos.

    Returns:
        logging.Logger: O logger configurado.
    """
    logger = logging.getLogger(name)  # Criação do logger com o nome especificado.
    formatter = logging.Formatter(fmt="%(asctime)s - %(levelname)s - %(message)s")  # Formatação do log.

    # Criação de handlers
    stream_handler = logging.StreamHandler(sys.stdout)  # Handler para saída no console.

    # Modificando o log_file para incluir a data no nome do arquivo
    date_str = datetime.now().strftime("%d%m%Y")  # Formato da data como DDMMAAAA
    log_file_with_date = f"{log_file}_{date_str}.log"  # Novo nome do arquivo com a data
    file_handler = RotatingFileHandler(log_file_with_date, maxBytes=max_bytes, backupCount=backup_count)  # Handler para gravar logs em um arquivo rotativo.

    # Definindo o formato dos handlers
    stream_handler.setFormatter(formatter)  # Aplicando o formato ao handler de console.
    file_handler.setFormatter(formatter)  # Aplicando o formato ao handler de arquivo.

    # Adicionando handlers ao logger
    logger.handlers = [stream_handler, file_handler]  # Associando os handlers ao logger.

    # Definindo o nível do logger
    logger.setLevel(level)  # Configurando o nível de log do logger.

    return logger  # Retornando o logger configurado.

# Configurando loggers para diferentes níveis de log
info_logger = setup_logger("info_logger", "storage/overwiew_consultores_oficina_info", logging.INFO)
debug_logger = setup_logger("debug_logger", "storage/overview_consultores_oficina_debug", logging.DEBUG)
error_logger = setup_logger("error_logger", "storage/overview_consultores_oficina_error", logging.ERROR)
