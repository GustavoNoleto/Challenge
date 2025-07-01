from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# Reading configuration from config.ini
config = ConfigParser()
config.read('infra/database/config/config.ini')

# Obtain configuration from config
driver = config.get('database', 'driver')
user = config.get('database', 'user')
password = config.get('database', 'password')
host = config.get('database', 'host')
database = config.get('database', 'database')


engine = create_engine(f'mssql+pyodbc://{user}:{password}@{host}/{database}?driver={driver}&timeout=30', pool_timeout=30, pool_recycle=1800)
Session = sessionmaker(bind=engine)
session = Session()

# Engine BD 2

# Obtain configuration from config
driver_2 = config.get('database_ruby', 'driver_2')
user_2 = config.get('database_ruby', 'user_2')
password_2 = config.get('database_ruby', 'password_2')
host_2 = config.get('database_ruby', 'host_2')
database_2 = config.get('database_ruby', 'database_2')


engine_bd2 = create_engine(f'mssql+pyodbc://{user_2}:{password_2}@{host_2}/{database_2}?driver={driver_2}&timeout=30', pool_timeout=30, pool_recycle=1800)
Session_bd2 = sessionmaker(bind=engine_bd2)
session_bd2 = Session_bd2()