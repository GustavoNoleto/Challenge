from configparser import ConfigParser

config = ConfigParser()
config.read('infra/database/config/config.ini')

user = config.get('login', 'user')
password = config.get('login', 'password')

class User():
    
    user = (f'{user}')
    password = (f"{password}")