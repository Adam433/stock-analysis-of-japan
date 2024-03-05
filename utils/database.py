import configparser
import mysql.connector

config = configparser.ConfigParser()
config.read('config.ini')

conn = mysql.connector.connect(
    host=config['mysql']['host'],
    user=config['mysql']['user'],
    password=config['mysql']['password'],
    database=config['mysql']['database']
)