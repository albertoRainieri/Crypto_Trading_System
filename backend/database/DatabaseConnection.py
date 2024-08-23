import os
from pymongo import MongoClient
import pymongo
from time import sleep
from app.Controller.LoggingController import LoggingController

class DatabaseConnection:

    def __init__(self):

        logger = LoggingController.start_logging()
        self.username = os.getenv('MONGO_USERNAME')
        self.password = os.getenv('MONGO_PASSWORD')
        self.analysis_mode = bool(int(os.getenv('ANALYSIS')))
        self.db_name = os.getenv('MONGO_DB_NAME')
        self.port = os.getenv('MONGO_PORT')
        
        if self.analysis_mode:
            logger.info("Analysis Mode activated")
            self.db_url = os.getenv('MONGO_DB_ANALYSIS_URL')

        else:
            logger.info('Backend is starting in production mode, be sure "mongo" container is running')
            self.db_url = os.getenv('MONGO_DB_URL')


        logger.info(f'ENV: db url : {self.db_url}')
        logger.info(f'ENV: Analysis: {self.analysis_mode}')
        if self.db_url == "mongo-analysis" and not self.analysis_mode:
            logger.info(f'WARNING: .env file is not set properly: \n MONGO_DB_URL = mongo-analysis \n ANALYSIS = {self.analysis_mode}. \n If you want the backend to work in anaysis mode, change the env variable ANALYSIS, otherwise change the db_url')
            sleep(1000000)

    def get_db(self, database='default'):
        try:

            db = MongoClient(host=self.db_url, port=int(self.port), username=self.username,
                                    password=self.password, serverSelectionTimeoutMS=1500)
                #db = MongoClient("mongodb://root:password@mongo:27017/")

            return db[database]
        except pymongo.errors.ServerSelectionTimeoutError as err:
            logger.info('Connection Refused or Error Auth')
            sleep(5)