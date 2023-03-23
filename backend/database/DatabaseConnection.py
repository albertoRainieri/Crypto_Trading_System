import os
from pymongo import MongoClient
import pymongo


class DatabaseConnection:

    def __init__(self):

        self.username = os.getenv('MONGO_USERNAME')
        self.password = os.getenv('MONGO_PASSWORD')
        self.db_url = os.getenv('MONGO_DB_URL')
        self.db_name = os.getenv('MONGO_DB_NAME')
        self.port = os.getenv('MONGO_PORT')

    def get_db(self, database='default'):
        try:

            db = MongoClient(host=self.db_url, port=int(self.port), username=self.username,
                                     password=self.password, serverSelectionTimeoutMS=1500)
            #db = MongoClient("mongodb://root:password@mongo:27017/")

            return db[database]
        except pymongo.errors.ServerSelectionTimeoutError as err:
            print('Connection Refused or Error Auth')