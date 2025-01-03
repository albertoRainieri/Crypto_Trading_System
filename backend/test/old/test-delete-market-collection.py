import os,sys
import json
sys.path.insert(0,'..')

import pymongo
from datetime import datetime, timedelta
from database.DatabaseConnection import DatabaseConnection
from constants.constants import *

'''
This script deletes records in the Tracker Database.
Inputs are "start_time" and "end_time"
'''


tokens_do_not_delete = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'ARBUSDT', 'PEPEUSDT', 'LTCUSDT',
                        'SUIUSDT', 'EDUUSDT', 'BNBUSDT', 'SOLUSDT', 'CFXUSDT', 'LDOUSDT', 
                        'DOGEUSDT', 'MATICUSDT', 'APTUSDT', 'FTMUSDT', 'GALAUSDT', 'ADAUSDT',
                        'EURUSDT', 'IDUSDT', 'INJUSDT', 'FILUSDT', 'OPUSDT', 'AGIXUSDT',
                        'SHIBUSDT', 'LINKUSDT', 'TRXUSDT', 'ATOMUSDT', 'ARPAUSDT', 'DOTUSDT',
                        'SANDUSDT', 'JASMYUSDT', 'WOOUSDT', 'LUNCUSDT']

# Connect to MongoDB
client = DatabaseConnection()
db_market = client.get_db(DATABASE_MARKET)
db_benchmark = client.get_db(DATABASE_BENCHMARK)

# Get the collection names
collection_names_market = db_market.list_collection_names()
collection_names_benchmark = db_benchmark.list_collection_names()

print('starting price1')
path = '/backend/info/prices1.json'
if os.path.exists(path):
    f = open (path, "r")
    last_prices = json.loads(f.read())

# start_time = datetime.utcnow().replace(hour=5, minute=32, second=0, microsecond=0)
# end_time = start_time + timedelta(hours=0, minutes=3)
# print(start_time)

# Iterate over each collection
for coin in last_prices:
    if last_prices[coin] == None:

        if coin in collection_names_market:
            result = db_market[coin].drop()
            print(f'{coin} to be deleted for db_market: {result}')
        if coin in collection_names_benchmark:
            result = db_benchmark[coin].drop()
            print(f'{coin} to be deleted for db_benchmark: {result}')

print('starting price2')
path = '/backend/info/prices2.json'
if os.path.exists(path):
    f = open (path, "r")
    last_prices = json.loads(f.read())

# start_time = datetime.utcnow().replace(hour=5, minute=32, second=0, microsecond=0)
# end_time = start_time + timedelta(hours=0, minutes=3)
# print(start_time)

# Iterate over each collection
for coin in last_prices:
    if last_prices[coin] == None:

        if coin in collection_names_market:
            result = db_market[coin].drop()
            print(f'{coin} to be deleted for db_market: {result}')
        if coin in collection_names_benchmark:
            result = db_benchmark[coin].drop()
            print(f'{coin} to be deleted for db_benchmark: {result}')
        

    # # Delete the records within the time range
    # result = collection.delete_many(query)
    # print(f"Deleted {result.deleted_count} records from '{collection_name}' collection.")

client.close()