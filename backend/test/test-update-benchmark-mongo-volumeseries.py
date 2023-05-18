import os,sys
sys.path.insert(0,'..')

import pymongo
from datetime import datetime, timedelta
from database.DatabaseConnection import DatabaseConnection
from constants.constants import *

'''
This script update records in the Benchmark Database.
It deletes the dates in the "volume_series" field older than 05 may 2023
Inputs are "start_time" and "end_time"
'''

# Define the time range for deletion (midnight UTC until 05:28 UTC)
start_time = datetime.utcnow().replace(year=2023, month=3, day=29, hour=0, minute=1, second=0, microsecond=0)
#end_time = start_time + timedelta(hours=5, minutes=30)
end_time = datetime.utcnow().replace(year=2023, month=5, day=16, hour=23, minute=59, second=59, microsecond=0)

tokens_do_not_update = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'ARBUSDT', 'PEPEUSDT', 'LTCUSDT',
                        'SUIUSDT', 'EDUUSDT', 'BNBUSDT', 'SOLUSDT', 'CFXUSDT', 'LDOUSDT', 
                        'DOGEUSDT', 'MATICUSDT', 'APTUSDT', 'FTMUSDT', 'GALAUSDT', 'ADAUSDT',
                        'EURUSDT', 'IDUSDT', 'INJUSDT', 'FILUSDT', 'OPUSDT', 'AGIXUSDT',
                        'SHIBUSDT', 'LINKUSDT', 'TRXUSDT', 'ATOMUSDT', 'ARPAUSDT', 'DOTUSDT',
                        'SANDUSDT', 'JASMYUSDT', 'WOOUSDT', 'LUNCUSDT']

# Connect to MongoDB
db = DatabaseConnection()
db_benchmark = db.get_db(DATABASE_BENCHMARK)

# Get the collection names
collection_names = db_benchmark.list_collection_names()


# start_time = datetime.utcnow().replace(hour=5, minute=32, second=0, microsecond=0)
# end_time = start_time + timedelta(hours=0, minutes=3)
# print(start_time)

# Iterate over each collection
for coin in collection_names:
    if coin in tokens_do_not_update:
        continue

    cursor_benchmark = list(db_benchmark[coin].find())
    id_benchmark = cursor_benchmark[0]['_id']
    volume_series = cursor_benchmark[0]['volume_series']
    for date in list(volume_series.keys()):
        date_split = date.split('-')
        year = int(date_split[0])
        month = int(date_split[1])
        day = int(date_split[2])

        if datetime(year=year, month=month, day=day) <= datetime(2023,5,16):
            volume_series.pop(date)

    db_benchmark[coin].update_one({"_id": id_benchmark}, {"$set": {"volume_series": volume_series}})