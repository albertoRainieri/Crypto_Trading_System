import os,sys
sys.path.insert(0,'..')

import pymongo
from datetime import datetime, timedelta
from database.DatabaseConnection import DatabaseConnection
from constants.constants import *

'''
This script deletes records in the Tracker Database.
Inputs are "start_time" and "end_time"
'''

# Define the time range for deletion 
start_time = datetime.utcnow().replace(year=2023, month=5, day=6, hour=23, minute=59, second=59, microsecond=0)
end_time = datetime.utcnow()


tokens_do_not_delete = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'ARBUSDT', 'PEPEUSDT', 'LTCUSDT',
                        'SUIUSDT', 'EDUUSDT', 'BNBUSDT', 'SOLUSDT', 'CFXUSDT', 'LDOUSDT', 
                        'DOGEUSDT', 'MATICUSDT', 'APTUSDT', 'FTMUSDT', 'GALAUSDT', 'ADAUSDT',
                        'EURUSDT', 'IDUSDT', 'INJUSDT', 'FILUSDT', 'OPUSDT', 'AGIXUSDT',
                        'SHIBUSDT', 'LINKUSDT', 'TRXUSDT', 'ATOMUSDT', 'ARPAUSDT', 'DOTUSDT',
                        'SANDUSDT', 'JASMYUSDT', 'WOOUSDT', 'LUNCUSDT']
print(len(tokens_do_not_delete), ' coins will not be deleted')
# Connect to MongoDB
db = DatabaseConnection()
db = db.get_db(DATABASE_TRACKER)

# Get the collection names
collection_names = db.list_collection_names()


# start_time = datetime.utcnow().replace(hour=5, minute=32, second=0, microsecond=0)
# end_time = start_time + timedelta(hours=0, minutes=3)
# print(start_time)

# Iterate over each collection
for collection_name in collection_names:
    if collection_name in tokens_do_not_delete:
        continue
    collection = db[collection_name]

    # Define the deletion query
    query = {
        '_id': {
            '$gte': start_time.isoformat(),
            '$lt': end_time.isoformat()
        }
    }

    # Delete the records within the time range
    result = collection.delete_many(query)
    print(f"Deleted {result.deleted_count} records from '{collection_name}' collection.")