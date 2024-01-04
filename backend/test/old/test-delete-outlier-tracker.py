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

# Define the time range for deletion (midnight UTC until 05:28 UTC)
start_time = datetime.now().replace(year=2023, month=5, day=17, hour=13, minute=20, second=59, microsecond=0)
#end_time = start_time + timedelta(hours=5, minutes=30)
end_time = datetime.now().replace(year=2023, month=5, day=17, hour=13, minute=21, second=5, microsecond=0)

collection_names = ['SUIUSDT', 'EDUUSDT', 'BNBUSDT', 'SOLUSDT', 'CFXUSDT', 'LDOUSDT', 
                        'DOGEUSDT', 'MATICUSDT', 'APTUSDT', 'FTMUSDT', 'GALAUSDT',
                          'IDUSDT', 'INJUSDT', 'FILUSDT', 'OPUSDT', 'AGIXUSDT',
                        'SHIBUSDT', 'TRXUSDT', 'ATOMUSDT', 'ARPAUSDT', 'DOTUSDT',
                        'SANDUSDT', 'JASMYUSDT', 'WOOUSDT', 'LUNCUSDT']

# Connect to MongoDB
db = DatabaseConnection()
db = db.get_db(DATABASE_TRACKER)

# start_time = datetime.utcnow().replace(hour=5, minute=32, second=0, microsecond=0)
# end_time = start_time + timedelta(hours=0, minutes=3)
# print(start_time)

# Iterate over each collection
for collection_name in collection_names:
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