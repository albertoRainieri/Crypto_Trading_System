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
start_time = datetime.utcnow().replace(year=2024, month=1, day=1, hour=12, minute=25, second=30, microsecond=0)
#end_time = start_time + timedelta(hours=5, minutes=30)
end_time = datetime.utcnow().replace(year=2024, month=9, day=1, hour=12, minute=26, second=30, microsecond=0)



# Connect to MongoDB
db = DatabaseConnection()
db = db.get_db(DATABASE_MARKET)

# Get the collection names
collection_name = 'ETHUSDT'


last_10_records = db[collection_name].find().sort("_id", -1).limit(5)

for record in last_10_records:
    print(record)

