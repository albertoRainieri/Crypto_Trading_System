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
start_time = datetime.utcnow().replace(year=2024, month=1, day=1, hour=13, minute=20, second=59, microsecond=0)
#end_time = start_time + timedelta(hours=5, minutes=30)
end_time = datetime.utcnow().replace(year=2025, month=2, day=1, hour=0, minute=0, second=0, microsecond=0)



# Connect to MongoDB
client = DatabaseConnection()
db = client.get_db(DATABASE_TRACKER)

# Get the collection names
collection_names = db.list_collection_names()
n_collections = len(collection_names)

# start_time = datetime.utcnow().replace(hour=5, minute=32, second=0, microsecond=0)
# end_time = start_time + timedelta(hours=0, minutes=3)
# print(start_time)
i = 0
# Iterate over each collection
for collection_name in collection_names:
    collection = db[collection_name]
    i += 1
    print(f"Processing {i}/{n_collections} collections")
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

client.close()