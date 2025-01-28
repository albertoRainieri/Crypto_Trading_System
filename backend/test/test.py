
import os,sys
sys.path.insert(0,'..')

import pymongo
from datetime import datetime, timedelta
from database.DatabaseConnection import DatabaseConnection
from constants.constants import *
import json

db = DatabaseConnection()
db = db.get_db(DATABASE_ORDER_BOOK)

collections = db.list_collection_names()
date = datetime(2025,1,28,0,7) - timedelta(days=1)

for collection in collections:

    query = {"_id": {"$gt": date.isoformat()}} 
    docs = list(db[collection].find(query,{'_id':1, 'coin': 1}))

    for doc in docs:
        print(doc)