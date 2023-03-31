import os,sys
sys.path.insert(0,'../..')
from database.DatabaseConnection import DatabaseConnection
from app.Helpers.Helpers import round_, timer_func

from constants.constants import *
import json


class AnalysisController:
    def __init__(self) -> None:
        pass
    
    @timer_func
    def getData(datetime_start):
        
        db = DatabaseConnection()
        db_market = db.get_db(DATABASE_TRACKER)
        coins_list = db_market.list_collection_names()

        print(datetime_start)

        dict_ = {}
        if datetime_start == None:
            for instrument_name in coins_list:
                docs = list(db_market[instrument_name].find())
                #print(docs)
                dict_[instrument_name] = docs
        else:
            for instrument_name in coins_list:
                docs = list(db_market[instrument_name].find({"_id": {"$gte": datetime_start}}))
                #print(docs)
                dict_[instrument_name] = docs
        


        #json_string = json.dumps(dict_)

        return dict_

        
        
        


