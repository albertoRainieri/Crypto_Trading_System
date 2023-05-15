import os,sys
sys.path.insert(0,'../..')
from database.DatabaseConnection import DatabaseConnection
from app.Helpers.Helpers import round_, timer_func
import requests
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from constants.constants import *
import json


class AnalysisController:
    def __init__(self) -> None:
        pass
    
    @timer_func
    def getData(datetime_start, datetime_end):

        
        
        db = DatabaseConnection()
        db_market = db.get_db(DATABASE_TRACKER)
        coins_list = db_market.list_collection_names()

        dict_ = {}


        for instrument_name in coins_list:
            docs = list(db_market[instrument_name].find({"_id": {"$gte": datetime_start, "$lt": datetime_end}}))
            dict_[instrument_name] = docs
        
        json_string = jsonable_encoder(dict_)
        return JSONResponse(content=json_string)
    

    def getMostTradedCoins():
        #TODO: UPDATE TO NUMBER_COINS_TO_TRADE_WSS AT 01/06
        f = open ('/backend/json/most_traded_coins.json', "r")
        data = json.loads(f.read())
        coin_list = data["most_traded_coins"][:NUMBER_COINS_TO_TRADE*SLICES+COINS_PRIORITY]
        coin_list = {"most_traded_coins": coin_list}

        return JSONResponse(json.dumps(coin_list))
    
    def getVolumeInfo():
        #TODO: UPDATE TO NUMBER_COINS_TO_TRADE_WSS AT 01/06
        f = open ('/backend/json/sorted_instruments.json', "r")
        data = json.loads(f.read())
        coin_list = data["most_traded_coins"][:NUMBER_COINS_TO_TRADE*SLICES+COINS_PRIORITY]
        volume_info = {"most_traded_coins": coin_list}

        return JSONResponse(json.dumps(volume_info))
        
        


