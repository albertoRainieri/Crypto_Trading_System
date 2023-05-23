import os,sys
sys.path.insert(0,'../..')
from database.DatabaseConnection import DatabaseConnection
from app.Helpers.Helpers import round_, timer_func
import requests
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from constants.constants import *
import json
from datetime import datetime, timedelta
import numpy as np


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
        f = open ('/backend/json/most_traded_coins.json', "r")
        data = json.loads(f.read())
        coin_list = data["most_traded_coins"]
        coin_list = {"most_traded_coins": coin_list}

        return JSONResponse(json.dumps(coin_list))
    
    def getVolumeInfo():
        f = open ('/backend/json/sorted_instruments.json', "r")
        data = json.loads(f.read())
        coin_list = data["most_traded_coins"]
        volume_info = {"most_traded_coins": coin_list}

        return JSONResponse(json.dumps(volume_info))
    

    def getBenchmarkInfo():
        
        db = DatabaseConnection()
        db_benchmark = db.get_db(DATABASE_BENCHMARK)
        coins_list = db_benchmark.list_collection_names()

        dict_ = {}
        for coin in coins_list:
            dict_[coin] = {}
            cursor_benchmark = list(db_benchmark[coin].find())
            
            fields = ['volume_30_avg', 'volume_30_std', 'Last_30_Trades', 'volume_series', 'volume_60_avg', 'volume_60_std', 'volume_90_avg', 'volume_90_std']
            volume_last_7days_mean_list = []
            volume_last_7days_std_list = []

            for field in fields:
                if field == 'volume_series':
                    volume_series = cursor_benchmark[0][field]

                    for date in list(volume_series.keys()):
                        date_split = date.split('-')
                        year = int(date_split[0])
                        month = int(date_split[1])
                        day = int(date_split[2])
                        # if coin == 'LOOMUSDT':
                        #     print(date)

                        if datetime(year=year, month=month, day=day) > datetime.now() - timedelta(days=7):
                            volume_last_7days_mean_list.append(volume_series[date][0])
                            volume_last_7days_std_list.append(volume_series[date][1])

                    if len(volume_last_7days_mean_list) > 0:
                        # if coin == 'LOOMUSDT':
                        #     print(volume_last_7days_mean_list)

                        vol_mean_7days = np.mean(volume_last_7days_mean_list)
                        vol_std_7days = np.mean(volume_last_7days_std_list)

                        dict_[coin]['vol_mean_7days'] = vol_mean_7days
                        dict_[coin]['vol_std_7days'] = vol_std_7days
                        dict_[coin]['momentum_7days_vol'] = round_(vol_mean_7days / cursor_benchmark[0]['volume_30_avg'],2)

                elif field == 'Last_30_Trades':
                    dict_[coin]['score_last_30_days'] = round_(cursor_benchmark[0][field]['score_last_30_trades'],2)
                    dict_[coin]['n_obs'] = len(cursor_benchmark[0][field]['list_last_30_trades'])

                else:
                    dict_[coin][field] = cursor_benchmark[0][field]
        
        json_string = jsonable_encoder(dict_)
        return JSONResponse(content=json_string)
        

        
        


