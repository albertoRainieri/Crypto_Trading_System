import os,sys
sys.path.insert(0,'../..')
from database.DatabaseConnection import DatabaseConnection
from app.Helpers.Helpers import round_, timer_func
import requests
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi import Request
from constants.constants import *
import json
from datetime import datetime, timedelta
import numpy as np
from time import time


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

                        if datetime(year=year, month=month, day=day) > datetime.now() - timedelta(days=7):
                            volume_last_7days_mean_list.append(volume_series[date][0])
                            volume_last_7days_std_list.append(volume_series[date][1])

                    if len(volume_last_7days_mean_list) > 0 :

                        vol_mean_7days = np.mean(volume_last_7days_mean_list)
                        vol_std_7days = np.mean(volume_last_7days_std_list)

                        dict_[coin]['vol_mean_7days'] = round_(vol_mean_7days,2)
                        dict_[coin]['vol_std_7days'] = round_(vol_std_7days,2)

                        if cursor_benchmark[0]['volume_30_avg'] != 0:
                            dict_[coin]['momentum_7days_vol'] = round_(vol_mean_7days / cursor_benchmark[0]['volume_30_avg'],2)
                        else: 
                            dict_[coin]['momentum_7days_vol'] = 0

                        
                        #print(coin, ': ', dict_[coin]['momentum_7days_vol'])

                    
                    dict_[coin][field] = cursor_benchmark[0][field]

                elif field == 'Last_30_Trades':
                    #print(coin)
                    if field in cursor_benchmark[0]:
                        dict_[coin]['score_last_30_days'] = round_(cursor_benchmark[0][field]['score_last_30_trades'],2)
                        dict_[coin]['n_obs'] = len(cursor_benchmark[0][field]['list_last_30_trades'])
                    else:
                        dict_[coin]['score_last_30_days'] = 0
                        dict_[coin]['n_obs'] = 0
                else:
                    dict_[coin][field] = cursor_benchmark[0][field]
        
        json_string = jsonable_encoder(dict_)
        return JSONResponse(content=json_string)

    def get_price_changes(request):
        '''
        this route outputs the price changes from the past until now. 
        The time intervals considered are those in the variable "list_timeframes"
        '''

        db = DatabaseConnection()
        db_tracker = db.get_db(DATABASE_TRACKER)

        list_timeframes = {'price_%_1h': 60, 'price_%_3h': 180,
                           'price_%_6h': 360, 'price_%_1d': 1440,
                           'price_%_2d': 1440*2, 'price_%_3d': 1440*3} #minutes


        response = {}
        for risk_key in request:
            if risk_key not in response:
                response[risk_key] = {}
            for coin in request[risk_key]:
                if coin not in response[risk_key]:
                    response[risk_key][coin] = {}
                for start_timestamp in request[risk_key][coin]:
                    # get price at the event timestamp
                    obj = list(db_tracker[coin].find({"_id": start_timestamp},{'_id': 0, 'price': 1}))
                    price_start_timestamp = obj[0]['price']
                    response[risk_key][coin][start_timestamp] = {}
                    t1 = time()
                    # get the prices in the past according to "list_timeframes"
                    for timeframe in list_timeframes:
                        x_time_ago = datetime.fromisoformat(start_timestamp) - timedelta(minutes=list_timeframes[timeframe])
                        timestamp_start = (x_time_ago - timedelta(minutes=10)).isoformat()
                        timestamp_end = (x_time_ago + timedelta(minutes=10)).isoformat()
                        docs = list(db_tracker[coin].find({"_id": {"$gte": timestamp_start, "$lt": timestamp_end}},{'_id': 0, 'price': 1}))
                        price_list = [obj['price'] for obj in docs]

                        # finally compute the price change with respect to the price at event start
                        if len(docs) > 0:
                            response[risk_key][coin][start_timestamp][timeframe] = round_((price_start_timestamp - np.mean(price_list)) / np.mean(price_list),4)
                        else:
                            response[risk_key][coin][start_timestamp][timeframe] = None
                    t2 = time()
                    time_spent = round_(t2-t1, 2)
                    #print(f'Time Spent: {time_spent}')
        return response
    

    def getTimeseries(request):
        '''
        this function delivers the timeseries for a set of coins.
        Coin, timestamp, timeframe must be given for each event
        '''

        #print(request)
        #request = json.loads(request)
        #let's define a limit number of events for each coin. This is to avoid responses too heavy.
        timeframe = request['timeframe']
        if timeframe > 4000:
            n_event_limit = 3
            n_coin_limit = 30
        elif timeframe > 1000:
            n_event_limit = 6
            n_coin_limit = 70
        elif timeframe > 300:
            n_event_limit = 15
            n_coin_limit = 100
        else:
            n_event_limit = 100
            n_coin_limit = 200

        db = DatabaseConnection()
        db_tracker = db.get_db(DATABASE_TRACKER)

        coins = list(request['info'].keys())
        # in case check_past exists, I want to retrieve x minutes of observations before che event.
        # if check_past is not None, then check_past must be an integer
        if 'check_past' in request:
            check_past = request['check_past']
        else:
            check_past = False

        response = {'data': {}, 'msg': 'All data have been downloaded'}

        n_coin = []

        for coin in coins:

            if len(n_coin) <= n_coin_limit:
                events = request['info'][coin]

                # let's retrieve the last event that has been already downloaded
                if 'last_timestamp' in request and coin in request['last_timestamp']:
                    most_recent_datetime = datetime.fromisoformat(request['last_timestamp'][coin])
                else:
                    most_recent_datetime = datetime(2023,5,11)

                n_events = 0

                for event in events:
                    if datetime.fromisoformat(event['event']) > most_recent_datetime and n_events <= n_event_limit:
                        if coin not in n_coin:
                            n_coin.append(coin)

                        n_events += 1
                        if not check_past:
                            # datetime_start is the timestamp of the triggered event
                            datetime_start = datetime.fromisoformat(event['event']).replace(second=0, microsecond=0)
                            datetime_end = datetime_start + timedelta(minutes=timeframe)
                        else:
                            # timeseries_start is the timestamp of the triggered event
                            timeseries_start = datetime.fromisoformat(event['event']).replace(second=0, microsecond=0)
                            # datetime_start is the beginning start from which retrieving the observation
                            datetime_start = timeseries_start - timedelta(minutes=check_past)
                            datetime_end = timeseries_start + timedelta(minutes=timeframe) + timedelta(minutes=timeframe/3)

                        
                        # let's get the iso format timestamps for querying mongodb
                        timestamp_start = datetime_start.isoformat()
                        timestamp_end = datetime_end.isoformat()
                        
                        docs = list(db_tracker[coin].find({"_id": {"$gte": timestamp_start, "$lt": timestamp_end}}))
                        
                        if coin not in response['data']:
                            response['data'][coin] = {}
                    
                        response['data'][coin][event['event']] = {'data': docs, 'statistics': {'mean': event['mean'], 'std': event['std'], 'timeframe': timeframe, }}
            else:
                response['msg'] = 'WARNING: Request too big. Not all data have been downloaded, retry...'
                break

        json_string = jsonable_encoder(response)
        return JSONResponse(content=json_string)
    

    def riskmanagement_configuration(request):

        
        file_path = "/backend/riskmanagement/riskmanagement.json"
        print(type(request))
        try:
            with open(file_path, 'w') as file:
                json.dump(request, file)
            
            msg = 'Success'
        except Exception as e:
            msg = f'Error: {e}'

        response = {'msg': msg}
        json_string = jsonable_encoder(response)
        return JSONResponse(content=json_string)


    def user_configuration(request):
        # check if request is fine:
        key_set = set(request.keys())
        
        if len(key_set) == len(request):
            keys_to_check = ['trading_live', 'initialized_balance_account', 'initialized_investment_amount',
                             'clean_db_trading', 'api_key_path', 'private_key_path']
            
            SEND_ERROR = False
            for user in request:
                if not set(keys_to_check).issubset(request[user]):
                    SEND_ERROR = True
                    user_error = user
                    msg = f'Configuration for {user} is incorrect. some keys are missing'

                if SEND_ERROR:
                    print(msg)
                    break
                else:
                    file_path = "/backend/user_configuration/userconfiguration.json"
                    #print(type(request))
                    try:
                        with open(file_path, 'w') as file:
                            json.dump(request, file)
                        
                        msg = 'Success'
                    except Exception as e:
                        msg = f'Error: {e}'
            
        else:
            
            msg = 'Users are not unique'
            print(msg)

        response = {'msg': msg}
        json_string = jsonable_encoder(response)
        return JSONResponse(content=json_string)

        
        


