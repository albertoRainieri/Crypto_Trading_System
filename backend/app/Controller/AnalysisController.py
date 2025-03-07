import os,sys
sys.path.insert(0,'../..')
from database.DatabaseConnection import DatabaseConnection
from app.Helpers.Helpers import round_, timer_func, getsubstring_fromkey
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
    def getDataTracker(datetime_start, datetime_end):

        client = DatabaseConnection()
        db_tracker = client.get_db(DATABASE_TRACKER)
        coins_list = db_tracker.list_collection_names()

        dict_ = {}

        filter_query = {'_id': 1, 'price': 1,
                        'vol_1m': 1, 'buy_vol_1m': 1,
                        'vol_5m': 1, 'buy_vol_5m': 1,
                            'vol_15m': 1, 'buy_vol_15m': 1,
                            'vol_30m': 1, 'buy_vol_30m': 1,
                                'vol_60m': 1, 'buy_vol_60m': 1}
                        


        for instrument_name in coins_list:
            docs = list(db_tracker[instrument_name].find({"_id": {"$gte": datetime_start, "$lt": datetime_end}}, filter_query))
            dict_[instrument_name] = docs
        
        client.close()
        json_string = jsonable_encoder(dict_)
        return JSONResponse(content=json_string)
    
    @timer_func
    def getDataMarket(datetime_start, datetime_end):

        client = DatabaseConnection()
        db_market = client.get_db(DATABASE_MARKET)
        coins_list = db_market.list_collection_names()

        dict_ = {}

        filter_query = {'_id': 1, 'price': 1,
                        'n_trades': 1, 'volume': 1,
                        'buy_volume': 1, 'buy_n': 1
                        }
                        

        for instrument_name in coins_list:
            docs = list(db_market[instrument_name].find({"_id": {"$gte": datetime_start, "$lt": datetime_end}}, filter_query))
            dict_[instrument_name] = docs
        
        client.close()
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
        
        client = DatabaseConnection()
        db_benchmark = client.get_db(DATABASE_BENCHMARK)
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

        client.close()
        json_string = jsonable_encoder(dict_)
        return JSONResponse(content=json_string)
    
    def try_different_timeframe(start_timestamp, timeframe, rules_for_nan, db_tracker, coin, vol_field, buy_vol_field, event_key, log_nan_replaced):
        '''
        this function is used exclusively by "get_price_changes" to limit the search of NaN values.
        Indeed this function will search for the next better available data
        '''
        # check if timeframe is reference to days or hours
        # 1== Day; 2==Hour, 3==minute
        if timeframe[-1] == 'd':
            isDayHourMinute = 1
        elif timeframe[-1] == 'h':
            isDayHourMinute = 2
        else:
            isDayHourMinute = 3
        
        for different_timeframe in rules_for_nan[timeframe]:
            # define minutes
            if isDayHourMinute==1:
                minutes = different_timeframe * 1440 # days * minutes in 1 day
            elif isDayHourMinute==2:
                minutes = different_timeframe * 60 # hours * minutes in 1 hour
            else:
                minutes = different_timeframe #  minutes
            
            # set timestamp start and timestamp end for db query
            x_time_ago = datetime.fromisoformat(start_timestamp) - timedelta(minutes=minutes)
            timestamp_start = (x_time_ago - timedelta(minutes=10)).isoformat()
            timestamp_end = (x_time_ago + timedelta(minutes=10)).isoformat()
            
            # query the db
            docs = list(db_tracker[coin].find({"_id": {"$gte": timestamp_start, "$lt": timestamp_end}},{'_id': 0, 'price': 1, vol_field: 1, buy_vol_field: 1}))
            price_list = [obj['price'] for obj in docs if 'price' in obj and obj['price'] != None]
            vol_list = [obj[vol_field] for obj in docs if vol_field in obj and obj[vol_field] != None]
            buy_vol_list = [obj[buy_vol_field] for obj in docs if buy_vol_field in obj and obj[buy_vol_field] != None]

            # if found an available timeframe, stop the loop and return the docs
            if len(price_list) != 0 and len(vol_list) != 0 and len(buy_vol_list) != 0:
                if isDayHourMinute == 1:
                    new_timeframe = 'info_' + str(different_timeframe) + 'd'
                elif isDayHourMinute == 2:
                    new_timeframe = 'info_' + str(different_timeframe) + 'h'
                else:
                    new_timeframe = 'info_' + str(different_timeframe) + 'm'

                msg = f'-{start_timestamp} - {coin}: {timeframe} is replaced with {new_timeframe} - {event_key}'
                log_nan_replaced += [msg]
                return docs, log_nan_replaced
        
        msg = f'-{start_timestamp} - {coin}: CANNOT REPLACE ALL NAN VALUES - {event_key}'
        log_nan_replaced += [msg]
        return docs, log_nan_replaced

    def get_price_changes(request):
        '''
        this route outputs the price changes, volume and buy_volumes from the past until now. 
        The time intervals considered are those in the variable "list_timeframes"
        '''

        client = DatabaseConnection()
        db_tracker = client.get_db(DATABASE_TRACKER)

        # define the db columns and their corresponding time frames
        list_timeframes = {'info_5m': 6, 'info_15m': 15, 'info_30m': 30,
                           'info_1h': 60, 'info_2h': 120, 'info_3h': 180,
                           'info_6h': 360, 'info_12h': 720, 'info_18h': 1080,
                           'info_1d': 1440, 'info_2d': 1440*2,
                           'info_3d': 1440*3,'info_5d': 1440*5, 'info_7d': 1440*7} #minutes
        
        # define the rules, through which the nan values are replaced with values found with different timeframe.
        # for example if "info_7d" is missing in db, then the next search will be of 6 days, if still it is nan values, then the search of 8 days will be performed and so on according to [6,8,5,9]
        # IMPORTANT:
        # KEYS MUST MATCH THOS OF "list_timeframes"
        rules_for_nan = {'info_5m': [7,8,9], 'info_15m': [13,17,11,19], 'info_30m': [28,32,26,34],
                         'info_1h': [0.5, 1.5, 2, 2.5], 'info_2h': [1.5, 2.5, 1, 3, 3.5], 'info_3h': [2,4,5], #hours
                           'info_6h': [5,7,4,8,9,10], 'info_12h': [11,13,10,14,15,16], 'info_18h': [17, 19, 16, 20, 16, 21], #hours
                           'info_1d': [0.85, 1.15, 0.7, 1.3, 1.5, 1.7, 2], 'info_2d': [1.85, 2.15, 1.7, 2.3, 1.5, 2.5, 2.75, 3],
                           'info_5d': [4.5, 5.5, 4,6, 3.5, 6.5, 3, 7], # days
                           'info_3d': [2.85, 3.15, 2.7, 3.3, 2.5, 3.5, 3.75, 2.25, 4, 4.5, 5],'info_7d': [6.5 ,7.5, 6, 7, 5, 9]} #days


        response = {}
        log_nan_replaced = []

        for event_key in request:
            if event_key not in response:
                response[event_key] = {}

            # get vol and buy_vol field (db columns)
            vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = getsubstring_fromkey(event_key)

            for coin in request[event_key]:
                if coin not in response[event_key]:
                    response[event_key][coin] = {}
                for start_timestamp in request[event_key][coin]:
                    # get price, vol and buy_vol at the event timestamp
                    obj = list(db_tracker[coin].find({"_id": start_timestamp},{'_id': 0, 'price': 1}))
                    price_start_timestamp = obj[0]['price']
                    response[event_key][coin][start_timestamp] = {}
                    
                    # get the prices, vol, and buy_vol in the past according to "list_timeframes"
                    for timeframe in list_timeframes:

                        # set timestamp start and timestamp end for db query
                        x_time_ago = datetime.fromisoformat(start_timestamp) - timedelta(minutes=list_timeframes[timeframe])
                        timestamp_start = (x_time_ago - timedelta(minutes=5)).isoformat()
                        timestamp_end = (x_time_ago + timedelta(minutes=5)).isoformat()
                        
                        # query the db
                        docs = list(db_tracker[coin].find({"_id": {"$gte": timestamp_start, "$lt": timestamp_end}},{'_id': 0, 'price': 1, vol_field: 1, buy_vol_field: 1}))
                        
                        # get price, vol and buy_vol from "docs" only if they are not None
                        price_list = [obj['price'] for obj in docs if 'price' in obj and obj['price'] != None]
                        vol_list = [obj[vol_field] for obj in docs if vol_field in obj and obj[vol_field] != None]
                        buy_vol_list = [obj[buy_vol_field] for obj in docs if buy_vol_field in obj and obj[buy_vol_field] != None]

                        # if nan values are found, try different timeframes as per "rules_for_nan"
                        if len(price_list) == 0 and len(vol_list) == 0 and len(buy_vol_list) == 0:
                            docs, log_nan_replaced = AnalysisController.try_different_timeframe(start_timestamp, timeframe, rules_for_nan, db_tracker, coin, vol_field, buy_vol_field, event_key,log_nan_replaced)
                            price_list = [obj['price'] for obj in docs if 'price' in obj and obj['price'] != None]
                            vol_list = [obj[vol_field] for obj in docs if vol_field in obj and obj[vol_field] != None]
                            buy_vol_list = [obj[buy_vol_field] for obj in docs if buy_vol_field in obj and obj[buy_vol_field] != None]

                        # finally compute the price change with respect to the price at event start. Compute also the mean for volume and buy_volume
                        if len(docs) > 0:
                            if len(price_list) > 0:
                                price_change = round_((price_start_timestamp - np.mean(price_list)) / np.mean(price_list),4)
                            else:
                                price_change = None

                            if len(vol_list) > 0:
                                vol_change = round_(np.mean(vol_list),4)
                            else:
                                vol_change = None

                            if len(buy_vol_list) > 0:
                                buy_vol_change = round_(np.mean(buy_vol_list),4)
                            else:
                                buy_vol_change = None

                            response[event_key][coin][start_timestamp][timeframe] = (price_change, vol_change, buy_vol_change)
                        else:
                            response[event_key][coin][start_timestamp][timeframe] = (None, None, None)

        client.close()
        final_response = {'data': response, 'msg': log_nan_replaced}
        return final_response
    
    def get_timeseries(request):
        '''
        this function delivers the timeseries for a set of coins.
        Coin, timestamp, timeframe must be given for each event
        '''

        if os.getenv('ANALYSIS') == '0':
            return 'CHANGE THIS ENV VAR: ANALYSIS TO 1. wrong db selected'
        #print(request)
        #request = json.loads(request)
        #let's define a limit number of events for each coin. This is to avoid responses too heavy.
        event_key = list(request['info'].keys())[0]
        vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = getsubstring_fromkey(event_key)
        timeframe = request['timeframe']
        check_past = request['check_past']
        check_future = request['check_future']


        client = DatabaseConnection()
        db_tracker = client.get_db(DATABASE_TRACKER)

        coins = list(request['info'][event_key].keys())

        response = {'data': {}, 'msg': 'All data have been downloaded', 'retry': False}
        STOP = False
        n_coin = []
        
        n_events = 0
        for coin in coins:
            start_timestamp_list = request['info'][event_key][coin]

            for start_timestamp in start_timestamp_list:
                if coin not in n_coin:
                    n_coin.append(coin)

                n_events += 1

                timeseries_start = datetime.fromisoformat(start_timestamp).replace(second=0, microsecond=0)
                # datetime_start is the beginning start from which retrieving the observation
                datetime_start = timeseries_start - timedelta(minutes=check_past)
                datetime_end = timeseries_start + timedelta(minutes=timeframe) + timedelta(minutes=check_future)
                # let's get the iso format timestamps for querying mongodb
                timestamp_start = datetime_start.isoformat()
                timestamp_end = datetime_end.isoformat()

                filter_query = {'_id': 1, 'price': 1,
                                vol_field: 1, buy_vol_field: 1}
                
                docs = list(db_tracker[coin].find({"_id": {"$gte": timestamp_start, "$lt": timestamp_end}}, filter_query))
                
                if coin not in response['data']:
                    response['data'][coin] = {}
            
                response['data'][coin][start_timestamp] = {'data': docs}

        total_timeseries_expected = sum([1 for coin in request['info'][event_key] for start_timestamp in request['info'][event_key][coin] ])
        total_timeseries_downloaded = sum([1 for coin in response['data'] for _ in response['data'][coin]])

        if response['msg'] == 'WARNING: Request too big. Not all data have been downloaded, retry...':
            response['msg'] = f'WARNING: Request too big. Not all data have been downloaded. {total_timeseries_downloaded}/{total_timeseries_expected}. Retry...'
        elif total_timeseries_expected != total_timeseries_downloaded:
            response['msg'] = f'WARNING: timeseries downloaded: {total_timeseries_downloaded}/{total_timeseries_expected}'
        else:
            response['msg'] = f'All data have been downloaded. {total_timeseries_downloaded}/{total_timeseries_expected}'

        client.close()
        json_string = jsonable_encoder(response)
        return JSONResponse(content=json_string)
    

    def getTimeseries(request):
        '''
        this function delivers the timeseries for a set of coins.
        Coin, timestamp, timeframe must be given for each event
        DEPRECATED
        '''

        #print(request)
        #request = json.loads(request)
        #let's define a limit number of events for each coin. This is to avoid responses too heavy.
        timeframe = request['timeframe']
        if timeframe > 4000:
            n_event_limit = 1000
        elif timeframe > 1000:
            n_event_limit = 200
        elif timeframe > 300:
            n_event_limit = 400
        else:
            n_event_limit = 800

        client = DatabaseConnection()
        db_tracker = client.get_db(DATABASE_TRACKER)

        coins = list(request['info'].keys())
        # in case check_past exists, I want to retrieve x minutes of observations before che event.
        # if check_past is not None, then check_past must be an integer
        if 'check_past' in request:
            check_past = request['check_past']
        else:
            check_past = False

        response = {'data': {}, 'msg': 'All data have been downloaded', 'retry': False}
        STOP = False
        n_coin = []

        for coin in coins:
            if 'last_timestamp' not in request:
                request['last_timestamp'] = {}

            # let's retrieve the last event that has been already downloaded
            if coin not in request['last_timestamp']:
                request['last_timestamp'][coin] = datetime(2023,4,1).isoformat()
        
        n_events = 0
        for coin in coins:
            events = request['info'][coin]

            for event in events:
                if datetime.fromisoformat(event['event']) > datetime.fromisoformat(request['last_timestamp'][coin]):
                    if n_events <= n_event_limit:
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

                        filter_query = {'_id': 1, 'price': 1,
                                        'vol_1m': 1, 'buy_vol_1m': 1,
                                        'vol_5m': 1, 'buy_vol_5m': 1,
                                            'vol_15m': 1, 'buy_vol_15m': 1,
                                            'vol_30m': 1, 'buy_vol_30m': 1,
                                                'vol_60m': 1, 'buy_vol_60m': 1,
                                                'vol_3h': 1, 'buy_vol_3h': 1,
                                                    'vol_6h': 1, 'buy_vol_6h': 1,
                                                    'vol_24h': 1, 'buy_vol_24h': 1, 'trades_1m': 1, 'trades_5m': 1}
                        
                        docs = list(db_tracker[coin].find({"_id": {"$gte": timestamp_start, "$lt": timestamp_end}}, filter_query))
                        
                        if coin not in response['data']:
                            response['data'][coin] = {}
                    
                        response['data'][coin][event['event']] = {'data': docs, 'statistics': {'mean': event['mean'], 'std': event['std'], 'timeframe': timeframe}}
                    else:
                        response['retry'] = True
                        response['msg'] = 'WARNING: Request too big. Not all data have been downloaded, retry...'
                        STOP = True

        total_timeseries_expected = sum([1 for coin in request['info'] for event in request['info'][coin] if datetime.fromisoformat(event['event']) > datetime.fromisoformat(request['last_timestamp'][coin])])
        total_timeseries_downloaded = sum([1 for coin in response['data'] for _ in response['data'][coin]])

        if response['msg'] == 'WARNING: Request too big. Not all data have been downloaded, retry...':
            response['msg'] = f'WARNING: Request too big. Not all data have been downloaded. {total_timeseries_downloaded}/{total_timeseries_expected}. Retry...'
        elif total_timeseries_expected != total_timeseries_downloaded:
            response['msg'] = f'WARNING: timeseries downloaded: {total_timeseries_downloaded}/{total_timeseries_expected}'
        else:
            response['msg'] = f'All data have been downloaded. {total_timeseries_downloaded}/{total_timeseries_expected}'

        client.close()
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
    

    def get_crypto_timeseries(request):
        client = DatabaseConnection()
        db_tracker = client.get_db(DATABASE_TRACKER)
        crypto_timeseries = {}

        # get last data available
        all_data_downloaded = False
        last_doc = db_tracker['SOLUSDT'].find_one(sort=[("_id", -1)])
        last_timestamp = last_doc['_id']
        print(f'Last Timestamp for SOLUSDT in tracker db: {last_timestamp}')
        if datetime.fromisoformat(request['SOLUSDT'][-1]) > datetime.fromisoformat(last_timestamp):
            all_data_downloaded = True

        for coin in request:
            #print(coin)
            start_timestamp = request[coin][0]
            end_timestamp = request[coin][-1]
            crypto_timeseries[coin] = {}
            
            #print(timestamp)
            timeseries = list(db_tracker[coin].find({"_id": {"$gte": start_timestamp, "$lt": end_timestamp}}, {'price': 1}))

            for doc in timeseries:
                timestamp = doc['_id']
                date = datetime.fromisoformat(timestamp).strftime("%Y-%m-%d")
                if date not in crypto_timeseries[coin]:
                    n = 1
                    crypto_timeseries[coin][date] = [doc['price'],n]
                else:
                    crypto_timeseries[coin][date][1] += 1

        response = {'data': crypto_timeseries, 'all_data_downloaded': all_data_downloaded}
        print(f'All Data Downloaded: {all_data_downloaded}')

        client.close()
        json_string = jsonable_encoder(response)
        return JSONResponse(content=json_string)
    

    def get_orderbook(request):
        client = DatabaseConnection()
        db_order_book = client.get_db(DATABASE_ORDER_BOOK)

        response = {}

        for event_key in request:
            if event_key not in response:
                response[event_key] = {}
            for coin in request[event_key]:
                if coin not in response[event_key]:
                    response[event_key][coin] = {}
                for _id in request[event_key][coin]:
                    docs = list(db_order_book[event_key].find({'_id': _id}, {'_id': 0, 'ranking': 1, 'data': 1}))
                    response[event_key][coin][_id] = docs
        
        client.close()
        json_string = jsonable_encoder(response)
        return JSONResponse(content=json_string)
            
                    





        
        


