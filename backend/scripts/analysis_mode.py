import os,sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import json
from datetime import datetime, timedelta
import os
from time import sleep
from constants.constants import *
from app.Helpers.Helpers import round_, timer_func
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from constants.constants import ACCESS_TOKEN
from constants.constants import *
import requests
import pytz
from pymongo import errors
from app.Helpers.Helpers import get_benchmark_info



def main():
    '''
    Main Script for Analysis Mode.
    This script is able sync the db and the fs with the production enabling all the analysis trials in local
    '''
    DELAY_SEARCH_SECONDS=30
    DATA_RECOVERING = False
    DAYS = 0.25 # HOW many days of data will be recovered each run
    sleep(10)
    db = DatabaseConnection()
    db_market = db.get_db(database=DATABASE_MARKET)
    db_tracker = db.get_db(database=DATABASE_TRACKER)
    db_benchmark = db.get_db(database=DATABASE_BENCHMARK)
    

    # db_tracker, db_market, db_benchmark
    while True:
        
        logger.info('')
        logger.info('Iteration Started')
        now = datetime.now()
        last_doc_saved_tracker = db_tracker['DOGEUSDT'].find_one(sort=[("_id", -1)])
        last_timestamp_db_tracker = last_doc_saved_tracker['_id']
        last_timestamp_db = datetime.fromisoformat(last_doc_saved_tracker['_id'])
        #last_doc_saved_market = db_market['DOGEUSDT'].find_one(sort=[("_id", -1)])
        get_benchmark_info(db_benchmark)
        
        # if last_doc_saved_tracker != None and last_doc_saved_market != None:
        #     last_timestamp_db_tracker = last_doc_saved_tracker['_id']
        #     last_timestamp_db_market = last_doc_saved_market['_id']
        #     last_timestamp_db_tracker = datetime.fromisoformat(last_timestamp_db_tracker)
        #     last_timestamp_db_market = datetime.fromisoformat(last_timestamp_db_market)
        #     logger.info(f'Last DB Market: {last_timestamp_db_market}')
        #     logger.info(f'Last DB Tracker: {last_timestamp_db_tracker}')
        # elif last_doc_saved_tracker == None and last_doc_saved_market == None:
        #     last_timestamp_db_tracker = datetime(year=2023, month=5, day=7)
        #     last_timestamp_db_market = datetime(year=2023, month=3, day=29)
        # elif last_doc_saved_tracker == None:
        #     last_timestamp_db_tracker = datetime(year=2023, month=5, day=7)
        #     last_timestamp_db_market = datetime.fromisoformat(last_doc_saved_market['_id'])
        # elif last_doc_saved_market == None:
        #     last_timestamp_db_market = datetime(year=2023, month=3, day=29)
        #     last_timestamp_db_tracker = datetime.fromisoformat(last_doc_saved_tracker['_id'])


        # if areSameTimestamps(last_timestamp_db_tracker.isoformat(), last_timestamp_db_market.isoformat()):
        #     logger.info('DB Market is aligned with DB_Tracker')
        #     last_timestamp_db = datetime.fromisoformat(last_doc_saved_tracker['_id'])
        #     TRACKER=True
        #     MARKET=True
        # else:
        #     if last_timestamp_db_tracker < last_timestamp_db_market:
        #         logger.info('DB Market is ahead of DB_Tracker, last_timestamp_db is taken from Tracker')
        #         last_timestamp_db = last_timestamp_db_tracker
        #         TRACKER=True
        #         MARKET=False
        #     elif last_timestamp_db_tracker > last_timestamp_db_market:
        #         logger.info('DB Tracker is ahead of DB Market, last_timestamp_db is taken from Market')
        #         last_timestamp_db = last_timestamp_db_market
        #         TRACKER=False
        #         MARKET=True

        days_missing = now - last_timestamp_db
        logger.info(days_missing)
        
        if DATA_RECOVERING or days_missing > timedelta(days=DAYS*2):
            logger.info('Started Run for recovering data from Server')
            # this section we make sure that analysis/json_tracker and, analysis/json_market and db are aligned
            path_json_tracker, path_json_market, data_tracker, data_market, last_datetime_saved, timedelta_market_tracker, TRACKER, MARKET= return_most_recent_json(last_timestamp_db)
            logger.info(f'TRACKER: {TRACKER}')
            logger.info(f'MARKET: {MARKET}')
            datetime_start = last_datetime_saved + timedelta(seconds=DELAY_SEARCH_SECONDS)
            
            # if there are more than DAYS days of missing data, keep fetching data
            if days_missing > timedelta(days=DAYS*2):
                DATA_RECOVERING = True

            # if there are less than DAYS days of missing data, stop fetching data
            if DATA_RECOVERING and days_missing < timedelta(days=DAYS*2):
                DATA_RECOVERING = False
                logger.info(f'Less than {DAYS} days of data missing. Last Run')
                # start last data fetching from server
            

            if timedelta_market_tracker != None:
                datetime_end = datetime_start + min(timedelta(days=DAYS), timedelta_market_tracker)
            else:
                datetime_end = datetime_start + timedelta(days=DAYS)
            
            #TODO: COMMENT
            # if MARKET and not TRACKER:
            #     datetime_end = datetime_start + timedelta(days=0.5)

            datetime_start_iso = datetime_start.isoformat()

            # THIS IS A WORKAROUND IN CASE DB IS NOT ALIGNED AND FILESYSTEM IS, 
            # IT CAN HAPPEN IF IN PRODUCTION, TRACKER HAS MISSED DATA, BUT MARKET IS OK
            if datetime_end < datetime_start:
                datetime_end = datetime_start + timedelta(days=DAYS)
            datetime_end_iso = datetime_end.isoformat()

            if TRACKER and MARKET:
                logger.info(f'Data will be fetched from server from {datetime_start_iso} to {datetime_end_iso} for both Tracker and Market')
            elif TRACKER:
                logger.info(f'Data will be fetched from server from {datetime_start_iso} to {datetime_end_iso} for Tracker ONLY')
                
            elif MARKET:
                logger.info(f'Data will be fetched from server from {datetime_start_iso} to {datetime_end_iso} for Market ONLY')
            else:
                logger.info(f'No Data will be fetched')
            

            response_tracker, response_market = request_fetching_data(datetime_start_iso, datetime_end_iso, TRACKER, MARKET)

            if response_tracker != None and response_market != None:
                tracker_status_code = response_tracker.status_code
                market_status_code = response_market.status_code
            elif response_tracker != None:
                tracker_status_code = response_tracker.status_code
                market_status_code = None
            elif response_market != None:
                market_status_code = response_market.status_code
                tracker_status_code = None
            
            if TRACKER and  MARKET and tracker_status_code == 200 and market_status_code == 200:
                logger.info(f'Requests for both market and tracker are SUCCESFULL')
            elif TRACKER and MARKET and tracker_status_code != 200 and market_status_code != 200:
                logger.info(f'Requests for both market and tracker are NOT SUCCESFULL')
                logger.info(f'Status Code Tracker: {tracker_status_code}')
                logger.info(f'Response Tracker: {response_tracker}')
                logger.info(f'Status Code Market: {market_status_code}')
                logger.info(f'Response Market: {response_market}')
            elif TRACKER and  MARKET and tracker_status_code != 200:
                logger.info(f'Status Code Tracker: {tracker_status_code}')
                logger.info(f'Response Tracker: {response_tracker}')
                logger.info(f'Request for tracker is NOT SUCCESFULL')
            elif TRACKER and  MARKET and market_status_code != 200:
                logger.info(f'Request for market is NOT SUCCESFULL')
                logger.info(f'Status Code Market: {market_status_code}')
                logger.info(f'Response Market: {response_market}')
            elif TRACKER and tracker_status_code == 200:
                logger.info(f'Request for tracker is SUCCESFULL')
            elif MARKET and market_status_code == 200:
                logger.info(f'Request for market is SUCCESFULL')
            elif TRACKER and tracker_status_code != 200:
                logger.info(f'Request for tracker is NOT SUCCESFULL')
                logger.info(f'Status Code Tracker: {tracker_status_code}')
                logger.info(f'Response Tracker: {response_tracker}')
            elif MARKET and market_status_code != 200:
                logger.info(f'Request for market is NOT SUCCESFULL')
                logger.info(f'Status Code Market: {market_status_code}')
                logger.info(f'Response Market: {response_market}')
            
                

            if response_tracker != None and response_market != None and tracker_status_code == 200 and market_status_code == 200:
                logger.info('Uploading both Market and Tracker Data')
                update_data_json(response_tracker, response_market, data_tracker, data_market, path_json_tracker, path_json_market, db_tracker, db_market, TRACKER, MARKET, DAYS, datetime_end, datetime_start)
                del data_tracker, data_market
                sleep(10)
            elif response_tracker != None and tracker_status_code == 200:
                logger.info('Uploading Tracker Data to DB and Filesystem')
                update_data_json(response_tracker, response_market, data_tracker, data_market, path_json_tracker, path_json_market, db_tracker, db_market, TRACKER, MARKET, DAYS, datetime_end, datetime_start)
                del data_tracker, data_market
                sleep(10)
            elif response_market != None and market_status_code == 200:
                logger.info('Uploading Market Data to DB and Filesystem')
                update_data_json(response_tracker, response_market, data_tracker, data_market, path_json_tracker, path_json_market, db_tracker, db_market, TRACKER, MARKET, DAYS, datetime_end, datetime_start)
                del data_tracker, data_market
                sleep(10)
            else:
                logger.info('Sleeping 5 Minutes')
                sleep(300)
            # Start saving data
        
        else:
            sleep(600)

def areSameTimestamps(timestamp_a, timestamp_b):
    '''
    check if timestamps coincide to the minute, not second
    '''

    datetime_a = datetime.fromisoformat(timestamp_a)
    datetime_b = datetime.fromisoformat(timestamp_b)


    if datetime_a.year == datetime_b.year and datetime_a.month == datetime_b.month and datetime_a.day == datetime_b.day and datetime_a.hour == datetime_b.hour and datetime_a.minute == datetime_b.minute:
        return True
    else:
        return False
            
def update_data_json(response_tracker, response_market, data_tracker, data_market, path_json_tracker, path_json_market, db_tracker, db_market, TRACKER, MARKET, DAYS, datetime_end, datetime_start):
    '''
    this function updates the most recent json in analysis/json/
    it also updates the mongodb_analysis
    '''
    if TRACKER:
        JSON_PATH_DIR = '/analysis/json_tracker'
        logger.info(f'Start updating {path_json_tracker}')
        new_data = response_tracker.json()
        btc_obs = len(new_data['DOGEUSDT'])
        count_coins = sum([1 for coin in list(new_data.keys()) if len(new_data[coin]) != 0])
        logger.info(f'{btc_obs} new observations for {count_coins} coins')
        for coin in new_data:
            coin_obs = []
            if coin not in data_tracker['data']:
                data_tracker['data'][coin] = []
            for new_obs in new_data[coin]:

                data_tracker['data'][coin].append(new_obs)
                coin_obs.append(new_obs)
            
            #TODO: COMMENT
            len_obs_coins = len(coin_obs)
            logger.info(f'New {len_obs_coins} observations for {coin}')

            if len(coin_obs) > 0:
                logger.info(f'Adding {coin} to DB')
                try_insert_many(coin_obs, db_tracker, coin)
        if len(coin_obs) > 0:
            logger.info(coin_obs[-1])

        if btc_obs == 0 and (datetime_end - datetime_start) >= timedelta(days=DAYS) - timedelta(minutes=1):
            datetime_creation = (datetime.fromisoformat(data_tracker['data']['DOGEUSDT'][-1]['_id']) + timedelta(days=DAYS))
            datetime_creation = (pytz.utc.localize(datetime_creation)).isoformat()
        elif btc_obs == 0:
            print(datetime_end.isoformat())
            datetime_creation = datetime_end.isoformat()
        else:
            datetime_creation = datetime.fromisoformat(data_tracker['data']['DOGEUSDT'][-1]['_id'])
            datetime_creation = (pytz.utc.localize(datetime_creation)).isoformat()

        data_tracker['datetime_creation'] = datetime_creation
        json_string = json.dumps(data_tracker)

        logger.info(f'Starting to save {path_json_tracker} in analysis/json')
        with open(path_json_tracker, 'w') as outfile:
            outfile.write(json_string)
        logger.info(f'Saving to {path_json_tracker} was succesfull') 
        #del data_tracker, json_string

    if MARKET:
        JSON_PATH_DIR = '/analysis/json_market'
        logger.info(f'Start updating {path_json_market}')
        new_data = response_market.json()
        btc_obs = len(new_data['DOGEUSDT'])
        count_coins = sum([1 for coin in list(new_data.keys()) if len(new_data[coin]) != 0])
        logger.info(f'{btc_obs} new observations for {count_coins} BTC')
        n_instrument_newdata = 0


        for coin in new_data:
            # coin_obs = []
            if coin not in data_market['data']:
                data_market['data'][coin] = []

            for new_obs in new_data[coin]:
                data_market['data'][coin].append(new_obs)
                # coin_obs.append(new_obs)
            
            #TODO: COMMENT
            # len_obs_coins = len(coin_obs)
            # logger.info(f'New {len_obs_coins} observations for {coin}')
            # logger.info('Example of observation:')

            # if len(coin_obs) > 0:
            #     logger.info(f'Adding {coin} to DB')
            #     try_insert_many(coin_obs, db_market, coin)
        # if len(coin_obs) > 0:
        #     logger.info(coin_obs[-1])

        if btc_obs == 0 and datetime_end - datetime_start >= timedelta(days=DAYS) - timedelta(minutes=1):
            datetime_creation = (datetime.fromisoformat(data_tracker['data']['DOGEUSDT'][-1]['_id']) + timedelta(days=DAYS))
            datetime_creation = (pytz.utc.localize(datetime_creation)).isoformat()
        elif btc_obs == 0:

            datetime_creation = datetime_end
        else:
            datetime_creation = datetime.fromisoformat(data_market['data']['DOGEUSDT'][-1]['_id'])
            datetime_creation = (pytz.utc.localize(datetime_creation)).isoformat()

        data_market['datetime_creation'] = datetime_creation
        json_string = json.dumps(data_market)

        logger.info(f'Starting to save {path_json_market} in analysis/json')
        with open(path_json_market, 'w') as outfile:
            outfile.write(json_string)
        logger.info(f'Saving to {path_json_market} was succesfull') 
        del data_market, json_string

def try_insert_many(coin_obs, db_tracker, coin):
    try:
        db_tracker[coin].insert_many(coin_obs)
    except errors.BulkWriteError as e:
        logger.info(f'Duplicate key: {e} - {coin}')
        # Handle the exception, print the details, or perform other error handling
        for obs in coin_obs:
            try:
                db_tracker[coin].insert_one(obs)
            except:
                # logger.info(f'Skipping: {obs} - {coin}')
                continue

def request_fetching_data(datetime_start_iso, datetime_end_iso, TRACKER, MARKET):
    '''
    Make request to the server for fetching data
    '''
    SERVER = 'https://algocrypto.eu'
    SERVER = 'http://149.62.189.169'
    headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {ACCESS_TOKEN}", 
            "Content-Type": "application/json"
        }
    params = {'datetime_start': datetime_start_iso, 'datetime_end': datetime_end_iso}
    if TRACKER and MARKET:

        logger.info('Sending the request to the server for fetching Tracker data')
        url = SERVER + '/analysis/get-data-tracker/'
        response_tracker = requests.get(url=url, headers=headers, params=params, timeout=180)

        logger.info('Sending the request to the server for fetching Market data')
        url = SERVER + '/analysis/get-data-market/'
        response_market = requests.get(url=url, headers=headers, params=params, timeout=180)
        
    elif MARKET:
        url = SERVER + '/analysis/get-data-market/'
        logger.info(f'Sending the request to the server for fetching Market data. Url: {url}')
        
        response_market = requests.get(url=url, headers=headers, params=params)
        response_tracker = None
        
    elif TRACKER:
        logger.info('Sending the request to the server for fetching Tracker data')
        url = SERVER + '/analysis/get-data-tracker/'
        response_tracker = requests.get(url=url, headers=headers, params=params, timeout=180)
        response_market = None
        
    return response_tracker, response_market

def returnMostRecentTimestamp_TrackerMarket(path_dir_tracker, path_dir_market, list_json_tracker, list_json_market):
    # get all full paths in json directory
    full_paths_tracker = [path_dir_tracker + "/{0}".format(x) for x in list_json_tracker]
    #print(full_path)
    most_recent_datetime_tracker = datetime(2020,1,1)
    # get the most recent json
    for full_path_tracker in full_paths_tracker:
        file_name_tracker = full_path_tracker.split('/')[-1]
        if file_name_tracker.startswith('data'):
            file_name_split_tracker = file_name_tracker.split('-')
            day_tracker = int(file_name_split_tracker[3])
            month_tracker = int(file_name_split_tracker[2])
            year_tracker = int(file_name_split_tracker[1])
        
            datetime_file_path_tracker = datetime(year=year_tracker, month=month_tracker, day=day_tracker)

            if datetime_file_path_tracker > most_recent_datetime_tracker:
                most_recent_datetime_tracker = datetime_file_path_tracker
                most_recent_file_tracker = full_path_tracker
                path_json_tracker = most_recent_file_tracker

        # get all full paths in json directory
    full_paths_market = [path_dir_market + "/{0}".format(x) for x in list_json_market]
    #logger.info(list_json_market)
    #print(full_path)
    most_recent_datetime_market = datetime(2020,1,1)
    # get the most recent json
    for full_path_market in full_paths_market:
        file_name_market = full_path_market.split('/')[-1]
        if file_name_market.startswith('data'):
            file_name_market = full_path_market.split('/')[-1]
            file_name_split_market = file_name_market.split('-')
            day_market = int(file_name_split_market[3])
            month_market = int(file_name_split_market[2])
            year_market = int(file_name_split_market[1])
            
            datetime_file_path_market = datetime(year=year_market, month=month_market, day=day_market)

            if datetime_file_path_market > most_recent_datetime_market:
                most_recent_datetime_market = datetime_file_path_market
                most_recent_file_market = full_path_market
                path_json_market = most_recent_file_market


    f = open (most_recent_file_tracker, "r")
    data_tracker = json.loads(f.read())
    last_timestamp_fs_tracker = data_tracker['datetime_creation']
    logger.info(f'{most_recent_file_tracker} last ts: {last_timestamp_fs_tracker}')


    f = open (most_recent_file_market, "r")
    data_market = json.loads(f.read())
    last_timestamp_fs_market = data_market['datetime_creation']
    logger.info(f'{most_recent_file_market} last ts: {last_timestamp_fs_market}')



    return last_timestamp_fs_tracker, last_timestamp_fs_market, data_tracker, data_market, path_json_tracker, path_json_market

def initialize_new_json(most_recent_file_tracker, most_recent_file_market, last_timestamp_fs_tracker, last_timestamp_fs_market, path_dir_tracker, path_dir_market, TRACKER, MARKET):

    if TRACKER:
        if os.path.getsize(most_recent_file_tracker) > 700000000:
            last_record_split = last_timestamp_fs_tracker.split('-')
            last_record_split2 = last_timestamp_fs_tracker.split(':')
            year = last_record_split[0]
            month = last_record_split[1]
            day = last_record_split[2][:2]
            hour = last_record_split2[0][-2:]
            minute = str(int(last_record_split2[1]) + 1)
            new_path_json_tracker = f'{path_dir_tracker}/data-{year}-{month}-{day}-{hour}-{minute}.json'
            logger.info(F'TRACKER: new json initialized with path {new_path_json_tracker}')
            data_tracker = {'datetime_creation': last_timestamp_fs_tracker, 'data': {}}
        else:
            new_path_json_tracker = None
            data_tracker = None
    else:
        new_path_json_tracker = None
        data_tracker = None
    
    if MARKET:
        if os.path.getsize(most_recent_file_market) > 350000000:
            last_record_split = last_timestamp_fs_market.split('-')
            last_record_split2 = last_timestamp_fs_market.split(':')
            year = last_record_split[0]
            month = last_record_split[1]
            day = last_record_split[2][:2]
            hour = last_record_split2[0][-2:]
            minute = str(int(last_record_split2[1]) + 1)
            new_path_json_market= f'{path_dir_market}/data-{year}-{month}-{day}-{hour}-{minute}.json'
            logger.info(F'MARKET: new json initialized with path {new_path_json_market}')
            data_market = {'datetime_creation': last_timestamp_fs_market, 'data': {}}
        else:
            data_market = None
            new_path_json_market = None
    else:
        new_path_json_market = None
        data_market = None
    
    return data_tracker, data_market, new_path_json_tracker, new_path_json_market

def check_tracker_market_db_are_aligned(last_timestamp_fs_tracker, last_timestamp_fs_market, last_timestamp_db):

    if areSameTimestamps(last_timestamp_fs_tracker, last_timestamp_fs_market):
        logger.info('Tracker and Market ON THE FILESYSTEM Json are ALIGNED')
        last_timestamp_fs = last_timestamp_fs_tracker
        timedelta_market_tracker = None
        TRACKER = True
        MARKET = True
        if datetime.fromisoformat(pytz.utc.localize(last_timestamp_db).isoformat()) < datetime.fromisoformat(last_timestamp_fs_tracker):
            logger.info('DB is behind, just TRACKER = True')
            TRACKER = True
            MARKET = False
    else:
        logger.info('Tracker and Market Json are NOT aligned ON THE FILESYSTEM')
        logger.info(f'Tracker Timestamp: {last_timestamp_fs_tracker}')
        logger.info(f'Market Timestamp: {last_timestamp_fs_market}')

        if datetime.fromisoformat(last_timestamp_fs_market) < datetime.fromisoformat(last_timestamp_fs_tracker):
            last_timestamp_fs = last_timestamp_fs_market
            timedelta_market_tracker = datetime.fromisoformat(last_timestamp_fs_tracker) - datetime.fromisoformat(last_timestamp_fs_market)
            days_ahead = timedelta_market_tracker.days
            logger.info(f'Timedelta - Filesystem: Tracker is ahead of {timedelta_market_tracker} of {days_ahead} days')
            # Tracker data will not be downloaded
            TRACKER = False
            MARKET = True
        else:
            last_timestamp_fs = last_timestamp_fs_tracker
            timedelta_market_tracker = datetime.fromisoformat(last_timestamp_fs_market) - datetime.fromisoformat(last_timestamp_fs_tracker)
            days_ahead = timedelta_market_tracker.days
            logger.info(f'Timedelta: Market is ahead of {timedelta_market_tracker} of {days_ahead} days')
            # Market data will not be downloaded
            TRACKER = True
            MARKET = False

    last_timestamp_db = (pytz.utc.localize(last_timestamp_db)).isoformat()


    if TRACKER and MARKET and last_timestamp_fs == last_timestamp_db:
        logger.info('PERFECT! DBs and FSs are ALIGNED')
    elif MARKET == True and last_timestamp_fs == last_timestamp_db:
        logger.info('WARNING: Market FS is aligned with Tracker DB, but they are behind Tracker FS')
    elif TRACKER and last_timestamp_fs == last_timestamp_db:
        logger.info('WARNING: Tracker FS is aligned with Tracker DB, but they are behind Market FS')
    elif TRACKER and MARKET and last_timestamp_fs != last_timestamp_db:
        logger.info(f'last_timestamp_fs FROM MARKET AND TRACKER: {last_timestamp_fs}')
        logger.info(f'last_timestamp_db: {last_timestamp_db}')
        logger.info('')
    elif MARKET and last_timestamp_fs != last_timestamp_db:
        logger.info('')
        logger.info('WARNING')
        logger.info('FILESYSTEM IS ALIGNED BETWEEN MARKET AND TRACKER. BUT DB IS NOT')
    elif TRACKER and last_timestamp_fs != last_timestamp_db:
        logger.info('')
        logger.info('WARNING')
        logger.info('Tracker FS is behind, Market FS and Tracker DB')
        logger.info(f'Timestamp from analysis/json_tracker {last_timestamp_fs}')
        logger.info(f'Timestamp from container db_tracker {last_timestamp_db}')
        logger.info('')
        
    return last_timestamp_fs, timedelta_market_tracker, TRACKER, MARKET




def return_most_recent_json(last_timestamp_db):
    '''
    This function returns the most recent json_path and relative data
    Dir Path is analsys/json/

    There are 3 scenarios:
    1) if no json_path exists, a new path is created and data initialized
    2) if json_path exists but it is greater than 1 GB, then a new path is created and data initialized
    3) otherwise most recent file and relative data are loaded
    '''

    JSON_PATH_DIR_TRACKER = '/analysis/json_tracker'
    JSON_PATH_DIR_MARKET = '/analysis/json_market'
    path_dir_tracker = JSON_PATH_DIR_TRACKER
    path_dir_market = JSON_PATH_DIR_MARKET
    list_json_tracker = os.listdir(path_dir_tracker)
    list_json_market = os.listdir(path_dir_market)
    # print(list_json_market)
    # print(list_json_tracker)
    #sleep(10000)

    # if at least one data.json exists, get saved data
    if len(list_json_tracker) != 0 and len(list_json_market) != 0:
        logger.info('return_most_recent_json: both lists exist')
        # return the last timestamp saved in tracker, and market fs. 
        last_timestamp_fs_tracker, last_timestamp_fs_market, data_tracker, data_market, path_json_tracker, path_json_market = returnMostRecentTimestamp_TrackerMarket(path_dir_tracker, path_dir_market, list_json_tracker, list_json_market)
        last_timestamp_fs, timedelta_market_tracker, TRACKER, MARKET = check_tracker_market_db_are_aligned(last_timestamp_fs_tracker, last_timestamp_fs_market, last_timestamp_db)
        logger.info(f'TRACKER AFTER CHECK: {TRACKER}')
        logger.info(f'MARKET AFTER CHECK: {MARKET}')
        data_tracker_tmp, data_market_tmp, new_path_json_tracker, new_path_json_market = initialize_new_json(path_json_tracker, path_json_market, last_timestamp_fs_tracker, last_timestamp_fs_market, path_dir_tracker, path_dir_market, TRACKER, MARKET)

        # update data_tracker or data_market if new data structurtes have been initialized
        if data_tracker_tmp != None:
            data_tracker = data_tracker_tmp
        if data_market_tmp != None:
            data_market = data_market_tmp
        if new_path_json_tracker != None:
            path_json_tracker = new_path_json_tracker
        if new_path_json_market != None:
            path_json_market = new_path_json_market

        
        return path_json_tracker, path_json_market, data_tracker, data_market, datetime.fromisoformat(last_timestamp_fs), timedelta_market_tracker, TRACKER, MARKET
    
    elif len(list_json_market) == 0 and len(list_json_tracker) == 0:
        TRACKER=True
        MARKET=True
        datetime_start_tracker = datetime(2024,4,2) #datetime(2023,5,7)
        datetime_start_tracker_iso = datetime_start_tracker.isoformat()
        datetime_start_market = datetime(2024,1,30) #datetime(2023,3,29,12)
        datetime_start_market_iso = datetime_start_market.isoformat()
        if datetime_start_tracker < datetime_start_market:
            TRACKER=True
            MARKET=False
            datetime_start = datetime_start_tracker
            path_json_tracker = f'{path_dir_tracker}/data-{year}-{month}-{day}-{hour}-{minute}.json'
            path_json_market = None
            data_tracker = {'datetime_creation': datetime_start_tracker_iso, 'data': {}}
            data_market = None
        elif datetime_start_tracker == datetime_start_market:
            TRACKER=True
            MARKET=True
            datetime_start = datetime_start_tracker
            path_json_tracker = f'{path_dir_tracker}/data-{year}-{month}-{day}-{hour}-{minute}.json'
            path_json_market = f'{path_dir_market}/data-{year}-{month}-{day}-{hour}-{minute}.json'
            data_tracker = {'datetime_creation': datetime_start_tracker_iso, 'data': {}}
            data_market = {'datetime_creation': datetime_start_market_iso, 'data': {}}
        else:
            TRACKER=False
            MARKET=True
            datetime_start = datetime_start_market
            path_json_tracker = None
            path_json_market = f'{path_dir_market}/data-{year}-{month}-{day}-{hour}-{minute}.json'
            data_tracker = None
            data_market = {'datetime_creation': datetime_start_market_iso, 'data': {}}

        year = str(datetime_start.year)
        month = str(datetime_start.month)
        day = str(datetime_start.day)
        minute = str(datetime_start.minute)
        hour = str(datetime_start.hour)

        
        timedelta_market_tracker = None
        return path_json_tracker, path_json_market, data_tracker, data_market, datetime_start, timedelta_market_tracker, TRACKER, MARKET
    
    #if data.json does not exists, initialize data variable
    elif len(list_json_tracker) == 0:
        TRACKER=True
        MARKET=False
        datetime_start = datetime(2024,4,2) #datetime(2023,5,7)
        datetime_start_iso = datetime_start.isoformat()
        year = str(datetime_start.year)
        month = str(datetime_start.month)
        day = str(datetime_start.day)
        minute = str(datetime_start.minute)
        hour = str(datetime_start.hour)
        path_json_tracker = f'{path_dir_tracker}/data-{year}-{month}-{day}-{hour}-{minute}.json'
        data_tracker = {'datetime_creation': datetime_start_iso, 'data': {}}
        path_json_market = None
        data_market = None
        timedelta_market_tracker = None
        return path_json_tracker, path_json_market, data_tracker, data_market, datetime_start, timedelta_market_tracker, TRACKER, MARKET
    
    #if data.json does not exists, initialize data variable
    elif len(list_json_market) == 0:
        TRACKER=False
        MARKET=True
        datetime_start = datetime(2024,1,30) #datetime(2023,3,29,12)
        datetime_start_iso = datetime_start.isoformat()
        year = str(datetime_start.year)
        month = str(datetime_start.month)
        day = str(datetime_start.day)
        minute = str(datetime_start.minute)
        hour = str(datetime_start.hour)
        path_json_market = f'{path_dir_market}/data-{year}-{month}-{day}-{hour}-{minute}.json'
        data_market = {'datetime_creation': datetime_start_iso, 'data': {}}
        path_json_tracker = None
        data_tracker = None
        timedelta_market_tracker = None
        return path_json_tracker, path_json_market, data_tracker, data_market, datetime_start, timedelta_market_tracker, TRACKER, MARKET

    

    

if __name__ == "__main__":
    logger = LoggingController.start_logging()
    logger.info('Analysis Mode Enabled')
    SLEEP_ANALYSIS_ENV=os.getenv('SLEEP_ANALYSIS')
    SLEEP_ANALYSIS = bool(int(SLEEP_ANALYSIS_ENV))
    print(f'sleep: {SLEEP_ANALYSIS_ENV}')

    if SLEEP_ANALYSIS:
        logger.info('')
        logger.info('#################################################################################################')
        logger.info('WARNING: Sleep Analysis mode activated. To disable sleep mode, change SLEEP_ANALYSIS env variable')
        logger.info('#################################################################################################')
        logger.info('')
        sleep(1000)
    else:
        
        main()


