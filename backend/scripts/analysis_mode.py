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



def main():
    '''
    Main Script for Analysis Mode.
    This script is able sync the db with the production enabling all the analysis trials in local
    '''
    DELAY_SEARCH_SECONDS=30
    DATA_RECOVERING = False
    DAYS = 0.25 # HOW many days of data will be recovered each run
    db = DatabaseConnection()
    db_backend = db.get_db(database=DATABASE_MARKET)
    db_tracker = db.get_db(database=DATABASE_TRACKER)
    db_benchmark = db.get_db(database=DATABASE_BENCHMARK)

    

    # db_tracker, db_market, db_benchmark
    while True:
        now = datetime.now()
        last_doc_saved = db_tracker['BTCUSDT'].find_one(sort=[("_id", -1)])
        last_timestamp_db = datetime.fromisoformat(last_doc_saved['_id'])
        days_missing = now - last_timestamp_db
        logger.info(days_missing)
        
        if DATA_RECOVERING or days_missing > timedelta(days=3):
            logger.info('Started Run for recovering data from Server')
            # this section we make sure that analysis/json and db are aligned
            path_json, data_json, last_datetime_saved = return_most_recent_json(last_timestamp_db)
            # if there are more than DAYS days of missing data, keep fetching data
            if days_missing > timedelta(days=DAYS):
                DATA_RECOVERING = True

            # if there are less than DAYS days of missing data, stop fetching data
            if DATA_RECOVERING and days_missing < timedelta(days=DAYS):
                DATA_RECOVERING = False
                logger.info(f'Less than {DAYS} days of data missing. Last Run')
                # start last data fetching from server
            
            datetime_start = last_datetime_saved + timedelta(seconds=DELAY_SEARCH_SECONDS)
            datetime_end = datetime_start + timedelta(days=DAYS)
            datetime_start_iso = datetime_start.isoformat()
            datetime_end_iso = datetime_end.isoformat()
            logger.info(f'Data will be fetched from server from {datetime_start_iso} to {datetime_end_iso}')
            

            response = request_fetching_data(datetime_start_iso, datetime_end_iso)
            status_code = response.status_code
            logger.info(f'Status Code: {status_code}. Check logs from Server')

            if response.status_code == 200:
                update_data_json(response, data_json, path_json, db_tracker, DAYS, DELAY_SEARCH_SECONDS)
                #TODO: COMMENT
                sleep(10)
            else:
                logger.info('Sleeping 5 Minutes')
                sleep(300)
            # Start saving data 
        
        else:
            sleep(600)
            
def update_data_json(response, data_json, path_json, db_tracker, DAYS, DELAY_SEARCH_SECONDS):
    '''
    this function updates the most recent json in analysis/json/
    it also updates the mongodb_analysis
    '''
    JSON_PATH_DIR = '/analysis/json_tracker'
    logger.info(f'Start updating {path_json}')
    new_data = response.json()
    btc_obs = len(new_data['BTCUSDT'])
    count_coins = sum([1 for coin in list(new_data.keys()) if len(new_data[coin]) != 0])
    logger.info(f'{btc_obs} new observations for {count_coins} BTC')
    n_instrument_newdata = 0


    for coin in new_data:
        coin_obs = []
        if coin not in data_json['data']:
            data_json['data'][coin] = []

        for new_obs in new_data[coin]:

            data_json['data'][coin].append(new_obs)
            coin_obs.append(new_obs)
        
        #TODO: COMMENT
        len_obs_coins = len(coin_obs)
        logger.info(f'New {len_obs_coins} observations for {coin}')
        # logger.info('Example of observation:')

        if len(coin_obs) > 0:
            logger.info(f'Adding {coin} to DB')
            try_insert_many(coin_obs, db_tracker, coin)
    
    logger.info(coin_obs[-1])

    
    #TODO: #delete next line
    #path_json = JSON_PATH_DIR + '/tmp.json'

    if btc_obs == 0:
        datetime_creation = (datetime.fromisoformat(data_json['data']['BTCUSDT'][-1]['_id']) + timedelta(days=DAYS))
        datetime_creation = (pytz.utc.localize(datetime_creation)).isoformat()
    else:
        datetime_creation = datetime.fromisoformat(data_json['data']['BTCUSDT'][-1]['_id'])
        datetime_creation = (pytz.utc.localize(datetime_creation)).isoformat()

    data_json['datetime_creation'] = datetime_creation
    json_string = json.dumps(data_json)

    logger.info(f'Starting to save {path_json} in analysi/json')
    with open(path_json, 'w') as outfile:
        outfile.write(json_string)
    logger.info(f'Saving to {path_json} was succesfull') 


    del data_json, json_string

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

def request_fetching_data(datetime_start_iso, datetime_end_iso):
    '''
    Make request to the server for fetching data
    '''
    SERVER = 'https://algocrypto.eu'

    logger.info('Making the request to the server')
    url = url = SERVER + '/analysis/get-data/'
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}", 
        "Content-Type": "application/json"
    }
    params = {'datetime_start': datetime_start_iso, 'datetime_end': datetime_end_iso}
    response = requests.get(url, headers=headers, params=params)
    return response


def return_most_recent_json(last_timestamp_db):
    '''
    This function returns the most recent json_path and relative data
    Dir Path is analsys/json/

    There are 3 scenarios:
    1) if no json_path exists, a new path is created and data initialized
    2) if json_path exists but it is greater than 1.4 GB, then a new path is created and data initialized
    3) otherwise most recent file and relative data are loaded
    '''

    JSON_PATH_DIR = '/analysis/json_tracker'
    path_dir = JSON_PATH_DIR
    list_json = os.listdir(path_dir)
    

    # if at least one data.json exists, get saved data
    if len(list_json) != 0:
        

        # get all full paths in json directory
        full_paths = [path_dir + "/{0}".format(x) for x in list_json]

        #print(full_path)
        most_recent_datetime = datetime(2020,1,1)
        # get the most recent json
        for full_path in full_paths:
            file_name = full_path.split('/')[-1]
            file_name_split = file_name.split('-')
            day = int(file_name_split[1])
            month = int(file_name_split[2])
            year = int(file_name_split[3])
            
            datetime_file_path = datetime(year=year, month=month, day=day)

            if datetime_file_path > most_recent_datetime:
                most_recent_datetime = datetime_file_path
                most_recent_file = full_path

        logger.info(f'Most recent file is {most_recent_file}')
        
        #Load data, and check "last_timestamp_fs" is synced with "last_timestamp_db"
        logger.info(f'Loading {most_recent_file} for updating with new data')
        f = open (most_recent_file, "r")
        data = json.loads(f.read())
        path_json = most_recent_file
        logger.info('Loaded')
        last_timestamp_fs = data['datetime_creation']

        last_timestamp_db = (pytz.utc.localize(last_timestamp_db)).isoformat()
        if last_timestamp_fs == last_timestamp_db:
            logger.info('Docker Container and filesystem /analysis/json are aligned')

        else:
            logger.info('')
            logger.info('WARNING')
            logger.info('Docker Container and filesystem /analysis/json are NOT aligned')
            logger.info(f'Timestamp from analysis/json {last_timestamp_fs}')
            logger.info(f'Timestamp from container db mongo-analysis {last_timestamp_db}')
            logger.info('')


        if os.path.getsize(most_recent_file) > 1200000000:
            last_record_split = last_timestamp_fs.split('-')
            last_record_split2 = last_timestamp_fs.split(':')
            year = last_record_split[0]
            month = last_record_split[1]
            day = last_record_split[2][:2]
            hour = last_record_split2[0][-2:]
            minute = str(int(last_record_split2[1]) + 1)
            path_json = f'{path_dir}/data-{day}-{month}-{year}-{hour}-{minute}.json'
            logger.info(F'NEW JSON INITIALIZED WITH PATH {path_json}')
            data = {'datetime_creation': last_timestamp_fs, 'data': {}}
        
        return path_json, data, datetime.fromisoformat(last_timestamp_fs)

    #if data.json does not exists, initialize data variable
    else:

        datetime_start = datetime(2023,5,7)
        datetime_start_iso = datetime_start.isoformat()
        year = str(datetime_start.year)
        month = str(datetime_start.month)
        day = str(datetime_start.day)
        minute = str(datetime_start.minute)
        hour = str(datetime_start.hour)
        path_json = f'{path_dir}/data-{day}-{month}-{year}-{hour}-{minute}.json'

        data = {'datetime_creation': datetime_start_iso, 'data': {}}
    
        return path_json, data, None
    

if __name__ == "__main__":
    logger = LoggingController.start_logging()
    logger.info('Analysis Mode Enabled')
    main()


