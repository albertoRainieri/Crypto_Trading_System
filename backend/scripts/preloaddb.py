import os,sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import json
from datetime import datetime, timedelta
import os
from time import sleep
from constants.constants import *
from app.Helpers.Helpers import round_, timer_func
from app.Controller.LoggingController import LoggingController
from constants.constants import *
from database.DatabaseConnection import DatabaseConnection
from database.DatabaseConnection import DatabaseConnection
from pymongo import errors

'''
This function is only used to move all the observations stored in analysis/json/* into a local MongoDB.
This script must be launched inside "backend" docker container
Make sure to give through Docker Desktop max RAM and CPU available
'''

def list_and_sort_json_files(directory):
    
    json_files = [f for f in os.listdir(directory) if f.endswith('.json')]

    # Define a custom key function for sorting based on the date and time information
    def key_function(file_name):
        # Extract the date and time components from the file name
        parts = file_name.split('-')
        parts[-1] = parts[-1].split('.')[0]
        date_str = '-'.join(parts[1:4])
        time_str = '-'.join(parts[4:])

        # Convert the date and time components to a datetime object
        file_datetime = datetime.strptime(f"{date_str}-{time_str}", "%d-%m-%Y-%H-%M")

        return file_datetime

    # Sort the list of files based on the custom key function
    sorted_json_files = sorted(json_files, key=key_function)
    #index = sorted_json_files.index('data-23-06-2023-08-02.json')

    #sorted_json_files = [sorted_json_files[0]]
    

    #return sorted_json_files[index+1:]
    return sorted_json_files


def try_insert_many(coin_obs, db_tracker, coin):
    try:
        db_tracker[coin].insert_many(coin_obs)
    except errors.BulkWriteError as e:
        print(f'Duplicate key: {e} - {coin}')
        # Handle the exception, print the details, or perform other error handling
        for obs in coin_obs:
            try:
                db_tracker[coin].insert_one(obs)
            except:
                print(f'Skipping: {obs} - {coin}')
                continue

def main():

    db = DatabaseConnection()
    db_backend = db.get_db(database=DATABASE_MARKET)
    db_tracker = db.get_db(database=DATABASE_TRACKER)
    db_benchmark = db.get_db(database=DATABASE_BENCHMARK)

    json_dir = '/analysis/json_tracker/'
    sorted_files = list_and_sort_json_files(json_dir)
    print(sorted_files)

    for file_path in sorted_files:
        
        print(f'Loading {file_path}')
        full_path = json_dir + file_path
        with open(full_path, 'r') as file:
            json_data = json.load(file)
        print(f'{file_path} loaded')
        
        filter_fields = ['price', 'vol_1m', 'buy_vol_1m', 'vol_5m', 'buy_vol_5m', 'vol_15m', 'buy_vol_15m', 'vol_30m', 'buy_vol_30m',
                          'vol_60m', 'buy_vol_60m', 'vol_3h', 'buy_vol_3h', 'vol_6h', 'buy_vol_6h', 'vol_24h', 'buy_vol_24h']
        
        n_coins = len(json_data['data'])
        for coin, i in zip(json_data['data'], range(n_coins)):
            last_doc_saved = db_tracker[coin].find_one(sort=[("_id", -1)])
            if last_doc_saved != None:
                last_timestamp_saved = last_doc_saved['_id']
            else:
                last_timestamp_saved = datetime(1970,1,1).isoformat()

            coin_obs = []
            for obs in json_data['data'][coin]:
                
                if datetime.fromisoformat(last_timestamp_saved) > datetime.fromisoformat(obs['_id']):
                    print(f'Data are already saved for coin {coin}')
                    continue

                new_obs = {'_id': obs['_id']}
                for key in filter_fields:
                    new_obs[key] = obs[key]
                
                coin_obs.append(new_obs)

            if len(coin_obs) > 0:
                try_insert_many(coin_obs, db_tracker, coin)
                
            print(f'{i}/{n_coins} - {file_path} - {coin}')

                
        del json_data
        #sleep(10)
        





if __name__ == "__main__":
    logger = LoggingController.start_logging()
    logger.info('Pre Loading DB Started')
    main()
