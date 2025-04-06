from time import time
from app.Controller.LoggingController import LoggingController
import requests
from datetime import datetime, timedelta
import pytz
import json, os
import re
from constants.constants import *
from pymongo import DESCENDING

logger = LoggingController.start_logging()
    
def round_(number, decimal):
    return float(format(number, f".{decimal}f"))

def timer_func(func):
    # This function shows the execution time of 
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        logger.info(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func

def get_coins_list():
    dir_info = '/tracker/json'
    coins_list_path = f'{dir_info}/coins_list.json'
    f = open (coins_list_path, "r")
    coins_list = json.loads(f.read())
    return coins_list

def discard_coin_list():
    return ['GBUSDT', 'FDUSDUSDT', 'EURUSDT', 'BTCUSDT', 'ETHUSDT']

def get_best_coins(volume_standings):
    SETS_WSS_BACKEND = int(os.getenv('SETS_WSS_BACKEND'))
    best_x_coins = list(volume_standings['standings'].keys())[:SETS_WSS_BACKEND]
    logger.info(f'Best {SETS_WSS_BACKEND} coins: {best_x_coins}')
    return best_x_coins


def get_volume_standings(db_volume_standings):
    today_date = datetime.now().strftime("%Y-%m-%d")
    volume_standings = db_volume_standings[COLLECTION_VOLUME_STANDINGS].find().sort([('_id', DESCENDING)]).limit(1).next()
    
    if volume_standings['_id'] != today_date:
        logger.info(f"WARNING: LAST DOC for  DB_VOLUME_STANDINGS is {volume_standings['_id']}")

    return volume_standings

def getsubstring_fromkey(text):
    '''
    This simple function returns the substrings for volume, buy_volume and timeframe from "key". 
    "key" is the label that defines an event. "key" is created in the function "wrap_analyze_events_multiprocessing"

    For example:
    from buy_vol_5m:0.65/vol_24h:8/timeframe:1440/vlty:1
    it returns:
    timeframe -> 1440
    buy_vol --> buy_vol_5m
    vol --> vol_24h
    buy_vol_value --> 0.65
    vol_value --> 8
    lvl --> if exists
    '''
    match = re.search(r'vol_(\d+m):(\d+(?:\.\d+)?)/vol_(\d+m):(\d+(?:\.\d+)?)/timeframe:(\d+)', text)
    if match:
        if 'lvl' in text:
            lvl = text.split('lvl:')[-1]
        else:
            lvl = None

        buy_vol = 'buy_vol_' + match.group(1)
        buy_vol_value = float(match.group(2))
        vol = 'vol_' + match.group(3)
        vol_value = int(match.group(4))
        timeframe = int(match.group(5))
    
    return vol, vol_value, buy_vol, buy_vol_value, timeframe, lvl

def get_benchmark_info(db_benchmark):
    '''
    this function queries the benchmark info from all the coins from the db on server
    '''
    now = datetime.now(tz=pytz.UTC) - timedelta(days=1)
    
    year = now.year
    month = now.month
    day = now.day
    file = 'benchmark-' + str(year) + '-' + str(month) + '-' + str(day) + '.json'
    full_path = '/analysis/benchmark_json/' + file

    if os.path.exists(full_path):
        print(f'Benchmark Info {full_path} exists.')

    else:
        print(f'{full_path} does not exist. Making the request to the server..')
        ENDPOINT = 'https://algocrypto.eu'
        METHOD = '/analysis/get-benchmarkinfo'

        url_mosttradedcoins = ENDPOINT + METHOD
        response = requests.get(url_mosttradedcoins)
        print(f'StatusCode for getting get-benchmarkinfo: {response.status_code}')
        benchmark_info = response.json()
        with open(full_path, 'w') as outfile:
            json.dump(benchmark_info, outfile)
        
        for coin in benchmark_info:
            db_benchmark[coin].insert_one(benchmark_info[coin])
        
