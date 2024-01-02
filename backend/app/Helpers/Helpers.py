from time import time
from app.Controller.LoggingController import LoggingController
import requests
from datetime import datetime, timedelta
import pytz
import json, os

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

def getsubstring_fromkey(key):
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
    '''
    # split the key
    key_split = key.split(':')

    # get buy_vol
    buy_vol = key_split[0]

    #get vol
    vol = key_split[1][key_split[1].index('/')+1:]

    # get buy_vol_value
    buy_vol_value = key.split(buy_vol + ':')[-1].split('/vol')[0]

    # get vol_value
    vol_value = key.split(vol + ':')[-1].split('/timeframe')[0]

    # get timeframe value
    # initializing substrings
    sub1 = "timeframe"
    sub2 = "/vlty"
    # getting index of substrings
    idx1 = key.index(sub1)

    # in case keys are grouped by volatility
    try:
        idx2 = key.index(sub2)
        timeframe = ''
        # getting elements in between
        for idx in range(idx1 + len(sub1) + 1, idx2):
            timeframe = timeframe + key[idx]
    # in case keys are NOT grouped by volatility
    except:
        timeframe = key.split('timeframe:')[-1]
        timeframe = timeframe[:-1]

    return vol, vol_value, buy_vol, buy_vol_value, timeframe

def get_benchmark_info(db_benchmark):
    '''
    this function queries the benchmark info from all the coins from the db on server
    '''
    now = datetime.now(tz=pytz.UTC) - timedelta(days=1)
    
    year = now.year
    month = now.month
    day = now.day
    file = 'benchmark-' + str(year) + '-' + str(month) + '-' + str(day)
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
        
