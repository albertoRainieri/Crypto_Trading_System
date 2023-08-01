from time import time
from app.Controller.LoggingController import LoggingController


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
    idx2 = key.index(sub2)
    timeframe = ''
    # getting elements in between
    for idx in range(idx1 + len(sub1) + 1, idx2):
        timeframe = timeframe + key[idx]

    return vol, vol_value, buy_vol, buy_vol_value, timeframe