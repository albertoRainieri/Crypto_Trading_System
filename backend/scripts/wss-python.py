import os,sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import websocket
import json
from datetime import datetime, timedelta
import os
from time import sleep
from constants.constants import *
from app.Helpers.Helpers import round_, timer_func
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
import logging
import threading


TIME = "T"
PRICE = "p"
QUANTITY = "q"
ORDER = "m"
METHOD = 'aggTrades'



def on_error(ws, error):
    error = str(error)
    logger.error(f'error is: {error}')
    #db_logger[DATABASE_API_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': error})
    

    if error == "\'data\'":
        second = datetime.now().second
        remaining_second = 60 - second + 1
        start_wss = datetime.now() + timedelta(seconds= remaining_second)
        logger.info(f'Starting collecting data at: {start_wss}')
        sleep(remaining_second)
    
    db_logger[DATABASE_API_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': error})

def on_close(*args):
    del os.environ['NOW']
    sleep(1)

    threading.Thread(target=close_connection).start()
    ws.run_forever()

def close_connection():
    '''
    compute total seconds until next wss restart.
    This function restarts the wss connection every 12 hours
    '''

    now = datetime.now()
    current_hour = now.hour
    current_minute = now.minute
    # if it is currently midnight --> current_hour is 0
    # if current_hour < 12:
    #     HOURS = 12 - current_hour
    # # if it is currently midday --> current_hour is 12
    # else:
    #     HOURS = 24 - current_hour
    HOURS = 24 - current_hour

    remaining_seconds = 60 - now.second + 1
    minutes_remaining = 59 - current_minute
    hours_remaining = HOURS - 1

    # total seconds until next wss restart
    total_remaining_seconds = remaining_seconds + minutes_remaining*60 + hours_remaining*60*60
    
    wss_restart_timestamp = (now + timedelta(seconds=total_remaining_seconds)).isoformat()
    logger.info(f'Next wss restart {wss_restart_timestamp}')
        
    sleep(total_remaining_seconds)
    msg = "RESTARTING WSS CONNECTION"
    db_logger[DATABASE_API_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
    logger.info('RESTARTING WSS CONNECTION')
    ws.close()
    

def on_open(ws):
    # Combined streams arec acessed at /stream?streams=<streamName1>/<streamName2>/<streamName3>
    if 'NOW' in os.environ:
        del os.environ['NOW']

    global prices
    global doc_db
    global coin_list

    msg = 'WSS CONNECTION STARTED'
    logger.info(msg)
    db_logger[DATABASE_API_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})


    f = open ('/backend/json/most_traded_coins.json', "r")
    data = json.loads(f.read())
    coin_list = data["most_traded_coins"][:300]
    doc_db, prices = initializeVariables(coin_list)
    

    parameters = []
    for coin in coin_list:
        parameters.append(coin.lower() + "@aggTrade")

    print("### Opened ###")
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": parameters,
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))

def on_message(ws, message):
    global prices
    global doc_db
    global coin_list
    
    data = json.loads(message)

    #get data and symbol
    
    data = data['data']
    instrument_name = data['s']

    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d:%H:%M")
    NOW = os.getenv('NOW')


    # if data['s'] == 'BTCUSDT':
    #     print(data)
    if NOW is not None:
        if NOW == formatted_date:
            getStatisticsOnTrades(data, instrument_name, doc_db, prices)
        else:
            os.environ['NOW'] = formatted_date
            saveTrades_toDB(prices, doc_db, database)
            doc_db, prices = initializeVariables(coin_list)
    else:
        os.environ['NOW'] = formatted_date
        doc_db, prices = initializeVariables(coin_list)

    
    

def initializeVariables(coin_list):
    
    doc_db = {}
    prices = {}
    for instrument_name in coin_list:
        doc_db[instrument_name] = {"_id": None, "price": None, "n_trades": 0,"volume": 0 , "buy_volume": 0, "sell_volume": 0, "buy_n": 0, "sell_n": 0, "quantity": 0}
        prices[instrument_name] = None
        

    return doc_db, prices


def getStatisticsOnTrades(trade, instrument_name, doc_db, prices):
    try:
        datetime_trade = datetime.fromtimestamp(trade[TIME]/1000)
    except:
        pass
    
    doc_db[instrument_name]['n_trades'] += 1

    if trade[ORDER]:
        order = 'SELL'
    else:
        order = 'BUY'

    quantity  = trade[QUANTITY]
    price =  trade[PRICE]


    prices[instrument_name] = float(trade[PRICE])

    doc_db[instrument_name]["quantity"] += float(quantity)
    
    if order == "BUY":
        doc_db[instrument_name]["buy_n"] += 1
        doc_db[instrument_name]["buy_volume"] += float(quantity) * float(price)
    else:
        doc_db[instrument_name]["sell_n"] += 1
        doc_db[instrument_name]["sell_volume"] += float(quantity) * float(price)

        
@timer_func
def saveTrades_toDB(prices, doc_db, database):
    for instrument_name in prices:
        if doc_db[instrument_name]['n_trades'] != 0:
            #doc_db[instrument_name]["n_trades_p_s"]= round_(np.mean(n_trades_p_s_dict[instrument_name]),2)
            doc_db[instrument_name]["price"]=prices[instrument_name]
            doc_db[instrument_name]["quantity"] = round_(doc_db[instrument_name]["quantity"],2)
            doc_db[instrument_name]["volume"] =  round_(doc_db[instrument_name]["buy_volume"] + doc_db[instrument_name]["sell_volume"],2)
            doc_db[instrument_name]["sell_volume"] = round(doc_db[instrument_name]["sell_volume"],2)
            doc_db[instrument_name]["buy_volume"] = round(doc_db[instrument_name]["buy_volume"],2)
            doc_db[instrument_name]['_id']= datetime.now().isoformat()
            database[instrument_name].insert_one(doc_db[instrument_name])
            #print(doc_db)

def get_db(db_name):
    '''
    Establish connectivity with db
    '''
    database = DatabaseConnection()
    db = database.get_db(db_name)
    return db

if __name__ == "__main__":
    db_logger = get_db(DATABASE_LOGGING)
    database = get_db(DATABASE_MARKET)
    logger = LoggingController.start_logging()
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/stream?streams=",
                              on_open = on_open,
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close
                              )
    threading.Thread(target=close_connection).start()
    ws.run_forever()


