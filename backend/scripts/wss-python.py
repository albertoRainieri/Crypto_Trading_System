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
import sys


LIST = sys.argv[1]
TIME = "T"
PRICE = "p"
QUANTITY = "q"
ORDER = "m"
METHOD = 'aggTrades'



def on_error(ws, error):
    error = str(error)    

    if error == "\'data\'":
        msg = f'{LIST} Started'
        logger.info(msg)
        db_logger[DATABASE_API_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
        if datetime.now().second > 5:
            sleep(60 - datetime.now().second)
        
    else:
        logger.error(f'error is: {error}')
        db_logger[DATABASE_API_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': error})


def on_close(*args):
    if 'NOW' in os.environ:
        del os.environ['NOW']
    
        
    sleep(10)
    msg = f"on_close: Closing Wss Connection {LIST}"
    db_logger[DATABASE_API_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
    logger.info(msg)

    if LIST == 'list2':
        extra_seconds_list_2 = 60
        sleep(extra_seconds_list_2)

    threading.Thread(target=restart_connection).start()
    ws.run_forever()

def restart_connection():
    '''
    compute total seconds until next wss restart.
    This function restarts the wss connection every 24 hours
    '''

    now = datetime.now()
    current_hour = now.hour
    current_minute = now.minute

    # compute how many seconds until next restart
    HOURS = 24 - current_hour
    remaining_seconds = 60 - now.second + 1
    minutes_remaining = 59 - current_minute
    hours_remaining = HOURS - 1

    # total seconds until next wss restart
    total_remaining_seconds = remaining_seconds + minutes_remaining*60 + hours_remaining*60*60

    #ONLY FOR TESTING
    #total_remaining_seconds = 240 + remaining_seconds
    
    # define timestamp for next wss restart
    wss_restart_timestamp = (now + timedelta(seconds=total_remaining_seconds)).isoformat()
    logger.info(f'on_restart_connection: Next wss restart {wss_restart_timestamp} for {LIST}')
    
    
    sleep(total_remaining_seconds)
    #sleep(70)
    ws.close()
    

def on_open(ws):
    # Combined streams arec acessed at /stream?streams=<streamName1>/<streamName2>/<streamName3>
    if 'NOW' in os.environ:
        del os.environ['NOW']

    global prices
    global doc_db
    global coin_list
    global n_list_coins # this variable is used to count number of coins in the last mu

    # msg = 'WSS CONNECTION STARTED'
    # logger.info(msg)
    # db_logger[DATABASE_API_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})


    msg = f'on_open: Wss Started for {LIST}'
    logger.info(msg)

    f = open ('/backend/json/most_traded_coins.json', "r")
    data = json.loads(f.read())
    #coin_list = data["most_traded_coins"][:NUMBER_COINS_TO_TRADE_WSS]
    # I AM MOVING ETHUSDT TO SECOND LIST BECAUSE I NOTED THAT SOMETIMES TRADE VOLUME IN LIST 2 IS VERY LOW AND THEREFORE THE SAVE TO DB HAPPENS TOO LATE,.
    # THIS IS RISKY FOR THE TRACKER WHICH IS NOT ABLE TO GET THE LAST DATA
    if LIST == 'list1':
        coin_list = data["most_traded_coins"][:240]
        if 'ETHUSDT' in coin_list:
            coin_list.remove('ETHUSDT')
        if 'BTCUSDT' not in coin_list:
            coin_list = ['BTCUSDT'] + coin_list
        
    else:
        coin_list = data["most_traded_coins"][240:]
        if 'ETHUSDT' not in coin_list:
            coin_list = ['ETHUSDT'] + coin_list
        if 'BTCUSDT' in coin_list:
            coin_list.remove('BTCUSDT')

    logger.info(f'{LIST}: {coin_list}')
    doc_db, prices, n_list_coins = initializeVariables(coin_list)
    

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
    global n_list_coins
    
    data = json.loads(message)

    #get data and symbol
    
    data = data['data']
    instrument_name = data['s']
    if instrument_name not in n_list_coins:
        n_list_coins.append(instrument_name)

    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d:%H:%M")
    NOW = os.getenv('NOW')


    # if data['s'] == 'BTCUSDT':
    #     print(data)
    if NOW is not None:
        # if same minute then keep on getting statistics
        if NOW == formatted_date:
            getStatisticsOnTrades(data, instrument_name, doc_db, prices)
        # otherwise let's save it to db
        else:
            n_coins = len(n_list_coins)
            tot_coins = len(prices)
            logger.info(f'{n_coins}/{tot_coins} coins have been traded in the last minute. {LIST}')
            os.environ['NOW'] = formatted_date
            saveTrades_toDB(prices, doc_db, database)
            doc_db, prices, n_list_coins = initializeVariables(coin_list)
    else:
        os.environ['NOW'] = formatted_date
        doc_db, prices, n_list_coins = initializeVariables(coin_list)

    
    

def initializeVariables(coin_list):
    
    doc_db = {}
    prices = {}
    for instrument_name in coin_list:
        doc_db[instrument_name] = {"_id": None, "price": None, "n_trades": 0, "buy_n": 0, "volume": 0 , "buy_volume": 0, "sell_volume": 0}
        prices[instrument_name] = None
    n_list_coins = []

    return doc_db, prices, n_list_coins


def getStatisticsOnTrades(trade, instrument_name, doc_db, prices):

    doc_db[instrument_name]['n_trades'] += 1

    if trade[ORDER]:
        order = 'SELL'
    else:
        order = 'BUY'

    quantity  = trade[QUANTITY]
    price =  trade[PRICE]

    if price != 0 and quantity != 0:
        prices[instrument_name] = float(price)

        #doc_db[instrument_name]["quantity"] += float(quantity)
        
        if order == "BUY":
            doc_db[instrument_name]["buy_n"] += 1
            doc_db[instrument_name]["buy_volume"] += float(quantity) * float(price)
        else:
            #doc_db[instrument_name]["sell_n"] += 1
            doc_db[instrument_name]["sell_volume"] += float(quantity) * float(price)
    else:
        logger.info("Duplicate Aggregate Trade")

        
@timer_func
def saveTrades_toDB(prices, doc_db, database):

    # read last prices if path exists already
    if LIST == 'list1':
        path = '/backend/info/prices1.json'
    else:
        path = '/backend/info/prices2.json'
        
    if os.path.exists(path):
        f = open (path, "r")
        last_prices = json.loads(f.read())
    else:
        last_prices = {}
        for instrument_name in prices:
            last_prices[instrument_name] = None

    for instrument_name in prices:
        if doc_db[instrument_name]['n_trades'] != 0:
            last_prices[instrument_name] = prices[instrument_name]
            doc_db[instrument_name]["price"]=prices[instrument_name]
            #doc_db[instrument_name]["quantity"] = round_(doc_db[instrument_name]["quantity"],2)
            doc_db[instrument_name]["volume"] =  int(doc_db[instrument_name]["buy_volume"] + doc_db[instrument_name]["sell_volume"])
            #doc_db[instrument_name]["sell_volume"] = round_(doc_db[instrument_name]["sell_volume"],2)
            del doc_db[instrument_name]["sell_volume"]
            doc_db[instrument_name]["buy_volume"] = int(doc_db[instrument_name]["buy_volume"])
            doc_db[instrument_name]['_id']= datetime.now().isoformat()
            database[instrument_name].insert_one(doc_db[instrument_name])
        else:
            if instrument_name in last_prices:
                doc_db[instrument_name]["price"]=last_prices[instrument_name]
            else:
                continue

            if doc_db[instrument_name]["price"] == None:
                continue

            #doc_db[instrument_name]["quantity"] = 0
            doc_db[instrument_name]["volume"] = 0
            #doc_db[instrument_name]["sell_volume"] = 0
            del doc_db[instrument_name]["sell_volume"]
            doc_db[instrument_name]["buy_volume"] = 0
            doc_db[instrument_name]['_id']= datetime.now().isoformat()
            database[instrument_name].insert_one(doc_db[instrument_name])
    

    with open(path, "w") as outfile_volume:
        outfile_volume.write(json.dumps(last_prices))
    
            #print(doc_db)

if __name__ == "__main__":
    client = DatabaseConnection()
    db_logger = client.get_db(DATABASE_LOGGING)
    database = client.get_db(DATABASE_MARKET)
    logger = LoggingController.start_logging()
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/stream?streams=",
                              on_open = on_open,
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close
                              )
    threading.Thread(target=restart_connection).start()
    ws.run_forever()


