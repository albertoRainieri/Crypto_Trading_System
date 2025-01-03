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

    if LIST == 'list2-extra':
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
    # if LIST == 'list1-extra':
    #     coin_list = data["most_traded_coins_extra"][:300]
    #     if 'ETHBTC' in coin_list:
    #         coin_list.remove('ETHBTC')
    #     if 'SOLBTC' not in coin_list:
    #         coin_list = ['SOLBTC'] + coin_list
        
    # else:
    #     coin_list = data["most_traded_coins_extra"][300:]
    #     if 'ETHBTC' not in coin_list:
    #         coin_list = ['ETHBTC'] + coin_list
    #     if 'SOLBTC' in coin_list:
    #         coin_list.remove('SOLBTC')

    most_traded_coins = data['most_traded_coins']
    unique_coins = [coin[:-4] for coin in most_traded_coins]
    most_traded_coins_extra = sorted(data['most_traded_coins_extra'])
    n_extra_pairs=int(len(most_traded_coins_extra)/2)
    if LIST == 'list1-extra':
        coin_list = most_traded_coins_extra[:n_extra_pairs]
        coin_list2 = most_traded_coins_extra[n_extra_pairs:]
    else:
        coin_list = most_traded_coins_extra[n_extra_pairs:]
        coin_list2 = most_traded_coins_extra[:n_extra_pairs]

    
    STOP=True
    while STOP:
        if LIST == 'list1-extra':
            if coin_list[-1][:-3] != coin_list2[0][:-3]:
                STOP=False
            else:
                logger.info(f'Adjusting the list for {LIST}')
                coin_list.append(coin_list2[0])
                coin_list2.remove(coin_list2[0])
        else:
            if coin_list[0][:-3] != coin_list2[-1][:-3]:
                STOP=False
            else:
                logger.info(f'Adjusting the list for {LIST}')
                coin_list2.append(coin_list[0])
                coin_list.remove(coin_list[0])
    
    coin_list.append('BTCUSDT')
    coin_list.append('ETHUSDT')

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
    instrument_name_usdt = instrument_name[:-3] + 'USDT'
    if instrument_name_usdt not in n_list_coins:
        n_list_coins.append(instrument_name_usdt)

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
    if LIST == 'list1-extra':
        path = '/backend/info/prices1-extra.json'
    else:
        path = '/backend/info/prices2-extra.json'

    if os.path.exists(path):
        with open(path, 'r') as file:
            prices = json.load(file)
    else:
        prices = {}

    for instrument_name in coin_list:
        if instrument_name != 'BTCUSDT' and instrument_name != 'ETHUSDT':
            instrument_name_usdt = instrument_name[:-3] + 'USDT'
        else:
            instrument_name_usdt = instrument_name

        if instrument_name_usdt not in doc_db:
            doc_db[instrument_name_usdt] = {"_id": None, "price": None, "n_trades": 0, "buy_n": 0, "volume": 0 , "buy_volume": 0, "sell_volume": 0}
            if not os.path.exists(path):
                prices[instrument_name_usdt] = None
    n_list_coins = []

    return doc_db, prices, n_list_coins


def getStatisticsOnTrades(trade, instrument_name, doc_db, prices):

    if trade[ORDER]:
        order = 'SELL'
    else:
        order = 'BUY'

    quantity  = trade[QUANTITY]
    price =  trade[PRICE]

    if price != 0 and quantity != 0:
        if instrument_name[-3:] == 'BTC':
            adjuster = 'BTCUSDT'
        elif instrument_name[-3:] == 'ETH':
            adjuster = 'ETHUSDT'


        if instrument_name != 'BTCUSDT' and instrument_name != 'ETHUSDT':
            instrument_name_usdt = instrument_name[:-3] + 'USDT'
            doc_db[instrument_name_usdt]['n_trades'] += 1
            prices[instrument_name_usdt] = float(price) * prices[adjuster]


            #doc_db[instrument_name_usdt]["quantity"] += float(quantity)
            
            if order == "BUY":
                doc_db[instrument_name_usdt]["buy_n"] += 1
                doc_db[instrument_name_usdt]["buy_volume"] += float(quantity) * float(price) * prices[adjuster]
            else:
                doc_db[instrument_name_usdt]["sell_volume"] += float(quantity) * float(price) * prices[adjuster]
        elif instrument_name == 'BTCUSDT':
            prices[instrument_name] = float(price)
        elif instrument_name == 'ETHUSDT':
            prices[instrument_name] = float(price)
    else:
        logger.info("Duplicate Aggregate Trade")

        
@timer_func
def saveTrades_toDB(prices, doc_db, database):

    # read last prices if path exists already
    if LIST == 'list1-extra':
        path = '/backend/info/prices1-extra.json'
    else:
        path = '/backend/info/prices2-extra.json'
        
    if os.path.exists(path):
        f = open (path, "r")
        last_prices = json.loads(f.read())
    else:
        last_prices = {}
        for instrument_name in prices:
            last_prices[instrument_name] = None

    for instrument_name in prices:
        if instrument_name != 'BTCUSDT' and instrument_name != 'ETHUSDT':
            if doc_db[instrument_name]['n_trades'] != 0:
                last_prices[instrument_name] = prices[instrument_name]
                if prices[instrument_name] > 0.1:
                    doc_db[instrument_name]["price"] = round_(prices[instrument_name],3)
                else:
                    doc_db[instrument_name]["price"] = round_(last_prices[instrument_name],count_zeros_after_dot(last_prices[instrument_name])+3)
                #doc_db[instrument_name]["quantity"] = round_(doc_db[instrument_name]["quantity"],2)
                doc_db[instrument_name]["volume"] =  int(doc_db[instrument_name]["buy_volume"] + doc_db[instrument_name]["sell_volume"])
                #doc_db[instrument_name]["sell_volume"] = round_(doc_db[instrument_name]["sell_volume"],2)

                doc_db[instrument_name]["buy_volume"] = int(doc_db[instrument_name]["buy_volume"])
                doc_db[instrument_name]['_id']= datetime.now().isoformat()

                doc_to_save = doc_db[instrument_name].copy()
                del doc_to_save["sell_volume"]

                database[instrument_name].insert_one(doc_to_save)
            else:
                if instrument_name in last_prices:
                    if prices[instrument_name] != None and prices[instrument_name] >= 0.1:
                        doc_db[instrument_name]["price"] = round_(last_prices[instrument_name],3)
                    elif prices[instrument_name] == None:
                        doc_db[instrument_name]["price"] = last_prices[instrument_name]
                    else:
                        doc_db[instrument_name]["price"] = round_(last_prices[instrument_name],count_zeros_after_dot(last_prices[instrument_name])+3)
                else:
                    continue

                if doc_db[instrument_name]["price"] == None:
                    continue

                #doc_db[instrument_name]["quantity"] = 0
                doc_db[instrument_name]["volume"] = 0
                #doc_db[instrument_name]["sell_volume"] = 0

                doc_db[instrument_name]["buy_volume"] = 0
                doc_db[instrument_name]['_id']= datetime.now().isoformat()

                doc_to_save = doc_db[instrument_name].copy()
                del doc_to_save["sell_volume"]

                database[instrument_name].insert_one(doc_to_save)
        else:
            last_prices[instrument_name] = prices[instrument_name]

    #logger.info('saving prices-file')
    with open(path, "w") as outfile_volume:
        outfile_volume.write(json.dumps(last_prices))
    #logger.info('saveTrades_toDB completed')
    
            #print(doc_db)

def get_db(db_name):
    '''
    Establish connectivity with db
    '''
    database = DatabaseConnection()
    db = database.get_db(db_name)
    return db

def count_zeros_after_dot(number):
    
    # Convert the number to a string
    num_str = str(number)

    # Find the index of the dot in the string
    dot_index = num_str.find('.')

    # If there is no dot or it's the last character, there are no zeros after the dot
    if dot_index == -1 or dot_index == len(num_str) - 1:
        return 0

    # Count the number of zeros after the dot
    zeros_count = 0
    for char in num_str[dot_index + 1:]:
        if char == '0':
            zeros_count += 1
        else:
            break  # Stop counting if a non-zero digit is encountered

    return zeros_count

if __name__ == "__main__":
    '''
    SCRIPT DEPRECATED 2025/01
    '''
    db_logger = get_db(DATABASE_LOGGING)
    database = get_db(DATABASE_MARKET_EXTRA)
    logger = LoggingController.start_logging()
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/stream?streams=",
                              on_open = on_open,
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close
                              )
    threading.Thread(target=restart_connection).start()
    ws.run_forever()


