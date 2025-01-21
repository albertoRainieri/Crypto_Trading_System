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
    
    if 'sell_volume' in error:
        threading.Thread(target=restart_connection).start()


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

def select_coins(LIST, db_benchmark, position_threshold):
    f = open ('/backend/json/most_traded_coins.json', "r")
    data = json.loads(f.read())
    #coin_list = data["most_traded_coins"][:NUMBER_COINS_TO_TRADE_WSS]
    # I AM MOVING ETHUSDT TO SECOND LIST BECAUSE I NOTED THAT SOMETIMES TRADE VOLUME IN LIST 2 IS VERY LOW AND THEREFORE THE SAVE TO DB HAPPENS TOO LATE,.
    # THIS IS RISKY FOR THE TRACKER WHICH IS NOT ABLE TO GET THE LAST DATA
    most_traded_coins = data["most_traded_coins"]

    collection_list = db_benchmark.list_collection_names()
    summary = {}
    for coin in collection_list:
        volume_benchmark_coin_doc = db_benchmark[coin].find_one()
        volume_30_avg = volume_benchmark_coin_doc['volume_30_avg']
        # if the coin has at least 30 days of analysis, it is eligible for deletion
        #logger.info(coin)
        if 'Last_30_Trades' not in volume_benchmark_coin_doc:
            elegible_for_deletion == False
        elif len(volume_benchmark_coin_doc['Last_30_Trades']['list_last_30_trades']) == 30:
            elegible_for_deletion = True
        else:
            elegible_for_deletion = False
        
        summary[coin] = {"volume_30_avg": volume_30_avg, "elegible_for_deletion": elegible_for_deletion}
    
    
    all_volumes = list(summary.values())
    #logger.info(all_volumes)
    sorted_volumes = sorted(all_volumes, key=lambda x: x['volume_30_avg'], reverse=True)
    #logger.info(sorted_volumes)
    #threshold_volume = sorted_volumes[position_threshold]
    
    most_traded_coins_first_filter = {}
    coins_discarded = 0
    
    for coin in most_traded_coins:
        
        # these coins are most likely not traded, but there some that are new entry, I need to list them all
        if coin not in summary:
            most_traded_coins_first_filter[coin] = {"position":None}
        else:
            position = sorted_volumes.index(summary[coin]) + 1
            # if coin not in summary, it is a new entry
            # if position below threshold, perfect
            # if coin does not have at least 30 obs, it is not eligible for deletion
            if position > position_threshold and summary[coin]['elegible_for_deletion']:
                coins_discarded += 1
                continue
            else:
                most_traded_coins_first_filter[coin] = {"position":position}
                # if LIST == 'list1' and position % 2 == 1:
                #     coin_list.append(coin)
                # elif LIST == 'list2' and position % 2 == 0:
                #     coin_list.append(coin)
    

    most_traded_coins_first_filter = sort_object_by_position(most_traded_coins_first_filter)

    
    #logger.info(most_traded_coins_first_filter)
    logger.info(f'coins discarded: {coins_discarded} for list: {LIST}')

    # the coin list of list1 is made of all coins in odd position, on the contrary list2 is made of all coins in even position
    # for balancing the volume, I put BTCUSDT in list2
    coin_list = []
    if LIST == 'list1':
        coin_list = ['ETHUSDT']
    else:
        coin_list = ['BTCUSDT']
    
    coins_in_none_position = []
    for tuple_ in most_traded_coins_first_filter:
        coin = tuple_[0]
        position = tuple_[1]['position']
        if coin in ['BTCUSDT', 'ETHUSDT']:
            continue
        if position != None:
            if LIST == 'list1' and position % 2 == 1:
                coin_list.append(coin)
            elif LIST == 'list2' and position % 2 == 0:
                coin_list.append(coin)
        else:
            coins_in_none_position.append(coin)
    
    n_coins = len(coin_list)
    logger.info(f'list: {LIST} - n_coins already traded in the app: {n_coins} ' )
    
    coins_in_none_position = sorted(coins_in_none_position)
    for coin in coins_in_none_position:
        position = coins_in_none_position.index(coin)
        if LIST == 'list1' and position % 2 == 0:
            coin_list.append(coin)
        elif LIST == 'list2' and position % 2 == 1:
            coin_list.append(coin)
    
    n_coins = len(coin_list)
    logger.info(f'list: {LIST} - total n_coins: {n_coins}' )
    #logger.info(coin_list)
    return coin_list


    # len_coins_list = len(coin_list)
    # logger.info(coin_list)
    # logger.info(f'Trading {len_coins_list} coins for {LIST}')
    # return coin_list


def sort_object_by_position(obj):
  """
  Sorts the child objects of a dictionary based on their 'position' key.

  Args:
    obj: A dictionary where child objects have a 'position' key.

  Returns:
    A list of tuples, where each tuple contains the key and its corresponding 
    child object, sorted by the 'position' key. 
    Child objects with 'position' equal to None are placed last.
  """

  def sort_key(item):
    position = item[1].get('position')
    return float('inf') if position is None else position

  return sorted(obj.items(), key=sort_key)         


    


    

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

    coin_list = select_coins(LIST, db_benchmark, position_threshold)

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
    db_benchmark = client.get_db(DATABASE_BENCHMARK)
    logger = LoggingController.start_logging()
    position_threshold = 299
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/stream?streams=",
                              on_open = on_open,
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close
                              )
    threading.Thread(target=restart_connection).start()
    ws.run_forever()


