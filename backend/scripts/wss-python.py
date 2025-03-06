import os,sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import websocket
import json
from datetime import datetime, timedelta
import os
from time import sleep
from constants.constants import *
from app.Helpers.Helpers import round_, timer_func, discard_coin_list
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
import logging
import threading
import sys


LIST = sys.argv[1]
LIST_N = int(LIST[-1])
TIME = "T"
PRICE = "p"
QUANTITY = "q"
ORDER = "m"
METHOD = 'aggTrades'
LOST_CONNECTION = []
MAX_CONNECTION_LOST = int(os.getenv('MAX_CONNECTION_LOST')) #2
CHECK_PERIOD_MINUTES = int(os.getenv('CHECK_PERIOD_MINUTES')) #60
SLEEP_LAST_LIST = int(os.getenv('SLEEP_LAST_LIST')) #60



def on_error(ws, error):
    global prices
    global doc_db
    error = str(error)

    if 'Connection to remote host was lost' in error:
        ws.close()
        ts = datetime.now().isoformat()
        LOST_CONNECTION.append(ts)
        discard_ts = []
        conn_lost = 0
        for ts in LOST_CONNECTION:
            if datetime.fromisoformat(ts) > datetime.now() - timedelta(minutes=CHECK_PERIOD_MINUTES):
                conn_lost += 1
            else:
                discard_ts.append(ts)
        logger.error(f'{conn_lost}th LOST CONNECTION for {LIST}')

        for ts in discard_ts:
            LOST_CONNECTION.remove(ts)
        if conn_lost >= MAX_CONNECTION_LOST:
            msg = f'SLEEP {LIST} for {SLEEP_LAST_LIST} minutes'
            logger.info(msg)
            sleep(60*SLEEP_LAST_LIST)
    

    
    # condition added because of bug error on 21/01/2025, the thread was stuck on "sell_volume" error...
    #  something wrong happened in variable initialization and this condition helps restarting the script in case of this error.
    if 'sell_volume' in error:
        logger.info(f'{LIST}: sell_volume error')
        ws.close()
        
        


def on_close(*args):
    
    if datetime.now().hour == 0 and datetime.now().minute == 0:
        sleep(LIST_N)

    threading.Thread(target=restart_connection).start()
    ws.run_forever()

def restart_connection():
    '''
    compute total seconds until next wss restart.
    This function restarts the wss connection every 24 hours
    '''
    logger.info(f'{LIST}: ws_close')
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
    #wss_restart_timestamp = (now + timedelta(seconds=total_remaining_seconds)).isoformat()
    #logger.info(f'on_restart_connection: Next wss restart {wss_restart_timestamp} for {LIST}')
    
    
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
    datetimenow = datetime.now()
    #datetimenow = datetime(2025,3,2)
    yesterday_date = (datetimenow - timedelta(days=1)).strftime("%Y-%m-%d")

    collection_list = db_benchmark.list_collection_names()
    summary = {}
    coins_no_longer_traded = {}
    for coin in collection_list:
        if coin in discard_coin_list():
            continue
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
        if 'volume_series' in volume_benchmark_coin_doc:
            volume_series = volume_benchmark_coin_doc['volume_series']
            dates_volume_series = list(volume_series.keys())
            # Convert date strings to datetime objects
            date_objects = [datetime.strptime(date_str, "%Y-%m-%d") for date_str in dates_volume_series]
            # Sort the datetime objects
            date_objects.sort()
            # Convert the sorted datetime objects back to strings
            sorted_date_strings = [date_obj.strftime("%Y-%m-%d") for date_obj in date_objects]
            last_date = sorted_date_strings[-1]
            if last_date == yesterday_date:
                summary[coin] = {"volume_30_avg": volume_30_avg, "elegible_for_deletion": elegible_for_deletion}
            else:
                coins_no_longer_traded[coin] = {"volume_30_avg": volume_30_avg, "elegible_for_deletion": elegible_for_deletion}

    n_summary = len(summary)

    
    discarded_volumes = list(coins_no_longer_traded.values())
    all_volumes = list(summary.values())
    sorted_discarded_volumes = sorted(discarded_volumes, key=lambda x: x['volume_30_avg'], reverse=True)
    sorted_volumes = sorted(all_volumes, key=lambda x: x['volume_30_avg'], reverse=True)
    #logger.info(sorted_volumes)
    #threshold_volume = sorted_volumes[position_threshold]
    
    most_traded_coins_first_filter = {}
    coins_discarded = 0
    coins_brought_back = []
    
    for coin in most_traded_coins:
        if coin in discard_coin_list():
            continue
        # these coins are most likely not traded, but there some that are new entry, I need to list them all
        if coin not in summary and coin not in coins_no_longer_traded:
            most_traded_coins_first_filter[coin] = {"position":None}
        elif coin in summary:
            position = sorted_volumes.index(summary[coin]) + 1
            # if coin not in summary, it is a new entry
            # if position below threshold, perfect
            # if coin does not have at least 30 obs, it is not eligible for deletion
            if position > position_threshold and summary[coin]['elegible_for_deletion']:
                coins_discarded += 1
                continue
            else:
                most_traded_coins_first_filter[coin] = {"position":position}
        else:
            position = sorted_discarded_volumes.index(coins_no_longer_traded[coin]) + 1 + n_summary
            if position > position_threshold and coins_no_longer_traded[coin]['elegible_for_deletion']:
                coins_discarded += 1
                continue
            else:
                coins_brought_back.append(coin)
                most_traded_coins_first_filter[coin] = {"position":position}

    most_traded_coins_first_filter = sort_object_by_position(most_traded_coins_first_filter)
    #logger.info(most_traded_coins_first_filter)

    #logger.info(f'coins discarded: {coins_discarded} for list: {LIST}')

    coin_list = {}
    for i in range(1,SETS_WSS_BACKEND+1):
        coin_list[f'list{str(i)}'] = []

    best_coins = [tuple_[0] for tuple_ in most_traded_coins_first_filter[:SETS_WSS_BACKEND]]
    
    coins_in_none_position = []
    for tuple_ in most_traded_coins_first_filter:
        coin = tuple_[0]
        position = tuple_[1]['position']
        if coin in best_coins:
            position_best_coin = best_coins.index(coin) + 1
            if position_best_coin == legend_list[LIST]['best_coin']:
                coin_list[LIST].append(coin)
            continue
        if position != None:
            #logger.info(f'{coin} - {position}')
            for list_name in legend_list:
                if position % SETS_WSS_BACKEND == legend_list[list_name]['position']:
                    coin_list[list_name].append(coin)
        else:
            coins_in_none_position.append(coin)
    n_coins_benchmark = len(coin_list[LIST])
    
    
    coins_in_none_position = sorted(coins_in_none_position)
    for coin in coins_in_none_position:
        position = coins_in_none_position.index(coin)
        for list_name in legend_list:
                if position % SETS_WSS_BACKEND == legend_list[list_name]['position']:
                    coin_list[list_name].append(coin)
    
    n_coins_binance = len(coin_list[LIST]) - n_coins_benchmark
    logger.info(f'{LIST} - coins from benchmark + coins from binance: {n_coins_benchmark} + {n_coins_binance}' )

    # Convert lists to sets
    all_lists = []
    for list_name in coin_list:
        all_lists.append(coin_list[list_name])
    
    common_pairs = {}
    # Iterate through each pair of lists
    for i in range(len(all_lists)):
        for j in range(i + 1, len(all_lists)):
            # Find common elements between the pair of lists
            common = set(all_lists[i]).intersection(all_lists[j])
            # Store the result in the dictionary
            common_pairs[(i, j)] = common
        # Check for common elements across all three sets
    
    for pair, common in common_pairs.items():
        if len(common) != 0:
            logger.info(f"Common elements between list{pair[0] + 1} and list{pair[1] + 1}: {common}")
    
    coin_list = coin_list[LIST]
    if datetime.now().hour == 0 and datetime.now().minute == 0:
        logger.info(f'{LIST}: {coin_list}')
        if LIST == f'list{str(SETS_WSS_BACKEND)}':
            n_coins_back = position_threshold - n_summary
            n_discarded_coins = len(coins_no_longer_traded)
            n_coins_traded = len(most_traded_coins_first_filter)
            n_coins_brought_back = len(coins_brought_back)
            logger.info('')
            logger.info('##### BACKEND INFO #####')
            logger.info(f'Number of coins traded in the last day FROM db_Bencharmk: {n_summary}')
            logger.info(f'Number of coins that have not been traded in the last day FROM db_Benchmark: {n_discarded_coins}')
            logger.info(f'Number of coins that are going to be brought back: {n_coins_back}')
            logger.info(f'Number of coins that are  brought back: {n_coins_brought_back}')
            logger.info(f'Coins List Brought Back: {coins_brought_back}')
            logger.info(f'Total Number of Coins traded: {n_coins_traded}')
            logger.info('##### BACKEND INFO #####')
            logger.info('')
    return coin_list



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

    global prices
    global doc_db
    global coin_list

    # msg = 'WSS CONNECTION STARTED'
    # logger.info(msg)
    # db_logger[DATABASE_API_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})

    # now = datetime.now().isoformat()
    # msg = f'{LIST}: on_open: Wss Started: {now}'
    # logger.info(msg)

    coin_list = select_coins(LIST, db_benchmark, position_threshold)
    #doc_db, prices = initializeVariables(coin_list)
    

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
    global now_ts
    
    data = json.loads(message)

    # LOCAL TEST
    # if LIST == 'list1' and datetime.now().minute == 48 and datetime.now().second == 57:
    #     raise ValueError("Connection to remote host was lost")
    # if LIST == 'list1' and datetime.now().minute == 48 and datetime.now().second == 59:
    #     logger.info(doc_db)

    #get data and symbol
    # if 'ping' in data:
    #     logger.info(f'ping: {data}')
    # if 'data' not in data or 'stream' not in data:
    #     logger.info(f'data: {data}')
    if 'data' in data:
        data = data['data']
        instrument_name = data['s']

        if now_ts is not None:
            # if same minute then keep on getting statistics
            if now_ts == datetime.now().strftime("%Y-%m-%d:%H:%M"):
                getStatisticsOnTrades(data, instrument_name, doc_db, prices)
            # otherwise let's save it to db
            else:
                #logger.info(f'{n_coins}/{tot_coins} coins have been traded in the last minute. {LIST}')
                now_ts = datetime.now().strftime("%Y-%m-%d:%H:%M")
                saveTrades_toDB(prices, doc_db, database)
                doc_db, prices = initializeVariables(coin_list)
        else:
            sleep(60 - datetime.now().second + 1)
            now_ts = datetime.now().strftime("%Y-%m-%d:%H:%M")
            doc_db, prices = initializeVariables(coin_list)

    
    

def initializeVariables(coin_list):
    
    doc_db = {}
    prices = {}
    for instrument_name in coin_list:
        doc_db[instrument_name] = {"_id": None, "price": None, "n_trades": 0, "buy_n": 0, "volume": 0 , "buy_volume": 0, "sell_volume": 0}
        prices[instrument_name] = None

    return doc_db, prices


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

        
#@timer_func
def saveTrades_toDB(prices, doc_db, database):
    path = legend_list[LIST]['path']
        
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
    

if __name__ == "__main__":
    client = DatabaseConnection()
    SETS_WSS_BACKEND = int(os.getenv('SETS_WSS_BACKEND'))
    db_logger = client.get_db(DATABASE_LOGGING)
    database = client.get_db(DATABASE_MARKET)
    db_benchmark = client.get_db(DATABASE_BENCHMARK)
    logger = LoggingController.start_logging()
    position_threshold = int(os.getenv('COINS_TRADED'))
    doc_db = None
    prices = None
    now_ts = None

    legend_list = {}
    x = [i for i in range(1,SETS_WSS_BACKEND+1)]
    for i in range(1,SETS_WSS_BACKEND+1):
        list_name = f'list{i}'
        legend_list[list_name] = {'position': i % SETS_WSS_BACKEND,
                                  'best_coin': x[-i],
                                  'path': f'/backend/info/prices{i}.json'}
    
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/stream?streams=",
                              on_open = on_open,
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close
                              )
    threading.Thread(target=restart_connection).start()
    ws.run_forever()



