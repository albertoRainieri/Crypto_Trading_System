import os,sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import json
from datetime import datetime, timedelta
from time import sleep
import re
import sys
import requests
from constants.constants import *
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from signal import SIGKILL
import random

def binance_order_book_request(coin):
    url = f"https://api.binance.com/api/v3/depth?symbol={coin}&limit=5000"
    try:
        response = requests.get(url=url)
    except:
        logger.info('Code Exception on requests.get api.binance.com/api/v3/depth')
        return None, None
    headers = response.headers
    status_code = response.status_code
    if status_code == 418:
        logger.info(headers)
        retry_after = int(headers['retry-after'])
        logger.info(f'ip banned, waiting {retry_after}')
        sleep(retry_after+1)
    return response.text, status_code

def round_(number, decimal):
    return float(format(number, f".{decimal}f"))

def get_statistics(resp):
    json_resp = json.loads(resp)
    total_ask_volume = 0
    total_bid_volume = 0

    # lowest_bid_price = json_resp['bids'][-1][0]
    # highest_ask_price = json_resp['asks'][-1][0]
    current_price = float(json_resp['bids'][0][0])

    for ask_order in json_resp['asks']:
        total_ask_volume += float(ask_order[0]) * float(ask_order[1])
    for bid_order in json_resp['bids']:
        total_bid_volume += float(bid_order[0]) * float(bid_order[1])

    delta = 0.01
    cumulative_ask_volume = 0
    cumulative_bid_volume = 0
    summary_ask_orders = []
    summary_bid_orders = []

    next_delta_threshold = 0 + delta
    for ask_order in json_resp['asks']:
        price_order = float(ask_order[0])
        quantity_order = float(ask_order[1])
        cumulative_ask_volume += price_order * quantity_order
        cumulative_ask_volume_ratio = round_((cumulative_ask_volume / total_ask_volume),2)
        if cumulative_ask_volume_ratio >= next_delta_threshold:
            summary_ask_orders.append((round_(( price_order -  current_price ) / current_price, 3), cumulative_ask_volume_ratio ))
            next_delta_threshold = cumulative_ask_volume_ratio + delta

    next_delta_threshold = 0 + delta
    for bid_order in json_resp['bids']:
        price_order = float(bid_order[0])
        quantity_order = float(bid_order[1])
        cumulative_bid_volume += price_order * quantity_order
        cumulative_bid_volume_ratio = round_((cumulative_bid_volume / total_bid_volume),2)
        if cumulative_bid_volume_ratio >= next_delta_threshold:
            summary_bid_orders.append((round_(( price_order -  current_price ) / current_price, 3), cumulative_bid_volume_ratio ))
            next_delta_threshold = cumulative_bid_volume_ratio + delta
            #print(f'{next_delta_threshold}: {bid_order}')

    return current_price, int(total_bid_volume), int(total_ask_volume), summary_bid_orders, summary_ask_orders,


def get_info_order_book(coin, logger):
    response, status_code = binance_order_book_request(coin)
    if status_code == 200:
        return get_statistics(response)
    else:
        return None

def extract_timeframe(input_string):
  """
  Extracts the timeframe value (e.g., 1440) from the given input string.

  Args:
    input_string: The input string containing the timeframe.

  Returns:
    The extracted timeframe value as a string, or None if no match is found.
  """
  match = re.search(r"timeframe:(\d+)", input_string)
  if match:
    return match.group(1)
  else:
    return None


def update_db_order_book_record(id, event_key, db_collection, order_book_info):


    new_data = []
    new_data.append(order_book_info[0]) #current_price 
    new_data.append(order_book_info[1]) #total_bid_volume
    new_data.append(order_book_info[2]) #total_ask_volume
    new_data.append(order_book_info[3]) #summary_bid_orders
    new_data.append(order_book_info[4]) #summary_ask_orders

    now = datetime.now().replace(microsecond=0).isoformat()
    filter_query = {"_id": id}
    update_doc = {"$set": {f"data.{now}": new_data}}
    result = db_collection.update_one(filter_query, update_doc)

    if result.modified_count != 1:
        now = datetime.now().isoformat()
        print(f"{now}: Order Book update failed for {event_key} with id {id}.") 

def get_current_number_of_orderbook_scripts(db, event_keys):
    live_order_book_scripts_number = 0
    for collection in event_keys:
        minutes_timeframe = int(extract_timeframe(collection))
        yesterday = datetime.now() - timedelta(minutes=minutes_timeframe)
        query = {"_id": {"$gt": yesterday.isoformat()}} 
        docs = list(db[collection].find(query,{'_id':1, 'coin': 1}))
        #logger.info(docs)
        #len_docs = len(docs)
        #logger.info(f'{len_docs} - {collection}')
        live_order_book_scripts_number += len(docs)
    
    return live_order_book_scripts_number

def get_sleep_seconds(live_order_book_scripts_number, number_script, second_binance_request, limit):
    '''
    Based on the number of live scripts, define the appropriate sleep time, for avoiding binance api ban
    No more than 20 polling order_book_api each minute
    '''
    
    # get the number of times the number of orderbookscripts is contained in the limit
    minute_api_trigger = live_order_book_scripts_number // limit + 1
    # based on the number script, assign the position of the starting minute (from 0,59)
    n_times_greater_than_limit = min(number_script // limit, minute_api_trigger -1)
    # define the minutes range, the script is allowed to poll binance api
    minute_range = [i for i in range(n_times_greater_than_limit,59, minute_api_trigger)]
    now = datetime.now()
    current_minute = now.minute
    for i in range(len(minute_range)):
        if i != len(minute_range) -1:
            if current_minute >= minute_range[i] and current_minute < minute_range[i+1]:
                # minutes remaining + seconds to 60 + second_binance_request
                sleep_seconds = (minute_range[i+1]- current_minute - 1)*60 + (60 - now.second) + second_binance_request
                return sleep_seconds
        else:
            sleep_seconds = (60 + minute_range[0]- current_minute - 1)*60 + (60 - now.second) + second_binance_request
            return sleep_seconds


if __name__ == "__main__":
    '''
    This Script starts an order book polling whenever an event trade is started
    '''
    #decide which second to make request to binance
    second_binance_request = random.randint(5, 58)
    logger = LoggingController.start_logging()

    coin = sys.argv[1]
    event_key = sys.argv[2]
    id = sys.argv[3]
    lvl = sys.argv[4]

    if lvl == 'None':
        lvl = None
    else:
        lvl = int(lvl)

    RESTART = bool(int(sys.argv[5]))
    minutes_timeframe = int(extract_timeframe(event_key))
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    limit = 20


    client = DatabaseConnection()
    db = client.get_db(DATABASE_ORDER_BOOK)
    db_collection = db[event_key]
    event_key_docs = list(db_collection.find({"_id": {"$gt": yesterday.isoformat()}} , {'_id':1,'coin':1, 'number':1}))
    db_volume_standings = client.get_db(DATABASE_VOLUME_STANDINGS)

    # at midnight (00:00 only) it is possible to have an exception due to unavailibility of the info
    try:
        id_volume_standings_db = now.strftime("%Y-%m-%d") 
        volume_standings = db_volume_standings[COLLECTION_VOLUME_STANDINGS].find_one({"_id": id_volume_standings_db})
        ranking = int(volume_standings['standings'][coin]['rank'])
    except:
        id_volume_standings_db_2 = yesterday.strftime("%Y-%m-%d")
        volume_standings = db_volume_standings[COLLECTION_VOLUME_STANDINGS].find_one({"_id": id_volume_standings_db_2})
        ranking = int(volume_standings['standings'][coin]['rank'])
    


    INITIALIZE_DOC_ORDERBOOK = True
    STOP_SCRIPT = False

    # count the current number of live order book scripts. Limit 20
    event_keys = db.list_collection_names()

    # check in the current event_key if there are current jobs
    for doc in event_key_docs:
        # If True, the event trigger has just started, otherwise the system has restarted and we are trying to resume the order book polling
        if doc['_id'] == id:
            INITIALIZE_DOC_ORDERBOOK = False
            if 'number' in doc:
                number_script=doc['number']
            else:
                number_script = None       
        # In case, the script has started in the script time windows, skip
        elif doc['coin'] == coin and now < datetime.fromisoformat(doc['_id']) + timedelta(minutes=minutes_timeframe):
            #logger.info(f'coin {coin}-{event_key} is running, skipping script')
            STOP_SCRIPT = True

    # if the event_key has a ranking threshold ("lvl") than check if ranking is in that threshold
    if not RESTART and lvl != None and ranking > lvl:
        #logger.info(f'coin {coin} has a ranking {ranking} higher than the threshold of the event_key lvl {event_key}')
        STOP_SCRIPT = True 
    
    live_order_book_scripts_number = get_current_number_of_orderbook_scripts(db, event_keys)
    if not RESTART:
        number_script = live_order_book_scripts_number
    elif number_script == None:
        number_script = live_order_book_scripts_number

    # initialize
    if not STOP_SCRIPT and INITIALIZE_DOC_ORDERBOOK and not RESTART:
        db_collection.insert_one({"_id": id, "coin": coin, "ranking": ranking, "data": {}, "number": live_order_book_scripts_number})

    if not STOP_SCRIPT:
        logger.info(f'Order-Book: event_key {event_key} triggered for {coin} - ranking {ranking }-  orderbook-script-number: {live_order_book_scripts_number}')
        if live_order_book_scripts_number // limit != 0:
            sleep(get_sleep_seconds(live_order_book_scripts_number, number_script, second_binance_request, limit))
            
        # this id is used to save the order book
        stop_script_datetime = datetime.now() + timedelta(minutes=minutes_timeframe)

        while datetime.now() < stop_script_datetime:
            order_book_info = get_info_order_book(coin, logger)
            if order_book_info != None:
                update_db_order_book_record(id, event_key, db_collection, order_book_info)

            event_keys = db.list_collection_names()
            live_order_book_scripts_number = get_current_number_of_orderbook_scripts(db, event_keys)
            sleep_seconds = get_sleep_seconds(live_order_book_scripts_number, number_script, second_binance_request, limit)
            next_iso_trigger = (datetime.now() + timedelta(seconds=sleep_seconds)).isoformat()
            #logger.info(f'Next trigger - {next_iso_trigger} - number_script: {number_script} -live_order_book_scripts_number {live_order_book_scripts_number} -limit: {limit} ')
            sleep(sleep_seconds)

    
    client.close()