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


def binance_order_book_request(coin):
    url = f"https://api.binance.com/api/v3/depth?symbol={coin}&limit=5000"
    response = requests.get(url=url)
    status_code = response.status_code
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
            summary_ask_orders.append((round_(( price_order -  current_price ) / current_price, 2), cumulative_ask_volume_ratio ))
            next_delta_threshold = cumulative_ask_volume_ratio + delta

    next_delta_threshold = 0 + delta
    for bid_order in json_resp['bids']:
        price_order = float(bid_order[0])
        quantity_order = float(bid_order[1])
        cumulative_bid_volume += price_order * quantity_order
        cumulative_bid_volume_ratio = round_((cumulative_bid_volume / total_bid_volume),2)
        if cumulative_bid_volume_ratio >= next_delta_threshold:
            summary_bid_orders.append((round_(( price_order -  current_price ) / current_price, 2), cumulative_bid_volume_ratio ))
            next_delta_threshold = cumulative_bid_volume_ratio + delta
            #print(f'{next_delta_threshold}: {bid_order}')

    return current_price, int(total_ask_volume), int(total_bid_volume), summary_ask_orders, summary_bid_orders


def get_info_order_book(coin, logger):
    response, status_code = binance_order_book_request(coin)
    if status_code == 200:
        return get_statistics(response)
    else:
        logger.info('Status Code for api/v3/depth is not 200')
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

    current_price = order_book_info[0]
    total_ask_volume = order_book_info[1]
    total_bid_volume = order_book_info[2]
    summary_ask_orders = order_book_info[3]
    summary_bid_orders = order_book_info[4]

    now = datetime.now().replace(microsecond=0).isoformat()
    filter_query = {"_id": id}
    update_doc = {"$set": {f"current_price.{now}": current_price,
                           f"ask_volume.{now}": total_ask_volume, f"bid_volume.{now}": total_bid_volume,
                           f"ask_orders.{now}": summary_ask_orders, f"bid_orders.{now}": summary_bid_orders}}
    result = db_collection.update_one(filter_query, update_doc)

    if result.modified_count != 1:
        print(f"Order Book update failed for {event_key} with id {id}.") 

if __name__ == "__main__":
    '''
    This Script starts an order book polling whenever an event trade is started
    '''

    SLEEP_SECONDS = 60
    coin = sys.argv[1]
    event_key = sys.argv[2]
    id = sys.argv[3]
    RESTART = bool(int(sys.argv[4]))
    minutes_timeframe = int(extract_timeframe(event_key))
    now = datetime.now()


    client = DatabaseConnection()
    db = client.get_db(DATABASE_ORDER_BOOK)
    db_collection = db[event_key]
    docs = list(db_collection.find({}))
    logger = LoggingController.start_logging()

    id_volume_standings_db = now.strftime("%Y-%m-%d") 
    db_volume_standings = client.get_db(DATABASE_VOLUME_STANDINGS)
    volume_standings = db_volume_standings[COLLECTION_VOLUME_STANDINGS].find_one({"_id": id_volume_standings_db})
    ranking = volume_standings['standings'][coin]['rank']

    INITIALIZE_DOC_ORDERBOOK = True
    STOP_SCRIPT = False

    for doc in docs:
        # If True, the event trigger has just started, otherwise the system has restarted and we are trying to resume the order book polling
        if doc['_id'] == id:
            INITIALIZE_DOC_ORDERBOOK = False
        # In case, the script has started in the script time windows, skip
        elif doc['coin'] == coin and now < datetime.fromisoformat(doc['_id']) + timedelta(minutes=minutes_timeframe):
            STOP_SCRIPT = True        
    # initialize doc

    if not STOP_SCRIPT and INITIALIZE_DOC_ORDERBOOK and not RESTART:
        db_collection.insert_one({"_id": id, "coin": coin, "ranking": ranking, "current_price": {},
                                "ask_volume": {}, "bid_volume": {},
                                "bid_orders": {}, "ask_orders": {}})

    if not STOP_SCRIPT:
        # this id is used to save the order book
        now = datetime.now()
        stop_script_datetime = datetime.now() + timedelta(minutes=minutes_timeframe)

        while datetime.now() < stop_script_datetime:
            order_book_info = get_info_order_book(coin, logger)
            if order_book_info != None:
                update_db_order_book_record(id, event_key, db_collection, order_book_info)
            sleep(SLEEP_SECONDS-datetime.now().second)

    
    client.close()