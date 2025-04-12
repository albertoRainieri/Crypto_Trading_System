import os, sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))
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
from numpy import arange, linspace
from numpy import mean as np_mean
from time import time
#import random


def binance_order_book_request(coin, limit=5000):
    url = f"https://api.binance.com/api/v3/depth?symbol={coin}&limit={limit}"
    try:
        response = requests.get(url=url)
    except:
        logger.info("Code Exception on requests.get api.binance.com/api/v3/depth")
        return None, None
    headers = response.headers
    status_code = response.status_code
    if status_code == 418:
        logger.info(headers)
        retry_after = int(headers["retry-after"])
        logger.info(f"ip banned, waiting {retry_after}")
        sleep(retry_after*2)
    return response.text, status_code

def get_most_efficient_limit_orderbook_api(limit):
    # REFERENCE: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#order-book

    request_weight_legend = {
            (1,100): 5,
            (101,500): 25,
            (501,1000):	50,
            (1001,5000): 250 }
    
    threshold_limit = 1000
    x = 0.2 #percentage
    y = 0.15 #percentage

    if limit >= threshold_limit*(1-x):
        return 250, 5000
    else:
        for limit_n_range in request_weight_legend:
            if limit*(1+y) >= limit_n_range[0] and limit*(1+y) < limit_n_range[1]:
                weight = request_weight_legend[limit_n_range]
                break
        return weight, limit*(1+y)

def round_(number, decimal):
    return float(format(number, f".{decimal}f"))


def get_statistics(resp):
    '''
    This function makes a first pre-processing of the orderbook data, the output is then analyzed for buy-events
    '''
    json_resp = json.loads(resp)
    total_ask_volume = 0
    total_bid_volume = 0

    # lowest_bid_price = json_resp['bids'][-1][0]
    # highest_ask_price = json_resp['asks'][-1][0]
    best_bid_price = float(json_resp["bids"][0][0])
    best_ask_price = float(json_resp["asks"][0][0])

    max_n_orders = max(len(json_resp["bids"]),len(json_resp["asks"]))

    for ask_order in json_resp["asks"]:
        total_ask_volume += float(ask_order[0]) * float(ask_order[1])
    for bid_order in json_resp["bids"]:
        total_bid_volume += float(bid_order[0]) * float(bid_order[1])

    delta = 0.01
    cumulative_ask_volume = 0
    cumulative_bid_volume = 0
    summary_ask_orders = []
    summary_bid_orders = []

    next_delta_threshold = 0 + delta
    for ask_order in json_resp["asks"]:
        price_order = float(ask_order[0])
        quantity_order = float(ask_order[1])
        cumulative_ask_volume += price_order * quantity_order
        cumulative_ask_volume_ratio = round_(
            (cumulative_ask_volume / total_ask_volume), 2
        )
        if cumulative_ask_volume_ratio >= next_delta_threshold:
            summary_ask_orders.append(
                (
                    round_((price_order - best_ask_price) / best_ask_price, 3),
                    cumulative_ask_volume_ratio,
                )
            )
            next_delta_threshold = cumulative_ask_volume_ratio + delta

    next_delta_threshold = 0 + delta
    for bid_order in json_resp["bids"]:
        price_order = float(bid_order[0])
        quantity_order = float(bid_order[1])
        cumulative_bid_volume += price_order * quantity_order
        cumulative_bid_volume_ratio = round_(
            (cumulative_bid_volume / total_bid_volume), 2
        )
        if cumulative_bid_volume_ratio >= next_delta_threshold:
            summary_bid_orders.append(
                (
                    round_((price_order - best_bid_price) / best_bid_price, 3),
                    cumulative_bid_volume_ratio,
                )
            )
            next_delta_threshold = cumulative_bid_volume_ratio + delta
            # print(f'{next_delta_threshold}: {bid_order}')

    return (
        best_bid_price,
        best_ask_price,
        int(total_bid_volume),
        int(total_ask_volume),
        summary_bid_orders,
        summary_ask_orders,
        max_n_orders
    )


def get_info_order_book(coin, limit=5000):
    response, status_code = binance_order_book_request(coin, limit)
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


def update_db_order_book_record(id, event_key, db_collection, order_book_info, weight, max_price, initial_price, buy_price,
                                summary_jump_price_level, ask_order_distribution_list, BUY):

    new_data = []
    #new_data.append(order_book_info[0])  # bid_price
    new_data.append(order_book_info[1])  # ask_price
    new_data.append(order_book_info[2])  # total_bid_volume
    new_data.append(order_book_info[3])  # total_ask_volume
    new_data.append(order_book_info[4])  # summary_bid_orders
    new_data.append(order_book_info[5])  # summary_ask_orders

    now = datetime.now().replace(microsecond=0).isoformat()
    filter_query = {"_id": id}
    update_doc = {"$set": {f"data.{now}": new_data, "weight": weight, "max_price": max_price, "initial_price": initial_price,
                           'summary_jump_price_level': summary_jump_price_level, 'ask_order_distribution_list': ask_order_distribution_list,
                             'buy': BUY, 'buy_price': buy_price}}
    
    result = db_collection.update_one(filter_query, update_doc)

    if result.modified_count != 1:
        now = datetime.now().isoformat()
        logger.info(f"{now}: Order Book update failed for {event_key} with id {id}.")


def get_current_number_of_orderbook_scripts(db, event_keys):
    live_order_book_scripts_number = 0
    numbers_filled = []
    current_weight = 0
    for collection in event_keys:
        if collection != COLLECTION_ORDERBOOK_METADATA:

            minutes_timeframe = int(extract_timeframe(collection))
            yesterday = datetime.now() - timedelta(minutes=minutes_timeframe)
            query = {"_id": {"$gt": yesterday.isoformat()}}
            docs = list(db[collection].find(query, {"_id": 1, "number": 1, "weight":1}))
            for doc in docs:
                numbers_filled.append(doc["number"])
                if "weight" in doc:
                    current_weight += doc["weight"]
                else:
                    current_weight += 250
            # logger.info(docs)
            # len_docs = len(docs)
            # logger.info(f'{len_docs} - {collection}')
            live_order_book_scripts_number += len(docs)

    return numbers_filled, live_order_book_scripts_number, current_weight


def get_sleep_seconds( live_order_book_scripts_number, number_script, second_binance_request, limit ):
    """
    Based on the number of live scripts, define the appropriate sleep time, for avoiding binance api ban
    No more than 20 polling order_book_api each minute
    """

    # get the number of times the number of orderbookscripts is contained in the limit
    minute_api_trigger = live_order_book_scripts_number // limit + 1
    # based on the number script, assign the position of the starting minute (from 0,59)
    n_times_greater_than_limit = min(number_script // limit, minute_api_trigger - 1)
    # define the minutes range, the script is allowed to poll binance api
    minute_range = [ i for i in range(n_times_greater_than_limit, 60, minute_api_trigger) ]
    now = datetime.now()
    current_minute = now.minute

    if current_minute in minute_range:
        correct_minute = True
    else:
        correct_minute = False

    for i in range(len(minute_range)):
        if i != len(minute_range) - 1:
            if ( current_minute >= minute_range[i] and current_minute < minute_range[i + 1] ):
                # minutes remaining + seconds to 60 + second_binance_request
                sleep_seconds = (
                    (minute_range[i + 1] - current_minute - 1) * 60
                    + (60 - now.second)
                    + second_binance_request
                )
                return sleep_seconds, correct_minute
        else:
            sleep_seconds = (
                (60 + minute_range[0] - current_minute - 1) * 60
                + (60 - now.second)
                + second_binance_request
            )
            return sleep_seconds, correct_minute
        
def count_decimals(num):
  """
  Determines the number of decimal places in a given number.

  Args:
    num: The number to check.

  Returns:
    The number of decimal places, or 0 if the number is an integer.
  """
  try:
    str_num = str(num)
    decimal_index = str_num.index('.')
    return len(str_num) - decimal_index
  except ValueError:
    # If no decimal point is found, it's an integer
    return 1
  
def save_trading_event(id, coin, buy_price, ranking, initial_price, max_price, strategy):
    client = DatabaseConnection()
    db = client.get_db(DATABASE_TRADING)
    user = os.getenv('SYS_ADMIN')
    db_collection = db[user]
    buy_ts = datetime.now().isoformat()
    db_collection.insert_one( {"_id": id, "coin": coin, "gain": '', "buy_price": buy_price, "sell_price": '', "buy_ts": buy_ts, "sell_ts": '', "ranking": ranking, "initial_price": initial_price, "max_price": max_price, "strategy": strategy} )
    client.close()

def update_trading_event(id, sell_price, buy_price):
    client = DatabaseConnection()
    db = client.get_db(DATABASE_TRADING)
    user = os.getenv('SYS_ADMIN')
    db_collection = db[user]
    sell_ts = datetime.now().isoformat()
    filter_query = {"_id": id}
    gain = round_((sell_price - buy_price) / buy_price, 3)
    update_doc = {"$set":{"gain": gain, "sell_ts": sell_ts, "sell_price": sell_price}}
    logger.info(f'SELL EVENT: coin: {coin} - SELL price: {sell_price} - BUY price: {buy_price} - GAIN: {gain}')
    
    result = db_collection.update_one(filter_query, update_doc)
    if result.modified_count != 1:
        now = datetime.now().isoformat()
        logger.info(f"{now}: Trading Collection update failed for {event_key} with id {id}.")

    client.close()
        
def get_price_levels(price, orders, cumulative_volume_jump=0.03, price_change_limit=0.4, price_change_jump=0.025):
    '''
    this function outputs the pricelevels (tuple: 4 elements), order_distribution, cumulative_level
    price_levels (TUPLE):
        [1] : absolute price_level
        [2] : relative price_level (percentage from current price)
        [3] : cumulative_level (from 0 to 100, which percentage of bid/ask total volume this level corresponds to) (NOT USED)
        [4] : this is the JUMP. distance from previous level (from 0 to 100, gt > "$jump"). Only if greater than "cumulative_volume_jump"
    order_distribution (OBJ): (KEYS: price_change; VALUES: density of this price level
      (i.e. 5% of the bid/ask volume is concentrated in the first 2.5% price range, considering ONLY the "price_change_limit" (e.g. 40% price range from current price) ))
        {0.025: 0.05,
         0.05:  0.02,
         ...
         }
    previous_cumulative_level: it is the percentage of the bid/ask volume considering only the "price_change_limit" (e.g. 40% price range from current price)
                                with respect to the total bid/ask volume
    '''
    previous_level = 0
    price_levels = []
    n_decimals = count_decimals(price)
    cumulative_level_without_jump = 0
    price_change_level = price_change_jump
    order_distribution = {}
    for i in arange(price_change_jump,price_change_limit+price_change_jump,price_change_jump):
        order_distribution[str(round_(i,3))] = 0
    previous_cumulative_level = 0

    for level in orders:
        cumulative_level = level[1]
        price_change = level[0]

        # in this "if condition", I see how orders are distributed along the (0,price_change_limit) range,
        #  the chunks of orders are divided relative to "price_change_jump" (i.e. if var=0.025, I check in the [0,2.5%] price change window how deep is the order book and so on)
        # if price_change is below next threshold, keep updating the cumulative volume level for that price level
        if abs(price_change) <= price_change_level:
            order_distribution[str(price_change_level)] = cumulative_level
        
        # in case is above next threshold, update price_change_level and initialize next price_change_level
        else:
            # before moving to the next price level, update the relative level
            order_distribution[str(price_change_level)] = round_(order_distribution[str(price_change_level)] - previous_cumulative_level,2)

            # now update the next price change level
            previous_cumulative_level += order_distribution[str(price_change_level)]
            price_change_level = round_(price_change_level + price_change_jump,3)

            # in case some price level is empty, skip to the next level
            while abs(price_change) > price_change_level:
                price_change_level = round_(price_change_level + price_change_jump,3)

            # next chunk is below next thrshold
            if abs(price_change) <= price_change_level and abs(price_change) <= price_change_limit:
                order_distribution[str(price_change_level)] = cumulative_level

        # here, I discover the jumps, the info is stored in "price_levels"
        if cumulative_level - previous_level >= cumulative_volume_jump and abs(price_change) <= price_change_limit and abs(price_change) >= 0.01:
            actual_jump = round_(cumulative_level - previous_level,3)
            price_level = price * (1+price_change)
            info = (round_(price_level,n_decimals), price_change, cumulative_level, actual_jump)
            #price_levels.append(info)
            price_levels.append(round_(price_level,n_decimals))
        elif abs(price_change) <= price_change_limit:
            cumulative_level_without_jump = cumulative_level

        if abs(price_change) > price_change_limit:
            break
        previous_level = cumulative_level
    
    # scale order_distribution to [0,100] range
    for lvl in order_distribution:
        if previous_cumulative_level != 0:
            order_distribution[lvl] = round_(order_distribution[lvl] / previous_cumulative_level,3)
        else:
            order_distribution[lvl] = 0
    
    # if there are not jumps, at least I want to get the cumulative volume at the limit price level
    if len(price_levels) == 0:
        info = (None, None, cumulative_level_without_jump, False)
        price_levels.append(None)
    
    # if cumulative_level_without_jump == 0:
    #     print(bid_orders)
    
    return price_levels, order_distribution, round_(previous_cumulative_level,3)

def hit_jump_price_levels_range(coin, current_price, bid_price_levels_dt, summary_jump_price_level, neighborhood_of_price_jump = 0.005, distance_jump_to_current_price=0.01, min_n_obs_jump_level=5):
    '''
    This function defines all the historical level whereas a jump price change was existent
    Since it can happen that price_jump_level are not always in the same point (price) I check if the jump price is in the neighboorhood of the historical jump price (average with np_mean)

    # THIS IS THE INPUT OF bid_price_levels
    Structure of ask price_levels: IT IS A LIST OF LISTS
    - bid_price_levels: e.g. [[13.978], [13.958], [13.958], [13.978], [13.949, 12.942], [13.97], [13.939, 12.933], [14.053]]
      EACH SUBLIST containes the jump prices at dt

    # THIS THE STRUCTURE OF SUMMARY_JUMP_PRICE
    [LIST [TUPLES]]
        [ (average price jump level, list_of_jump_price_levels )]
    '''

    # iterate throgh 
    for abs_price in bid_price_levels_dt:
        if abs_price == None:
            continue
        if len(summary_jump_price_level) != 0:
            IS_NEW_X = True
            for x in summary_jump_price_level:
                historical_price_level = summary_jump_price_level[x][0]
                historical_price_level_n_obs = summary_jump_price_level[x][1]
                if abs_price <= historical_price_level * (1 + neighborhood_of_price_jump) and abs_price >= historical_price_level * (1 - neighborhood_of_price_jump):
                    #historical_price_level_list.append(abs_price)
                    historical_price_level = (historical_price_level*historical_price_level_n_obs + abs_price) / (historical_price_level_n_obs + 1)
                    summary_jump_price_level[x] = (historical_price_level, historical_price_level_n_obs + 1)
                    IS_NEW_X = False
                    break
            if IS_NEW_X:
                list_x = [int(i) for i in list(summary_jump_price_level.keys())]
                new_x = str(max(list_x) + 1)
                summary_jump_price_level[new_x] = (abs_price, 1)
        else:
            #print(abs_price)
            summary_jump_price_level['1'] = (abs_price, 1)
            
    #print(summary_jump_price_level)
    for x in summary_jump_price_level:
        #logger.info(summary_jump_price_level)
        jump = abs((current_price - summary_jump_price_level[x][0] ) / current_price )
        historical_price_level_n_obs = summary_jump_price_level[x][1]
        #logger.info(f'jump price level - {coin}: {jump} vs {distance_jump_to_current_price}')
        if jump <= distance_jump_to_current_price and historical_price_level_n_obs >= min_n_obs_jump_level:
            return True, summary_jump_price_level
            #print(current_price, dt)
    return False, summary_jump_price_level

def trigger_entrypoint(id, coin, price, initial_price, max_price,
                        bid_price_levels_dt, summary_jump_price_level, ask_order_distribution_list, price_change_jump,
                        max_limit, price_drop_limit, distance_jump_to_current_price,
                        max_ask_order_distribution_level, last_i_ask_order_distribution, min_n_obs_jump_level, strategy):
    
    BUY = False
    dt_ask = datetime.now() - timedelta(minutes=last_i_ask_order_distribution) + timedelta(seconds=10)
    max_price = max(price, max_price)
    max_change = ( max_price - initial_price ) / initial_price
    current_price_drop = abs( (price - max_price) / max_price )
    selected_ask_order_distribution_list = []
    for ask_order_distribution in ask_order_distribution_list:
        if ask_order_distribution['dt'] > dt_ask:
            selected_ask_order_distribution_list.append(ask_order_distribution)

    # PHASE 2
    # DEFINE HOW CLOSE THE PRICE IS TO HISTORICAL JUMP LEVELS
    t1 = time()
    is_jump_price_level, summary_jump_price_level  = hit_jump_price_levels_range(coin=coin, current_price=price, bid_price_levels_dt=bid_price_levels_dt,
                                                        summary_jump_price_level=summary_jump_price_level, distance_jump_to_current_price=distance_jump_to_current_price,
                                                        min_n_obs_jump_level=min_n_obs_jump_level)
    t2 = time()
    time_spent = round_(t2-t1,5)
    #logger.info(f'hit_jump_price_levels_range executed in {time_spent}s')
    
    #logger.info(f'{coin}: {current_price_drop} >= {price_drop_limit} and {max_change} <= {max_limit}')
    if is_jump_price_level and current_price_drop >= price_drop_limit and max_change <= max_limit:
                
        # get keys of ask_order_distribution [0.025, 0.05, 0.075, ...]
        if len(selected_ask_order_distribution_list) >= last_i_ask_order_distribution:
            keys_ask_order_distribution = list(selected_ask_order_distribution_list[-1]['ask'].keys())

            # initialize new var for average ask order distribution
            avg_ask_order_distribution = {}

            for lvl in keys_ask_order_distribution:
                avg_ask_order_distribution[lvl] = []

            for selected_ask_order_distribution in selected_ask_order_distribution_list:
                for lvl in selected_ask_order_distribution['ask']:
                    avg_ask_order_distribution[lvl].append(selected_ask_order_distribution['ask'][lvl])
            
            # compute the mean
            for lvl in avg_ask_order_distribution:
                avg_ask_order_distribution[lvl] = np_mean(avg_ask_order_distribution[lvl])

            # TODO: check only first level, or next ones too ?
            #logger.info(f'avg_ask_order_distribution: {avg_ask_order_distribution}')
            if avg_ask_order_distribution[str(price_change_jump)] < max_ask_order_distribution_level:
                BUY = True
                x = avg_ask_order_distribution[str(price_change_jump)]
                logger.info(f'BUY EVENT: coin: {coin} - price: {price} - avg_ask_order: {x}')
                save_trading_event(id, coin, price, ranking, initial_price, max_price, strategy)

                return max_price, selected_ask_order_distribution_list, BUY, price, summary_jump_price_level
        
    return max_price, selected_ask_order_distribution_list, BUY, None, summary_jump_price_level

def analyze_ask_orders(id, coin, order_book_info, initial_price, max_price, summary_jump_price_level, ask_order_distribution_list,
                        strategy):
    '''
    order_book_info[0] --> bid_price
    order_book_info[1] --> ask_price
    order_book_info[2] --> total_bid_volume
    order_book_info[3] --> total_ask_volume
    order_book_info[4] --> summary_bid_orders
    order_book_info[5] --> summary_ask_orders
    '''

    cumulative_volume_jump = strategy['strategy_jump']
    price_change_limit = strategy['limit']
    price_change_jump = strategy['price_change_jump']
    max_limit = strategy['max_limit']
    price_drop_limit = strategy['price_drop_limit']
    distance_jump_to_current_price = strategy['distance_jump_to_current_price']
    max_ask_order_distribution_level = strategy['max_ask_order_distribution_level']
    last_i_ask_order_distribution = strategy['last_i_ask_order_distribution']
    min_n_obs_jump_level = strategy['min_n_obs_jump_level']
    
    dt = datetime.now()
    price = order_book_info[1] #ask price
    if initial_price == None:
        initial_price = price

    bid_orders = order_book_info[4]
    ask_orders = order_book_info[5]

    t1 = time()
    ask_price_level_dt, ask_order_distribution, ask_cumulative_level = get_price_levels(price, ask_orders, cumulative_volume_jump, price_change_limit, price_change_jump)
    bid_price_level_dt, bid_order_distribution, bid_cumulative_level = get_price_levels(price, bid_orders, cumulative_volume_jump, price_change_limit, price_change_jump)
    t2 = time()
    time_spent = round_(t2-t1,5)
    #logger.info(f'get_price_levels executed in {time_spent}s')

    ask_order_distribution_list.append({'ask': ask_order_distribution, 'dt': dt})
    max_price, ask_order_distribution_list, BUY, buy_price, summary_jump_price_level = trigger_entrypoint(id,
                        coin, price, initial_price, max_price, bid_price_level_dt, summary_jump_price_level, ask_order_distribution_list, price_change_jump,
                        max_limit, price_drop_limit, distance_jump_to_current_price,
                        max_ask_order_distribution_level, last_i_ask_order_distribution, min_n_obs_jump_level, strategy)

    return max_price, initial_price, summary_jump_price_level, ask_order_distribution_list, BUY, buy_price


if __name__ == "__main__":
    """
    This Script starts an order book polling whenever an event trade is started
    """
    # decide which second to make request to binance
    #second_binance_request = random.randint(5, 58)

    logger = LoggingController.start_logging()
    
    # MANAGEMENT VARIABLES
    LIMIT = 20
    efficient_limit = 5000
    range_second_trigger = [round_(i,2) for i in linspace(10,58,20)]

    # STRATEGY VARIABLES
    BUY = False
    summary_jump_price_level = {}
    ask_order_distribution_list = []
    max_price = 0
    initial_price = None
    buy_price = None

    # INPUT VARIABLE SCRIPT
    coin = sys.argv[1]
    event_key = sys.argv[2]
    id = sys.argv[3]
    ranking = sys.argv[4]
    RESTART = bool(int(sys.argv[5]))
    event_keys_json = json.loads(sys.argv[7])
    event_keys = event_keys_json['event_keys']

    if not RESTART:
        strategy_parameters = json.loads(sys.argv[6])

    # TIME VARIABLE
    minutes_timeframe = int(extract_timeframe(event_key))
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    # this variable sets the minute range, within which the coin can not be traded again
    coin_exclusive_window_minutes = int(os.getenv('COIN_EXCLUSIVE_ORDERBOOK_WINDOW_MINUTES'))

    # DB VARIABLES
    client = DatabaseConnection()
    db = client.get_db(DATABASE_ORDER_BOOK)
    db_collection = db[event_key]
    event_key_docs = list( db_collection.find( {"_id": {"$gt": yesterday.isoformat()}}, {"_id": 1, "coin": 1,
                                                                                          "number": 1, "strategy_parameters": 1,
                                                                                          "initial_price": 1, "max_price": 1, "buy":1, "buy_price": 1,
                                                                                            "summary_jump_price_level":1, "ask_order_distribution_list": 1} ) )
    
    INITIALIZE_DOC_ORDERBOOK = True
    STOP_SCRIPT = False

    # count the current number of live order book scripts. LIMIT 20
    # check in the current event_key if there are current jobs
    for doc in event_key_docs:
        # If True, the system has restarted and we are trying to resume the order book polling
        if doc["_id"] == id:
            INITIALIZE_DOC_ORDERBOOK = False
            number_script = doc["number"]
            strategy_parameters = doc['strategy_parameters']
            if 'initial_price' in doc:
                initial_price = doc['initial_price']
            if 'max_price' in doc:
                max_price = doc['max_price']
            if 'buy' in doc:
                BUY = doc['buy']
            if 'summary_jump_price_level' in doc:
                summary_jump_price_level = doc['summary_jump_price_level']
            if 'ask_order_distribution_list' in doc: 
                ask_order_distribution_list = doc['ask_order_distribution_list']
            if 'buy_price' in doc:
                buy_price = doc['buy_price']
                
            second_binance_request = range_second_trigger[number_script % LIMIT]

        # In case, the script has started in the script time windows, skip
        elif doc["coin"] == coin and now < datetime.fromisoformat( doc["_id"] ) + timedelta(minutes=coin_exclusive_window_minutes):
            # logger.info(f'coin {coin}-{event_key} is running, skipping script')
            STOP_SCRIPT = True

    numbers_filled, live_order_book_scripts_number, current_weight = get_current_number_of_orderbook_scripts(db, event_keys)
    # initialize
    if not STOP_SCRIPT and INITIALIZE_DOC_ORDERBOOK and not RESTART:
        for i in range(live_order_book_scripts_number + 1):
            if i not in numbers_filled:
                number_script = i
                break
        

        second_binance_request = range_second_trigger[number_script % LIMIT]
        db_collection.insert_one( { "_id": id, "coin": coin, "ranking": ranking, "data": {}, "initial_price": initial_price, "max_price": max_price, "buy_price": '',
                                     "buy": BUY, "number": number_script, "strategy_parameters": strategy_parameters, "weight": 250,
                                     "summary_jump_price_level": summary_jump_price_level, "ask_order_distribution_list": ask_order_distribution_list} )

    if not STOP_SCRIPT:
        current_number_script = number_script + 1
        total_scripts_live = live_order_book_scripts_number + 1
        logger.info( f"Order-Book: event_key {event_key} triggered for {coin} - ranking {ranking }-  orderbook-script-number: {current_number_script}/{total_scripts_live}")
        sleep_seconds, correct_minute = get_sleep_seconds( live_order_book_scripts_number, number_script, second_binance_request, LIMIT )
        if live_order_book_scripts_number // LIMIT != 0 and not correct_minute: # or is_same_minute :
            #logger.info(f'sleep second: {sleep_seconds}')
            sleep( sleep_seconds )

        # this id is used to save the order book
        stop_script_datetime = datetime.fromisoformat(id) + timedelta(minutes=minutes_timeframe)

        while datetime.now() < stop_script_datetime:
            #logger.info(f'{number_script}')
            order_book_info = get_info_order_book(coin, limit=efficient_limit)
            if order_book_info != None:
                
                if not BUY:
                    max_price, initial_price, summary_jump_price_level, ask_order_distribution_list, BUY, buy_price = analyze_ask_orders(id,
                        coin, order_book_info, initial_price, max_price, summary_jump_price_level, ask_order_distribution_list, strategy_parameters
                        )
                
                # here, I try to get an efficient limit (parameter) for orderbook api, for respecting the request weight limit
                # coin with less volume, tend to need a lower limit, so I compute it
                # (25/02/2025) it looks it is unneccessary, weight tends to be always at max (250), even for coins with few volume
                limit_orderbook_api = order_book_info[6]
                weight, efficient_limit = get_most_efficient_limit_orderbook_api(limit_orderbook_api)
                update_db_order_book_record( id, event_key, db_collection, order_book_info, weight,
                                            max_price, initial_price, buy_price,
                                            summary_jump_price_level, ask_order_distribution_list, BUY)


            numbers_filled, live_order_book_scripts_number, current_weight = ( get_current_number_of_orderbook_scripts(db, event_keys) )
            times_in_live_order_book_scripts_number = live_order_book_scripts_number // LIMIT
            sleep_seconds, correct_minute = get_sleep_seconds(  live_order_book_scripts_number, number_script, second_binance_request, LIMIT)
            # next_iso_trigger = (  datetime.now() + timedelta(seconds=sleep_seconds) ).isoformat()
            # logger.info(f'coin: {coin}; position: {ranking}; weight: {weight}; efficient_limit: {efficient_limit}')
            # if number_script == 1:
            #     logger.info(f'Current Limit Weight: {current_weight}')
            # logger.info(f'Next trigger - {next_iso_trigger} - number_script: {number_script} -live_order_book_scripts_number {live_order_book_scripts_number} -LIMIT: {LIMIT} ')
            sleep(sleep_seconds)

            # this step is necessary in case the number of live scripts has increased and the minute trigger has shifted, so sleep_seconds need to be adjusted
            # "correct_minute" is a boolean and if the minute trigger has shifted, then take extra sleep, but if this minute is in minute_range, then do not take extra sleep
            numbers_filled, live_order_book_scripts_number, current_weight = get_current_number_of_orderbook_scripts(db, event_keys)
            NEXT_times_in_live_order_book_scripts_number = live_order_book_scripts_number // LIMIT
            sleep_seconds, correct_minute = get_sleep_seconds(  live_order_book_scripts_number, number_script, second_binance_request, LIMIT)
            if times_in_live_order_book_scripts_number != NEXT_times_in_live_order_book_scripts_number and not correct_minute:
                #logger.info(f'{number_script} switch')
                sleep(sleep_seconds)
        
        pid = os.getpid()
        logger.info(f'DONE: {number_script} - {coin} - killing pid {pid}')

        if BUY:
            sell_price = order_book_info[0] #bid_price
            update_trading_event(id=id, sell_price=sell_price, buy_price=buy_price)

            

    client.close()
    pid = os.getpid()
    os.kill(pid, SIGKILL)
