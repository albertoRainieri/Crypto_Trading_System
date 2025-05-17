import os, sys

sys.path.insert(1, os.path.join(sys.path[0], ".."))

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
METHOD = "aggTrades"
TRADEID = "a"
LOST_CONNECTION = []
MAX_CONNECTION_LOST = int(os.getenv("MAX_CONNECTION_LOST"))  # 2
CHECK_PERIOD_MINUTES = int(os.getenv("CHECK_PERIOD_MINUTES"))  # 60
SLEEP_LAST_LIST = int(os.getenv("SLEEP_LAST_LIST"))  # 60
MAX_ERRORS_PER_MINUTE = 10
errors_in_last_minute = []


def on_error(ws, error):
    global error_summary
    global coin_list
    global errors_in_last_minute

    # Add current error timestamp to track errors per minute
    now = datetime.now()
    errors_in_last_minute.append(now)
    
    # Remove errors older than 1 minute
    errors_in_last_minute = [t for t in errors_in_last_minute if (now - t).total_seconds() < 60]
    
    # Check if more than MAX_ERRORS_PER_MINUTE errors in last minute
    if len(errors_in_last_minute) > MAX_ERRORS_PER_MINUTE:
        logger.error(f"{LIST}: Too many errors in last minute ({len(errors_in_last_minute)}). Restarting connection.")
        ws.close()
        return

    error = str(error)
    if error not in coin_list:
        logger.error(f"{LIST}: {error}")

        if error in error_summary:
            error_summary[error] += 1
        else:
            error_summary[error] = 1

        # logger.info(f"{LIST}: {error_summary}")
        # test_date =  (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        # test_date2 = datetime.now().strftime("%Y-%m-%d")
        error_summary = manage_update(
            error_summary
        )  # , current_date=test_date, yesterday_date=test_date2)

        if "Connection to remote host was lost" in error:
            ws.close()
        if "sell_volume" in error:
            ws.close()
        # else:
        #     logger.error(f"Error not caught: #{error}#")
    else:
        sleep(60 - (datetime.now().second + (datetime.now().microsecond // 10**6)))


def on_close(*args):
    if datetime.now().hour == 0 and datetime.now().minute == 0:
        sleep(LIST_N)

    threading.Thread(target=restart_connection).start()
    ws.run_forever()


def restart_logic():
    """
    compute total seconds until next wss restart.
    This function restarts the wss connection every 24 hours
    """
    global error_summary
    global errors_in_last_minute

    # Reset errors in last minute counter on restart
    errors_in_last_minute = []

    now = datetime.now()
    current_hour = now.hour
    current_minute = now.minute

    # compute how many seconds until next restart
    HOURS = 24 - current_hour
    remaining_seconds = 60 - now.second + 1
    minutes_remaining = 59 - current_minute
    hours_remaining = HOURS - 1

    # total seconds until next wss restart
    total_remaining_seconds = (
        remaining_seconds + minutes_remaining * 60 + hours_remaining * 60 * 60
    )

    # ONLY FOR TESTING
    # total_remaining_seconds = 240 + remaining_seconds

    # define timestamp for next wss restart
    # wss_restart_timestamp = (now + timedelta(seconds=total_remaining_seconds)).isoformat()
    # logger.info(f'on_restart_connection: Next wss restart {wss_restart_timestamp} for {LIST}')

    sleep(total_remaining_seconds)
    logger.info(f"{LIST}: restart connection")


def on_message(ws, message):
    global prices
    global doc_db
    global coin_list
    global now_ts
    global trade_ids

    data = json.loads(message)

    if "data" in data:
        data = data["data"]
        trade_id = data["a"]
        instrument_name = data["s"]

        # LOCAL TEST
        # if (datetime.now().minute in [i for i in range(60)]) and datetime.now().second in [30]:
        #     raise ValueError("Connection to remote host was lost")
        if instrument_name not in trade_ids:
            doc_db[instrument_name] = {
                "_id": None,
                "price": None,
                "n_trades": 0,
                "buy_n": 0,
                "volume": 0,
                "buy_volume": 0,
                "sell_volume": 0,
            }
            prices[instrument_name] = None
            trade_ids[instrument_name] = []


        if trade_id not in trade_ids[instrument_name]:
            trade_ids[instrument_name].append(trade_id)

            if now_ts is not None:
                # if same minute then keep on getting statistics
                if now_ts == datetime.now().strftime("%Y-%m-%d:%H:%M"):
                    getStatisticsOnTrades(data, instrument_name, doc_db, prices)
                # otherwise let's save it to db
                else:
                    # logger.info(f'{n_coins}/{tot_coins} coins have been traded in the last minute. {LIST}')
                    now_ts = datetime.now().strftime("%Y-%m-%d:%H:%M")
                    # logger.info('save')
                    saveTrades_toDB(prices, doc_db, database)
                    # logger.info(trade_ids)
                    doc_db, prices, trade_ids = initializeVariables(coin_list)
            else:
                sleep(60 - datetime.now().second + 1)
                now_ts = datetime.now().strftime("%Y-%m-%d:%H:%M")
                doc_db, prices, trade_ids = initializeVariables(coin_list)
    # else:
    #     logger.info(data)


def on_open(ws):

    global prices
    global doc_db
    global coin_list
    global trade_ids

    # msg = 'WSS CONNECTION STARTED'
    # logger.info(msg)
    # db_logger[DATABASE_API_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})

    # now = datetime.now().isoformat()
    # msg = f'{LIST}: on_open'
    # logger.info(msg)

    coin_list = select_coins(
        LIST, db_benchmark, position_threshold, type_USDT=os.getenv("TYPE_USDT")
    )
    parameters = []
    for coin in coin_list:
        parameters.append(coin.lower() + "@aggTrade")

    doc_db, prices, trade_ids = initializeVariables(coin_list)

    # print("### Opened ###")
    subscribe_message = {"method": "SUBSCRIBE", "params": parameters, "id": 1}
    ws.send(json.dumps(subscribe_message))


def restart_connection():
    """
    this function is implemented for avoiding multiple restarts
    """
    global connection_restart_running
    with connection_restart_lock:
        if connection_restart_running:
            logger.info(f"{LIST}: Connection restart already in progress.")
            return
        connection_restart_running = True

    try:
        restart_logic()
    finally:
        with connection_restart_lock:
            connection_restart_running = False
            ws.close()


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
        position = item[1].get("position")
        return float("inf") if position is None else position

    return sorted(obj.items(), key=sort_key)


def initializeVariables(coin_list):
    # logger.info(f'{LIST} initializeVariables')

    doc_db = {}
    prices = {}
    trade_ids = {}

    for instrument_name in coin_list:
        doc_db[instrument_name] = {
            "_id": None,
            "price": None,
            "n_trades": 0,
            "buy_n": 0,
            "volume": 0,
            "buy_volume": 0,
            "sell_volume": 0,
        }
        prices[instrument_name] = None
        trade_ids[instrument_name] = []

    return doc_db, prices, trade_ids


def getStatisticsOnTrades(trade, instrument_name, doc_db, prices):

    if instrument_name in doc_db:
        doc_db[instrument_name]["n_trades"] += 1

        if trade[ORDER]:
            order = "SELL"
        else:
            order = "BUY"

        quantity = trade[QUANTITY]
        price = trade[PRICE]

        if price != 0 and quantity != 0:
            prices[instrument_name] = float(price)

            # doc_db[instrument_name]["quantity"] += float(quantity)

            if order == "BUY":
                doc_db[instrument_name]["buy_n"] += 1
                doc_db[instrument_name]["buy_volume"] += float(quantity) * float(price)
            else:
                # doc_db[instrument_name]["sell_n"] += 1
                doc_db[instrument_name]["sell_volume"] += float(quantity) * float(price)
        else:
            logger.info("Duplicate Aggregate Trade")


# @timer_func
def saveTrades_toDB(prices, doc_db, database):

    path = legend_list[LIST]["path"]

    if os.path.exists(path):
        f = open(path, "r")
        last_prices = json.loads(f.read())
    else:
        last_prices = {}
        for instrument_name in prices:
            last_prices[instrument_name] = None

    for instrument_name in prices:
        if doc_db[instrument_name]["n_trades"] != 0:
            last_prices[instrument_name] = prices[instrument_name]
            doc_db[instrument_name]["price"] = prices[instrument_name]
            # doc_db[instrument_name]["quantity"] = round_(doc_db[instrument_name]["quantity"],2)
            doc_db[instrument_name]["volume"] = int(
                doc_db[instrument_name]["buy_volume"]
                + doc_db[instrument_name]["sell_volume"]
            )
            # doc_db[instrument_name]["sell_volume"] = round_(doc_db[instrument_name]["sell_volume"],2)
            del doc_db[instrument_name]["sell_volume"]
            doc_db[instrument_name]["buy_volume"] = int(
                doc_db[instrument_name]["buy_volume"]
            )
            doc_db[instrument_name]["_id"] = datetime.now().isoformat()
            database[instrument_name].insert_one(doc_db[instrument_name])
        else:
            if instrument_name in last_prices:
                doc_db[instrument_name]["price"] = last_prices[instrument_name]
            else:
                continue

            if doc_db[instrument_name]["price"] == None:
                continue

            # doc_db[instrument_name]["quantity"] = 0
            doc_db[instrument_name]["volume"] = 0
            # doc_db[instrument_name]["sell_volume"] = 0
            del doc_db[instrument_name]["sell_volume"]
            doc_db[instrument_name]["buy_volume"] = 0
            doc_db[instrument_name]["_id"] = datetime.now().isoformat()
            database[instrument_name].insert_one(doc_db[instrument_name])

    with open(path, "w") as outfile_volume:
        outfile_volume.write(json.dumps(last_prices))


def initialize_new_doc_db_logging(current_date=datetime.now().strftime("%Y-%m-%d")):

    last_doc = list(logging_collection.find({"_id": current_date}))
    if len(last_doc) == 0:
        doc = {"_id": current_date}
        logging_collection.insert(doc)
        error_summary = {}
    elif LIST in last_doc[0]:
        error_summary = last_doc[0][LIST]
    else:
        error_summary = {}

    return error_summary


def update_db_logging(error_summary, date):

    filter_query = {"_id": date}
    # last_doc = last_doc[0]
    # last_doc[LIST] = error_summary
    update_doc = {"$set": {LIST: error_summary}}
    # logger.info(f'update_doc {update_doc}')
    result = logging_collection.update_one(filter_query, update_doc)
    if result.modified_count != 1:
        logger.info(f"Update DB Logging failed.")


def manage_update(
    error_summary,
    current_date=datetime.now().strftime("%Y-%m-%d"),
    yesterday_date=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
):

    last_doc = list(logging_collection.find({"_id": current_date}))
    # in this scenario, it is midnight (restart is ), update logging collections and initialize a new error summary
    if len(last_doc) == 0:
        last_doc = list(logging_collection.find({"_id": yesterday_date}))
        if len(last_doc) != 0:
            # logger.info('update')
            update_db_logging(error_summary, yesterday_date)
        else:
            logger.info("Disconnection at the first run ever")
            pass  # disconnection at the first run ever
        error_summary = initialize_new_doc_db_logging()
    # this is the case where an error has occurred (likely connection error), just update logging collection
    else:
        update_db_logging(error_summary, current_date)
    return error_summary


def on_ping(ws, message):
    ws.send(message, websocket.ABNF.OPCODE_PONG)


def select_coins(
    LIST,
    db_benchmark,
    position_threshold,
    logging=True,
    type_USDT=os.getenv("TYPE_USDT"),
):
    """
    type_USDT: (POSSIBLE OPTIONS)
        - usdt_coins: ALL USDT
        - usdc_coins: ALL USDC
        - list_usdt_common_usdc: ALL USDT ALSO TRADED IN USDC
        - list_usdc_common_usdt: ALL USDC ALSO TRADED IN USDT

    THIS FUNCTION outputs the list of coins to be fetched and traded
    """

    dir_info = "/backend/json"
    coins_list_path = f"{dir_info}/coins_list.json"
    f = open(coins_list_path, "r")
    coins_list_info = json.loads(f.read())
    # coin_list = data["most_traded_coins"][:NUMBER_COINS_TO_TRADE_WSS]
    # I AM MOVING ETHUSDT TO SECOND LIST BECAUSE I NOTED THAT SOMETIMES TRADE VOLUME IN LIST 2 IS VERY LOW AND THEREFORE THE SAVE TO DB HAPPENS TOO LATE,.
    # THIS IS RISKY FOR THE TRACKER WHICH IS NOT ABLE TO GET THE LAST DATA
    coins_list = coins_list_info[type_USDT]
    datetimenow = datetime.now()
    # datetimenow = datetime(2025,3,2)
    yesterday_date = (datetimenow - timedelta(days=1)).strftime("%Y-%m-%d")

    collection_list = db_benchmark.list_collection_names()
    summary = {}
    coins_no_longer_traded = {}
    for coin in collection_list:
        if coin in discard_coin_list():
            continue
        if coin not in coins_list:
            continue
        volume_benchmark_coin_doc = db_benchmark[coin].find_one()
        volume_30_avg = volume_benchmark_coin_doc["volume_30_avg"]
        # if the coin has at least 30 days of analysis, it is eligible for deletion
        # logger.info(coin)
        if "Last_30_Trades" not in volume_benchmark_coin_doc:
            elegible_for_deletion == False
        elif (
            len(volume_benchmark_coin_doc["Last_30_Trades"]["list_last_30_trades"])
            == 30
        ):
            elegible_for_deletion = True
        else:
            elegible_for_deletion = False
        if "volume_series" in volume_benchmark_coin_doc:
            volume_series = volume_benchmark_coin_doc["volume_series"]
            dates_volume_series = list(volume_series.keys())
            # Convert date strings to datetime objects
            date_objects = [
                datetime.strptime(date_str, "%Y-%m-%d")
                for date_str in dates_volume_series
            ]
            # Sort the datetime objects
            date_objects.sort()
            # Convert the sorted datetime objects back to strings
            sorted_date_strings = [
                date_obj.strftime("%Y-%m-%d") for date_obj in date_objects
            ]
            last_date = sorted_date_strings[-1]
            if last_date == yesterday_date:
                summary[coin] = {
                    "volume_30_avg": volume_30_avg,
                    "elegible_for_deletion": elegible_for_deletion,
                }
            else:
                coins_no_longer_traded[coin] = {
                    "volume_30_avg": volume_30_avg,
                    "elegible_for_deletion": elegible_for_deletion,
                }

    n_summary = len(summary)

    discarded_volumes = list(coins_no_longer_traded.values())
    all_volumes = list(summary.values())
    sorted_discarded_volumes = sorted(
        discarded_volumes, key=lambda x: x["volume_30_avg"], reverse=True
    )
    sorted_volumes = sorted(all_volumes, key=lambda x: x["volume_30_avg"], reverse=True)
    # logger.info(sorted_volumes)
    # threshold_volume = sorted_volumes[position_threshold]

    most_traded_coins_first_filter = {}
    coins_discarded = 0
    coins_brought_back = []

    for coin in coins_list:
        if coin in discard_coin_list():
            continue
        # these coins are most likely not traded, but there some that are new entry, I need to list them all
        if coin not in summary and coin not in coins_no_longer_traded:
            most_traded_coins_first_filter[coin] = {"position": None}
        elif coin in summary:
            position = sorted_volumes.index(summary[coin]) + 1
            # if coin not in summary, it is a new entry
            # if position below threshold, perfect
            # if coin does not have at least 30 obs, it is not eligible for deletion
            if position > position_threshold and summary[coin]["elegible_for_deletion"]:
                coins_discarded += 1
                continue
            else:
                most_traded_coins_first_filter[coin] = {"position": position}
        else:
            position = (
                sorted_discarded_volumes.index(coins_no_longer_traded[coin])
                + 1
                + n_summary
            )
            if (
                position > position_threshold
                and coins_no_longer_traded[coin]["elegible_for_deletion"]
            ):
                coins_discarded += 1
                continue
            else:
                coins_brought_back.append(coin)
                most_traded_coins_first_filter[coin] = {"position": position}

    most_traded_coins_first_filter = sort_object_by_position(
        most_traded_coins_first_filter
    )
    # logger.info(most_traded_coins_first_filter)

    # logger.info(f'coins discarded: {coins_discarded} for list: {LIST}')

    coin_list = {}
    for i in range(1, SETS_WSS_BACKEND + 1):
        coin_list[f"list{str(i)}"] = []

    best_coins = [
        tuple_[0] for tuple_ in most_traded_coins_first_filter[:SETS_WSS_BACKEND]
    ]

    coins_in_none_position = []
    for tuple_ in most_traded_coins_first_filter:
        coin = tuple_[0]
        position = tuple_[1]["position"]
        if coin in best_coins:
            position_best_coin = best_coins.index(coin) + 1
            if position_best_coin == legend_list[LIST]["best_coin"]:
                coin_list[LIST].append(coin)
            continue
        if position != None:
            # logger.info(f'{coin} - {position}')
            for list_name in legend_list:
                if position % SETS_WSS_BACKEND == legend_list[list_name]["position"]:
                    coin_list[list_name].append(coin)
        else:
            coins_in_none_position.append(coin)
    n_coins_benchmark = len(coin_list[LIST])

    coins_in_none_position = sorted(coins_in_none_position)
    for coin in coins_in_none_position:
        position = coins_in_none_position.index(coin)
        for list_name in legend_list:
            if position % SETS_WSS_BACKEND == legend_list[list_name]["position"]:
                coin_list[list_name].append(coin)

    n_coins_binance = len(coin_list[LIST]) - n_coins_benchmark
    if logging:
        logger.info(f"on_open: {LIST}: {n_coins_benchmark} + {n_coins_binance}")

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
            if logging:
                logger.info(
                    f"Common elements between list{pair[0] + 1} and list{pair[1] + 1}: {common}"
                )

    coin_list = coin_list[LIST]
    if datetime.now().hour == 0 and datetime.now().minute == 0:
        if logging:
            logger.info(f"{LIST}: {coin_list}")
        if LIST == f"list{str(SETS_WSS_BACKEND)}":
            n_coins_back = position_threshold - n_summary
            n_discarded_coins = len(coins_no_longer_traded)
            n_coins_traded = len(most_traded_coins_first_filter)
            n_coins_brought_back = len(coins_brought_back)
            if logging:
                logger.info("")
                logger.info("##### BACKEND INFO #####")
                logger.info(
                    f"Number of coins traded in the last day FROM db_Bencharmk: {n_summary}"
                )
                logger.info(
                    f"Number of coins that have not been traded in the last day FROM db_Benchmark: {n_discarded_coins}"
                )
                logger.info(
                    f"Number of coins that are going to be brought back: {n_coins_back}"
                )
                logger.info(
                    f"Number of coins that are  brought back: {n_coins_brought_back}"
                )
                logger.info(f"Coins List Brought Back: {coins_brought_back}")
                logger.info(f"Total Number of Coins traded: {n_coins_traded}")
                logger.info("##### BACKEND INFO #####")
                logger.info("")

    return coin_list


if __name__ == "__main__":

    client = DatabaseConnection()
    SETS_WSS_BACKEND = int(os.getenv("SETS_WSS_BACKEND"))
    db_logger = client.get_db(DATABASE_LOGGING)
    database = client.get_db(DATABASE_MARKET)
    db_benchmark = client.get_db(DATABASE_BENCHMARK)
    db_logging = client.get_db(DATABASE_LOGGING)
    logging_collection = db_logging["Binance"]
    error_summary = initialize_new_doc_db_logging()

    logger = LoggingController.start_logging()
    logger.info("#########################")
    logger.info(f" {LIST}: script started")
    logger.info("#########################")
    position_threshold = int(os.getenv("COINS_TRADED"))
    doc_db = None
    prices = None
    now_ts = None
    connection_restart_running = False
    connection_restart_lock = threading.Lock()  # Add a lock to avoid race conditions

    legend_list = {}
    x = [i for i in range(1, SETS_WSS_BACKEND + 1)]
    for i in range(1, SETS_WSS_BACKEND + 1):
        list_name = f"list{i}"
        legend_list[list_name] = {
            "position": i % SETS_WSS_BACKEND,
            "best_coin": x[-i],
            "path": f"/backend/info/prices{i}.json",
        }

    TYPE_USDT = os.getenv("TYPE_USDT")
    coin_list = select_coins(
        LIST, db_benchmark, position_threshold, logging=False, type_USDT=TYPE_USDT
    )
    doc_db, prices, trade_ids = initializeVariables(coin_list)

    ws = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/stream?streams=",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_ping=on_ping,
    )
    threading.Thread(target=restart_connection).start()
    ws.run_forever()
