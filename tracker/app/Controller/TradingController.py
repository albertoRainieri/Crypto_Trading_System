import os, sys

sys.path.insert(0, "../../..")
from app.Controller.LoggingController import LoggingController
import json
from tracker.constants.constants import *
from datetime import datetime, timedelta
from tracker.app.Helpers.Helpers import round_, timer_func
from tracker.database.DatabaseConnection import DatabaseConnection
from tracker.app.Helpers.Helpers import getsubstring_fromkey, timer_func
import requests
import subprocess
from time import time
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import base64
from pymongo import DESCENDING
from pathlib import Path
import re
from time import sleep


class TradingController:

    def __init__(self) -> None:
        pass

    def returnRiskConfiguration(riskconfiguration, volatility_coin):
        """
        This function is used to return the riskconfiguration either if the riskconfiguration is groped by volatility or not
        """
        # CHECK IF the first random key of riskconfiguration is:
        # 1) '1' or '2', ... --> VOLATILITY_GROUPED = TRUE
        # 2) 'buy_vol_60m:0.85/vol_60m:15/timeframe:360' --> VOLATILITY_GROUPED = FALSE
        # print(riskconfiguration)
        # print(volatility_coin)
        if list(riskconfiguration.keys())[0].isdigit():
            if volatility_coin in riskconfiguration:
                return riskconfiguration[volatility_coin]
            else:
                return None
        else:
            return riskconfiguration

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

    def get_current_number_of_orderbook_scripts(db, event_keys):
        live_order_book_scripts_number = 0
        numbers_filled = []
        current_weight = 0
        for collection in event_keys:
            if collection != COLLECTION_ORDERBOOK_METADATA:

                minutes_timeframe = int(TradingController.extract_timeframe(collection))
                yesterday = datetime.now() - timedelta(minutes=minutes_timeframe)
                query = {"_id": {"$gt": yesterday.isoformat()}}
                docs = list(
                    db[collection].find(query, {"_id": 1, "number": 1, "weight": 1})
                )
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

    def buy_event_analysis(coin, obs, strategy_configuration, logger, volume_standings):
        """
        This functions triggers a market order if the condition of the risk strategies are met.
        The risk strategies are defined by "risk_configuration"
        """

        # event_keys = strategy_configuration['event_keys']
        event_keys = list(strategy_configuration["event_keys"].keys())
        FIRST_EVENT_KEY = True
        RESTART = "0"

        if coin in volume_standings["standings"]:
            ranking = int(volume_standings["standings"][coin]["rank"])
        else:
            return

        for event_key in event_keys:
            #logger.info(f'event_key: {event_key}')

            # check if coin is already on trade for this specific event, if True, pass
            vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe, lvl = (  getsubstring_fromkey(event_key) )

            if ranking > int(lvl):
                continue

            if obs[vol_field] == None or obs[buy_vol_field] == None:
                continue

            # if vol is higher than threshold, go ahead
            if obs[vol_field] >= vol_value:
                # determine if the buy_vol is below/above threshold. the operator changes if the threshold is below/above 0.5
                if ( buy_vol_value == 0 ) or ( buy_vol_value < 0.5 and obs[buy_vol_field] <= buy_vol_value ) \
                    or (buy_vol_value > 0.5 and obs[buy_vol_field] >= buy_vol_value):
                    #logger.info(f"Buy event analysis for {coin} with event_key {event_key} and id {obs['_id']}")

                    _id = obs["_id"]
                    # this variable sets the minute range, within which the coin can not be traded again
                    coin_exclusive_window_minutes = int( os.getenv("COIN_EXCLUSIVE_ORDERBOOK_WINDOW_MINUTES") )

                    coin_window_ts = (  datetime.now() - timedelta(minutes=coin_exclusive_window_minutes) ).isoformat()
                    # initalize metadata orderbook db collection
                    client = DatabaseConnection()
                    db = client.get_db(DATABASE_ORDER_BOOK)
                    db_collection = db[COLLECTION_ORDERBOOK_METADATA]

                    # determine if there were scripts for "$coin" in the last 3hours
                    docs = list( db_collection.find({"_id": {"$gt": coin_window_ts}, "coin": coin}))

                    # this is the case where the orderbook script was never executed for $coin in the last day
                    if len(docs) == 0:
                        #logger.info(f"Inserting new orderbook script for {coin} with event_key {event_key} and id {obs['_id']}")
                        end_observation = (datetime.now() + timedelta(minutes=int(timeframe))).isoformat()
                        db_collection.insert( {"_id": _id,"coin": coin,"event_key": event_key, "status": "pending", "buy_price": 0, "sell_price": 0,
                                                 "end_observation": end_observation, "riskmanagement_configuration": None, "ranking": ranking} )
                    elif len(docs) == 1:
                        # Get the _id from the first doc (assuming only one per coin in this window)
                        try:
                            doc_id = docs[0]["_id"]
                            new_end_observation = (datetime.now() + timedelta(minutes=int(timeframe))).isoformat()
                            db_collection.update_one( {"_id": doc_id}, {"$set": {"end_observation": new_end_observation, "status": "on_update"}})
                        except Exception as e:
                            logger.error(f"TradingController: Error updating orderbook metadata for {coin}: {e}")
                            continue
                    
                    return

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

    def restart_order_book_polling(logger):
        f = open("/tracker/riskmanagement/riskmanagement.json", "r")
        risk_configuration = json.loads(f.read())
        event_keys = list(risk_configuration["event_keys"].keys())
        event_keys_json = json.dumps({'event_keys': event_keys})

        RESTART = "1"

        logger.info("TradingController.restart_order_book_polling is started")
        client = DatabaseConnection()

        for event_key in event_keys:
            if event_key != COLLECTION_ORDERBOOK_METADATA:
                minutes_timeframe = int(TradingController.extract_timeframe(event_key))

                db = client.get_db(DATABASE_ORDER_BOOK)
                db_collection = db[event_key]
                now = datetime.now()
                yesterday = now - timedelta(minutes=minutes_timeframe)
                docs = list(
                    db_collection.find(
                        {"_id": {"$gt": yesterday.isoformat()}},
                        {"_id": 1, "coin": 1, "ranking": 1},
                    )
                )
                for doc in docs:
                    id = doc["_id"]
                    # if event trigger time window is still open
                    # if datetime.now() <  datetime.fromisoformat(id) + timedelta(minutes=minutes_timeframe):
                    coin = doc["coin"]
                    ranking = doc["ranking"]
                    logger.info(
                        f"Resuming Order Book for coin {coin}, event_key {event_key} and id {id}"
                    )
                    strategy_parameters = "None"
                    subprocess.Popen(
                        [
                            "python3",
                            "/tracker/trading/wss-start-order-book.py",
                            coin,
                            event_key,
                            id,
                            str(ranking),
                            RESTART,
                            strategy_parameters,
                            event_keys_json
                        ]
                    )
        client.close()

    def clean_db_trading(logger, db_logger, user_configuration):
        """
        This function is invoked during start of tracker container.
        This can clean db_trading orders that have not been closed and close orderd in Binance Platoform
        or it can resume the orders that have not been close updating some info in db_trading
        This function iterates this task for each user making attention of not starting a same process (wss-trading.py) more than once
        """
        complete_process_overview = {}
        client = DatabaseConnection()

        for user in user_configuration:
            db_name = DATABASE_TRADING + "_" + user
            db_trading = client.get_db(db_name)

            CLEAN_DB_TRADING = user_configuration[user]["clean_db_trading"]
            api_key_path = user_configuration[user]["api_key_path"]
            private_key_path = user_configuration[user]["private_key_path"]
            logger.info(
                f"Tracker Started in Mode Clean DB Trading: {CLEAN_DB_TRADING} for {user}"
            )
            query = {"on_trade": True}
            coins_live = list(db_trading[COLLECTION_TRADING_LIVE].find(query))
            coins_history = list(db_trading[COLLECTION_TRADING_HISTORY].find(query))

            if CLEAN_DB_TRADING:

                deleted_docs_live = 0
                updated_docs_history = 0

                if len(coins_live) == 0:
                    logger.info("There are no live coins to clean in db_trading")
                if len(coins_history) == 0:
                    logger.info("There are no history coins to clean in db_trading")

                # CLEAN DB_TRADING WITH "on_trade" == True
                for doc in coins_live:

                    # Query to find the document you want to delete (in this example, we delete by "_id")
                    query = {"_id": doc["_id"]}
                    trading_live = doc["trading_live"]
                    coin = doc["coin"]
                    id = doc["_id"]

                    # IF TRADING LIVE, THEN SEND SELL ORDER TO BINANCE and update the db
                    now = datetime.now()
                    update = {
                        "$set": {
                            "on_trade": False,
                            "exit_timestamp": now.isoformat(),
                        }
                    }
                    if trading_live:

                        quantity = doc["quantity"]
                        response, status_code = TradingController.create_order(
                            api_key_path=api_key_path,
                            private_key_path=private_key_path,
                            coin=coin,
                            side="SELL",
                            quantity=quantity,
                        )
                        # if REQUEST is succesful, then update db
                        if status_code == 200:
                            # get info
                            event_key = doc["event"]

                            response = response.json()
                            quantity_executed = float(response["executedQty"])
                            sell_price = float(response["price"])

                            # update database trading_history, and delete record for trading_live
                            update["$set"]["current_price"] = sell_price
                            update["$set"]["quantity_sell"] = quantity_executed
                            result_history = db_trading[
                                COLLECTION_TRADING_HISTORY
                            ].update_one(query, update)
                            result_live = db_trading[
                                COLLECTION_TRADING_LIVE
                            ].delete_one({"_id": id})

                            # notify/update dbs
                            msg = f"SELL Order Succeded for {coin}:{id}. origQty: {quantity}, execQty: {quantity_executed} for user {user} during DB Cleaning"
                            logger.info(msg)
                            db_logger[DATABASE_TRADING_INFO].insert_one(
                                {"_id": datetime.now().isoformat(), "msg": msg}
                            )

                            # kill the process

                        # if REQUEST is not succesfull, then notify to db_logger and do not kill the process
                        else:
                            msg_text = (
                                f"SELL Order FAILED for {coin}:{id}; user: {user}"
                            )
                            msg = {"msg": msg_text, "error": response.text}
                            logger.info(msg)
                            db_logger[DATABASE_TRADING_INFO].insert_one(
                                {"_id": datetime.now().isoformat(), "msg": msg}
                            )
                            db_logger[DATABASE_TRADING_ERROR].insert_one(
                                {"_id": datetime.now().isoformat(), "msg": msg}
                            )

                    # if trading_live is false, just update the db
                    else:
                        result_live = db_trading[COLLECTION_TRADING_LIVE].delete_one(
                            query
                        )
                        result_history = db_trading[
                            COLLECTION_TRADING_HISTORY
                        ].update_one(query, update)

                    if result_history.modified_count == 1:
                        msg = f"{coin}:{id} has been succesfully updated in DB History. Trading_live {trading_live}. User: {user}"
                        logger.info(msg)
                        updated_docs_history += 1
                    else:
                        msg = f"ERROR: {coin}:{id} has NOT been updated in DB History. Trading_live {trading_live}. User: {user}"
                        logger.info(msg)
                        db_logger[DATABASE_TRADING_ERROR].insert_one(
                            {"_id": datetime.now().isoformat(), "msg": msg}
                        )

                    if result_live.deleted_count == 1:
                        msg = f"{coin}:{id} has been succesfully deleted in DB Live. Trading_live {trading_live}. User: {user}"
                        logger.info(msg)
                        deleted_docs_live += 1
                    else:
                        msg = f"ERROR: {coin}:{id} has NOT been deleted in DB Live. Trading_live {trading_live}. User: {user}"
                        logger.info(msg)
                        db_logger[DATABASE_TRADING_ERROR].insert_one(
                            {"_id": datetime.now().isoformat(), "msg": msg}
                        )

                logger.info(
                    f"Total Deleted Documents in DB Live: {deleted_docs_live} in DB_Trading for User {user}"
                )
                logger.info(
                    f"Total Updated Documents in DB History: {updated_docs_history} in DB_Trading for User {user}"
                )
                # TODO: DELETE ORDER IN BINANCE PLATFORM: we need an ORDER_ID to be stored IN DB_TRADING_LIVE

            else:
                for doc in coins_live:
                    coin = doc["coin"]
                    purchase_price = doc["purchase_price"]
                    event_key = doc["event"]
                    (
                        vol_field,
                        vol_value,
                        buy_vol_field,
                        buy_vol_value,
                        timeframe,
                        lvl,
                    ) = getsubstring_fromkey(event_key)

                    risk_management_configuration = json.dumps(
                        doc["risk_configuration"]
                    )
                    id = doc["_id"]
                    logger.info(
                        f"WSS Connection has resumed for {coin}: {id}. User: {user}"
                    )

                    # avoid to start the same process more than once
                    process_key = event_key + coin
                    if process_key not in complete_process_overview:
                        process = subprocess.Popen(
                            [
                                "python3",
                                "/tracker/trading/wss-trading.py",
                                coin,
                                id,
                                str(purchase_price),
                                str(timeframe),
                                risk_management_configuration,
                            ]
                        )
                        complete_process_overview[process_key] = process.pid
                        pid = process.pid
                    else:
                        pid = complete_process_overview[process_key]
                        logger.info(f"Process already started by another user")

                    query = {"_id": id}

                    # add this to update the current trades with old configuration
                    if "trading_live" not in doc:
                        update_data = {
                            "$set": {
                                "pid": pid,
                                "trading_live": False,
                                "exit_timestamp": datetime.fromtimestamp(0).isoformat(),
                                "quantity": doc["investment_amount"]
                                / doc["purchase_price"],
                            }
                        }
                    else:
                        update_data = {"$set": {"pid": pid}}

                    db_trading[COLLECTION_TRADING_HISTORY].update_one(
                        query, update_data
                    )
                    db_trading[COLLECTION_TRADING_LIVE].update_one(query, update_data)

            del db_trading
        client.close()

    @timer_func
    def checkPerformance_test(db_trading, logger):
        """
        This function has been used to test the performances of trading simulations. It is replace by "checkPerformance" function
        """
        events_history = list(db_trading[COLLECTION_TRADING_HISTORY].find())

        now = datetime.now()
        one_week_ago = now - timedelta(days=7)
        one_month_ago = now - timedelta(days=30)
        three_months_ago = now - timedelta(days=90)
        six_months_ago = now - timedelta(days=180)
        # one_year_ago = now - timedelta(days=365)

        total_performance = 0
        total_performance_list = []
        performance_one_week = []
        performance_one_month = []
        performance_three_month = []
        performance_six_month = []
        total_events = len(events_history)

        # first_datetime_investment = datetime.fromisoformat(events_history[0]['_id'])

        if len(events_history) > 0:

            for event in events_history:
                profit = event["profit"]
                investment_amount = float(event["investment_amount"])
                datetime_investment = datetime.fromisoformat(event["_id"])
                total_performance += investment_amount * profit
                total_performance_list.append(profit)

                if datetime_investment > one_week_ago:
                    performance_one_week.append(profit)
                    performance_one_month.append(profit)
                    performance_three_month.append(profit)
                    performance_six_month.append(profit)

                elif datetime_investment > one_month_ago:
                    performance_one_month.append(profit)
                    performance_three_month.append(profit)
                    performance_six_month.append(profit)

                elif datetime_investment > three_months_ago:
                    performance_three_month.append(profit)
                    performance_six_month.append(profit)

                elif datetime_investment > six_months_ago:
                    performance_six_month.append(profit)

            profit_one_week = round_(np.mean(performance_one_week), 4)
            profit_one_month = round_(np.mean(performance_one_month), 4)
            profit_three_months = round_(np.mean(performance_three_month), 4)
            profit_six_months = round_(np.mean(performance_six_month), 4)
            total_profit = round_(np.mean(total_performance_list), 4)
            total_performance = round_(total_performance, 2)

            db_doc = {
                "total_gain": total_performance,
                "total_profit": total_profit,
                "one_week_profit": profit_one_week,
                "one_month_profit": profit_one_month,
                "three_months_profit": profit_three_months,
                "six_months_profit": profit_six_months,
                "total_events": total_events,
            }

            db_performance = list(
                db_trading[COLLECTION_TRADING_PERFORMANCE_TESTING].find()
            )

            if len(db_performance) == 0:
                db_trading[COLLECTION_TRADING_PERFORMANCE_TESTING].insert(db_doc)
            else:
                id = db_performance[0]["_id"]
                query = {"_id": id}
                update = {"$set": db_doc}
                db_trading[COLLECTION_TRADING_PERFORMANCE_TESTING].update_one(
                    query, update
                )

    @timer_func
    def checkPerformance(client, logger, user_configuration):
        """
        this function check the performance of the trading bot if mode TRADING_LIVE is enabled
        """
        SYS_ADMIN = os.getenv("SYS_ADMIN")

        for user in user_configuration:
            TRADING_LIVE = user_configuration[user]["trading_live"]

            # in case regular user is not on trading_live, then skip
            if TRADING_LIVE == False and user != SYS_ADMIN:
                continue

            db_name = DATABASE_TRADING + "_" + user
            db_trading = client.get_db(db_name)

            id = datetime.now().isoformat()

            # IF TRADING LIVE fetch only those trades that have been live, and select the DB COLLECTION_PERFORMANCE FOR LIVE_TRADING
            if TRADING_LIVE:
                COLLECTION_PERFORMANCE = COLLECTION_TRADING_PERFORMANCE
                last_record = db_trading[COLLECTION_PERFORMANCE].find_one(
                    {}, sort=[("_id", DESCENDING)]
                )
                coins_history = list(
                    db_trading[COLLECTION_TRADING_HISTORY]
                    .find({"on_trade": False, "trading_live": True})
                    .sort("exit_timestamp", DESCENDING)
                )
            # TRADING_LIVE mode is not enabled, then consider all the trades (live and not) and save the performance to the DB COLLECTION_PERFORMANCE for NOT LIVE_TRADING
            else:
                COLLECTION_PERFORMANCE = COLLECTION_TRADING_PERFORMANCE_TESTING
                last_record = db_trading[COLLECTION_PERFORMANCE].find_one(
                    {}, sort=[("_id", DESCENDING)]
                )
                coins_history = list(
                    db_trading[COLLECTION_TRADING_HISTORY]
                    .find({"on_trade": False})
                    .sort("exit_timestamp", DESCENDING)
                )

            n_coins_history = len(coins_history)

            if last_record == None:
                total_events_recorded = 0
                absolute_profit = 0
                total_investment = 0
                clean_profit = 0
                weighted_performance_percentage = 0
                new_clean_profit = 0
            else:
                total_events_recorded = last_record["total_events_recorded"]
                absolute_profit = last_record["absolute_profit"]
                total_investment = last_record["total_investment"]
                clean_profit = last_record["clean_profit"]
                weighted_performance_percentage = last_record[
                    "weighted_performance_percentage"
                ]
                new_clean_profit = last_record["clean_profit"]

            if n_coins_history != total_events_recorded:
                new_events_to_register = n_coins_history - total_events_recorded
                logger.info(
                    f"There are {new_events_to_register} events to register in {COLLECTION_TRADING_BALANCE_ACCOUNT} collection"
                )
                for event_to_add in coins_history[:new_events_to_register]:
                    logger.info(
                        f'{event_to_add["coin"]}:{event_to_add["_id"]} has been added'
                    )
                    absolute_profit += (
                        event_to_add["investment_amount"] * event_to_add["profit"]
                    )
                    total_investment += event_to_add["investment_amount"]
                    weighted_performance_percentage = absolute_profit / total_investment
                    if total_events_recorded != 0:
                        new_clean_profit = (
                            (clean_profit * total_events_recorded)
                            + event_to_add["profit"]
                        ) / (total_events_recorded + 1)
                    else:
                        clean_profit = event_to_add["profit"]
                        new_clean_profit = event_to_add["profit"]
                    total_events_recorded += 1
            else:
                logger.info(
                    f"No events to register the {COLLECTION_PERFORMANCE} performance"
                )

            doc_db = {
                "_id": id,
                "absolute_profit": round_(absolute_profit, 2),
                "weighted_performance_percentage": round_(
                    weighted_performance_percentage, 4
                ),
                "clean_profit": round_(new_clean_profit, 4),
                "total_investment": total_investment,
                "total_events_recorded": total_events_recorded,
            }

            db_trading[COLLECTION_PERFORMANCE].insert(doc_db)
            del db_trading

    def makeRequest(api_path, method, params, api_key_path, private_key_path):

        f = open(api_key_path, "r")
        # API_KEY = f.read()

        byte_string = Path(api_key_path).read_text()
        # Decoding the bytes object to a regular string
        # API_KEY = byte_string.decode('utf-8')

        # Removing leading/trailing whitespace and newline characters
        API_KEY = byte_string.strip()

        timestamp = int(time() * 1000)
        params["timestamp"] = timestamp

        with open(private_key_path, "rb") as f:
            private_key = load_pem_private_key(data=f.read(), password=None)

        payload = "&".join([f"{param}={value}" for param, value in params.items()])
        signature = base64.b64encode(private_key.sign(payload.encode("ASCII")))
        params["signature"] = signature

        if method == "GET":
            response = requests.request(
                method=method,
                url="https://api.binance.com" + api_path,
                params=params,
                headers={
                    "X-MBX-APIKEY": API_KEY,
                },
            )
        else:
            print(params)
            response = requests.request(
                method=method,
                url="https://api.binance.com" + api_path,
                data=params,
                headers={
                    "X-MBX-APIKEY": API_KEY,
                },
            )

        status_code = response.status_code
        response = json.loads(response.text)
        return response, status_code

    def get_asset_composition(api_key_path, private_key_path):

        # make request for fetching asset composition
        api_path = "/api/v3/account"
        params = {}
        method = "GET"
        data, status_code = TradingController.makeRequest(
            api_path=api_path,
            params=params,
            method=method,
            api_key_path=api_key_path,
            private_key_path=private_key_path,
        )

        if status_code == 200:
            # fetch asset whose balance is different from zero

            data = data["balances"]
            # print(data)
            current_wallet = {}
            for asset in data:
                if float(asset["free"]) != 0:
                    current_wallet[asset["asset"]] = float(asset["free"])

            return current_wallet, status_code
        else:
            msg = f"Status Code: {status_code}: get_asset_composition did not work as expected"
            return msg, status_code

    def load_user_configuration():
        """Loads a JSON file in a single line (excluding import)."""
        f = open("/tracker/user_configuration/userconfiguration.json", "r")
        user_configuration = json.loads(f.read())
        return user_configuration

    def create_order(
        coin,
        side,
        type,
        usdt=None,
        quantity=None,
        user=os.getenv("SYS_ADMIN"),
        test=True,
        user_configuration=load_user_configuration(),
    ):
        # # only for testing
        # if coin == None:
        #     coin = 'BTCUSDT',
        #     side = 'BUY', #BUY, SELL
        #     usdt = 10
        #     type = 'MARKET'
        if coin[-4:] == "USDT":
            coin = coin.replace("USDT", "USDC")

        api_key_path = user_configuration[user]["api_key_path"]
        private_key_path = user_configuration[user]["private_key_path"]

        params = {"symbol": coin, "side": side, "type": type}
        if test:
            api_path = "/api/v3/order/test"
            params["computeCommissionRates"] = "true"
        else:
            api_path = "/api/v3/order"

        # Use "quoteOrderQty" for executing BUY orders with USDT
        if quantity == None:
            params["quoteOrderQty"] = str(usdt)
        # Use "quantity" for executing SELL orders with number of crypto units (i.e. quantity)
        else:
            params["quantity"] = str(quantity)

        method = "POST"
        response, status_code = TradingController.makeRequest(
            api_path=api_path,
            params=params,
            method=method,
            api_key_path=api_key_path,
            private_key_path=private_key_path,
        )

        if status_code == 200:
            info_buy = {}
            # clientOrderId = response['clientOrderId']
            # transactTime = datetime.fromisoformat(response['transactTime']/1000)
            # origQty = response['origQty']
            # executedQty = response['executedQty']
            # origQuoteOrderQty = response['origQuoteOrderQty']

        print(response)
        print(status_code)
        return response, status_code

    def get_balance_account(
        client=DatabaseConnection(),
        logger=LoggingController.start_logging(),
        user_configuration=load_user_configuration(),
        only_admin=True,
        user_x=None,
    ):
        """
        if you want to get the balance account of sysadmin: only_admin=True
        if you want to get the balance account of a user: only_admin=False & user_x=<username>
        if you want to get the balance account of all users: only_admin=False & user_x=None
        """

        balance_account = {}

        for user in user_configuration:
            sys_admin = os.getenv("SYS_ADMIN")
            if only_admin and user != sys_admin:
                continue
            elif not only_admin and user_x != None and user_x not in user_configuration:
                return ValueError("The user is not present in user_configuration")
            elif not only_admin and user_x != None and user != user_x:
                continue

            TRADING_LIVE = user_configuration[user]["trading_live"]
            api_key_path = user_configuration[user]["api_key_path"]
            private_key_path = user_configuration[user]["private_key_path"]

            response, status_code = TradingController.get_asset_composition(
                api_key_path, private_key_path
            )
            balance_account[user] = response

        return balance_account

    def get_usdc_available(
        client=DatabaseConnection(),
        logger=LoggingController.start_logging(),
        user_configuration=load_user_configuration(),
        only_admin=True,
        user_x=None,
    ):
        balance_account = TradingController.get_balance_account(
            client=client,
            logger=logger,
            user_configuration=user_configuration,
            only_admin=only_admin,
            user_x=user_x,
        )
        print(balance_account)
        if user_x != None:
            if "USDC" in balance_account[user_x]:
                return balance_account[user_x]["USDC"]
            else:
                return 0
        else:
            ValueError("Provide Username")

    # @timer_func
    def save_balance_account(
        client=DatabaseConnection(),
        logger=LoggingController.start_logging(),
        user_configuration=load_user_configuration(),
    ):
        # t1 = time()
        SYS_ADMIN = os.getenv("SYS_ADMIN")

        for user in user_configuration:
            TRADING_LIVE = user_configuration[user]["trading_live"]
            db_name = DATABASE_TRADING + "_" + user
            db_trading = client.get_db(db_name)

            if user != SYS_ADMIN and TRADING_LIVE == False:
                continue

            api_key_path = user_configuration[user]["api_key_path"]
            private_key_path = user_configuration[user]["private_key_path"]

            now = datetime.now()
            now_isoformat = now.isoformat()
            current_day_of_the_week = now.weekday()

            response, status_code = TradingController.get_asset_composition(
                api_key_path, private_key_path
            )
            balance_account = 0
            last_record = db_trading[COLLECTION_TRADING_BALANCE_ACCOUNT].find_one(
                {}, sort=[("_id", DESCENDING)]
            )

            # the "CREATE_NEW" flag is used to create a new record every week.
            # this record has the goal to re-compute the "investment_amount" based on the past balance account (i.e. last_balance_account)
            CREATE_NEW = False

            # INITIALIZE VARIABLES
            # in case the BALANCE_ACCOUNT_COLLECTION is empy, initialize variables
            if last_record == None:
                CREATE_NEW = True
                last_investment_amount = user_configuration[user][
                    "initialized_investment_amount"
                ]
                average_balance_account = user_configuration[user][
                    "initialized_balance_account"
                ]
            else:
                last_investment_amount = last_record["investment_amount"]
                average_balance_account = last_record["average_balance"]
                query = {"_id": last_record["_id"]}

                # if It is a new week (it is Monday)
                if current_day_of_the_week == 0 and now - datetime.fromisoformat(
                    last_record["_id"]
                ) > timedelta(days=6):
                    CREATE_NEW = True

            if status_code == 200:
                if "USDT" in response:
                    balance_account_usdt = response["USDT"]
                    balance_account = balance_account_usdt
                else:
                    balance_account_usdt = 0
                    balance_account = 0

                coins_live = list(
                    db_trading[COLLECTION_TRADING_LIVE].find({"on_trade": True})
                )

                # get balance account, absolute profit and total investment of only official events
                for coin_live in coins_live:

                    # if event is not live (simulated only), then skip

                    # if TRADING_LIVE env var is ON, and coin was not started as live, then skip
                    if TRADING_LIVE and not coin_live["trading_live"]:
                        continue

                    # get balance account
                    # balance_account += (coin_live["purchase_price"] * coin_live["quantity"]) * coin_live["profit"]
                    balance_account += (
                        coin_live["current_price"] * coin_live["quantity"]
                    )

                update_data = {"$set": {}}

                # Update the average balance account considering the minutes passed from the beginning of the week
                # balance_account refers to the most updated balanace account
                # average_balance refers to average of balance account in the last week (i.e. from Monday)
                if not CREATE_NEW:
                    minutes_passed = (
                        current_day_of_the_week * 24 + now.hour
                    ) * 60 + now.minute

                    last_balance = last_record["average_balance"]
                    # logger.info(f'last_record_balance_account: {last_balance}')
                    # logger.info(f'minutes passed {minutes_passed}')
                    # logger.info(f' balance_account now: {balance_account}')

                    average_balance = (
                        (last_record["average_balance"] * (minutes_passed - 1))
                        + balance_account
                    ) / minutes_passed
                    update_data["$set"]["average_balance"] = average_balance
                    update_data["$set"]["balance_account"] = round_(balance_account, 2)
                    update_data["$set"]["balance_usdt"] = balance_account_usdt
                    update_data["$set"]["last_update"] = now_isoformat

                    db_trading[COLLECTION_TRADING_BALANCE_ACCOUNT].update_one(
                        query, update_data
                    )
                else:
                    # every week a new record is inserted in this collection.
                    # The investment_amount is computed, considering an expected loss of "MAXIMUM_LOSS" which is (e.g. 2) times the investment amount.
                    # it also takes account of the scenario where a lot of events are triggered "MAX_EVENTS_SYNCED".
                    # this considerations should take the risk of sudden losses and of the reduced balance account
                    # for this reason, the investment amount is never reduced, in order to keep the investment as more consistent as possible.
                    start_of_week = (
                        now - timedelta(days=current_day_of_the_week)
                    ).replace(hour=0, minute=0, second=0, microsecond=0)
                    # weekly id
                    id = start_of_week.isoformat()
                    # maximum synced events (e.g. 15)
                    maximum_events_sync = int(os.getenv("MAX_EVENTS_SYNCED"))
                    # maximum loss in terms of multiple of investment_amount (e.g. 2)
                    maximum_loss = float(os.getenv("MAXIMUM_LOSS"))
                    # potential_investment_amount (this is generally a float number), but I want a multiple of 5 to make the investments more consistent
                    # the value of the investment_amount depends on the performance of the average_balance_account of the last week.
                    potential_investment_amount = average_balance_account / (
                        maximum_events_sync + maximum_loss
                    )
                    # maximum between the average balance account of the last week and the last_investment_amount
                    if TRADING_LIVE:
                        investment_amount = max(
                            int(
                                potential_investment_amount
                                - int(potential_investment_amount % 5)
                            ),
                            last_investment_amount,
                        )
                    else:
                        investment_amount = 100
                    # finally I initialize the balance account and average balance account with the "new" balance_account
                    doc_db = {
                        "_id": id,
                        "balance_account": round_(balance_account, 2),
                        "balance_usdt": balance_account_usdt,
                        "average_balance": balance_account,
                        "investment_amount": investment_amount,
                        "last_update": now_isoformat,
                    }
                    db_trading[COLLECTION_TRADING_BALANCE_ACCOUNT].insert(doc_db)

                # t2 = time()
                # time_spent = round_(t2 - t1,2)
                # logger.info(f'Time spent for getting balance account info {time_spent}')

            else:
                logger.info(
                    "ERROR: WAS NOT ABLE TO RETRIEVE INFO FROM /v3/account Binance API"
                )
                logger.info(response)

    def binance_order_book_request(
        coin, limit=5000, logger=LoggingController.start_logging()
    ):
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
            sleep(retry_after * 2)
        return response.text, status_code

    def get_statistics(resp):
        """
        This function makes a first pre-processing of the orderbook data, the output is then analyzed for buy-events
        """
        json_resp = json.loads(resp)
        total_ask_volume = 0
        total_bid_volume = 0

        # lowest_bid_price = json_resp['bids'][-1][0]
        # highest_ask_price = json_resp['asks'][-1][0]
        best_bid_price = float(json_resp["bids"][0][0])
        best_ask_price = float(json_resp["asks"][0][0])

        max_n_orders = max(len(json_resp["bids"]), len(json_resp["asks"]))

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
            max_n_orders,
        )

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
            decimal_index = str_num.index(".")
            return len(str_num) - decimal_index
        except ValueError:
            # If no decimal point is found, it's an integer
            return 1

    def compute_quantity_buy(usdc, coin, current_ask_price):
        with open("minimal_notional.json", "r") as f:
            infocoin = json.load(f)
        if usdc < int(infocoin[coin]["MIN_NOTIONAL"]):
            return None
        n_decimals_smallest_unit = count_decimals(float(infocoin[coin]["LOT_SIZE"]))
        # quantity = round_(usdc / current_ask_price, n_decimals_smallest_unit)
        quantity = math.floor(usdc / current_ask_price / 10 ** (-3)) * 10 ** (-3)
        x = quantity * current_ask_price
        print(f"Expected usd spent: {x}")

    def adjust_quantity_sell(quantity, coin):
        with open("minimal_notional.json", "r") as f:
            infocoin = json.load(f)
        n_decimals_smallest_unit = count_decimals(float(infocoin[coin]["LOT_SIZE"]))
        quantity = round_(quantity, n_decimals_smallest_unit)
        return quantity

    def get_minimal_notional_value(logger):
        url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
        response = requests.request(method="GET", url=url)
        # print(response)
        status_code = response.status_code
        response = json.loads(response.text)

        # print(status_code)
        # print(response.keys())
        if status_code == 200:
            try:
                minimal_notional_value = {}
                for obj in response["symbols"]:
                    coin = obj["symbol"]
                    if "USDT" in coin:
                        # coin = coin[:-1]
                        info = {"MIN_NOTIONAL": None, "LOT_SIZE": None}
                        for obj_i in obj["filters"]:
                            if (
                                "filterType" in obj_i
                                and obj_i["filterType"] == "MIN_NOTIONAL"
                            ):  # , 'LOT_SIZE']:
                                info["MIN_NOTIONAL"] = obj_i["notional"]
                            if (
                                "filterType" in obj_i
                                and obj_i["filterType"] == "LOT_SIZE"
                            ):  # , 'LOT_SIZE']:
                                info["LOT_SIZE"] = obj_i["stepSize"]
                        minimal_notional_value[coin] = info

                n_coin_minimal_notional_value = len(list(minimal_notional_value.keys()))
                logger.info(
                    f"There are {n_coin_minimal_notional_value} coins in minimal notional value"
                )
                min_coins_minimum_coins = 100
                if n_coin_minimal_notional_value > min_coins_minimum_coins:
                    with open(
                        f"/tracker/json/minimal_notional.json", "w"
                    ) as f:  # 'w' opens the file for writing.
                        json.dump(minimal_notional_value, f, indent=4)
                else:
                    logger.info(
                        f"WARNING: Number of records in minimal_notional.json is less than {min_coins_minimum_coins}"
                    )
            except Exception as e:
                logger.info(
                    f"WARNING: Request for Minimum Notional Value Failed. Error {e}"
                )
        else:
            logger.info(
                f"WARNING: Request for Minimum Notional Value Failed. STATUS CODE {url}: {status_code}"
            )
