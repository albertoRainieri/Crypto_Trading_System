import os,sys
sys.path.insert(0,'../../..')
from app.Controller.LoggingController import LoggingController
import json
from tracker.constants.constants import *
from datetime import datetime, timedelta
from tracker.app.Helpers.Helpers import round_, timer_func
import numpy as np
from tracker.database.DatabaseConnection import DatabaseConnection
from tracker.app.Helpers.Helpers import getsubstring_fromkey, timer_func
import requests
import subprocess
from time import time
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import base64
from pymongo import DESCENDING



class TradingController:
    
    def __init__(self) -> None:
        pass
    
    # ASYNC ENABLED call. Enable only if cpu support is high. 4CPUs are probably minimum
    #async def check_event_triggering(coin, obs, volatility_coin, logger, db_trading, db_logger, trading_coins_list, trading_configuration, last_coin, first_coin):
    
    # ASYNC DISABLED. THIS IS PREFERRED CHOICE even if CPU Support is high
    def check_event_triggering(coin, obs, volatility_coin, logger, db_trading, db_logger, trading_coins_list, trading_configuration, last_coin, first_coin):

        '''
        This functions triggers a market order if the condition of the risk strategies are met.
        The risk strategies are defined by "trading_configuration"
        '''
        # STRUCTURE TRADING CONFIGURATION
        # {
        #     '<volatility_1>' : {
        #         '<key_1>': {
        #             'riskmanagement_conf': {
        #                 'golden_zone': '<GOLDEN_ZONE>',
        #                 'step_golden': '<STEP>'
        #                 'step_nogolden': '<STEP_NOGOLDEN>'
        #                 'extra_timeframe': '<EXTRA_TIMEFRAME>'
        #             }
        #         }
        #     }
        # }

        # if coin == last_coin or coin == first_coin:
        #     now = datetime.now()
        #     now_isoformat = now.isoformat()
        #     logger.info(f'TradingController: {coin}')

        if volatility_coin in trading_configuration:
            
            for event_key in trading_configuration[volatility_coin]:

                # check if coin is already on trade for this specific event, if True, pass
                COIN_ON_TRADING = False
    
                vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe = getsubstring_fromkey(event_key)

                if obs[vol_field] == None or obs[buy_vol_field] == None:
                    continue

                if obs[vol_field] >= float(vol_value) and obs[buy_vol_field] >= float(buy_vol_value):

                    if len(trading_coins_list) != 0:
                        for coin_on_trade in trading_coins_list:
                            if coin_on_trade['coin'] == coin and coin_on_trade['event'] == event_key:
                                COIN_ON_TRADING = True
                                #logger.info(f'{coin} is already on trade for {event_key} skipping')
                    
                    
                    if not COIN_ON_TRADING:
                        last_record = db_trading[COLLECTION_TRADING_BALANCE_ACCOUNT].find_one({}, sort=[("_id", DESCENDING)])
                        if last_record != None:
                            investment_amount = last_record['investment_amount']
                        else:
                            investment_amount = int(os.getenv('INITIALIZED_INVESTMENT_AMOUNT'))
                        # EVENT TRIGGERED
                        # get id and purchase_price
                        id = datetime.now().isoformat()
                        
                        ################################################################
                        TRADING_LIVE = bool(int(os.getenv('TRADING_LIVE')))

                        
                        quantity = round_(investment_amount / obs['price'],8)

                        if TRADING_LIVE:
                            response, status_code = TradingController.create_order(coin=coin, side="BUY", usdt=investment_amount)
                            if status_code == 200:
                                response = response.json()
                                
                                # let's get the real quantity and price executed, thus the investment_amount
                                quantity = float(response["executedQty"])
                                purchase_price = float(response["price"])
                                investment_amount = quantity * purchase_price
                                trading_live = True
                                msg = f"{coin} - Event Triggered: {vol_field}:{vol_value} - {buy_vol_field}:{buy_vol_value} Live Trading: {TRADING_LIVE}"
                            else:
                                msg_text = f'Status code: {status_code} BUY Order Failed for {coin}. key event: {event_key}'
                                msg = {'msg': msg_text, 'error': response.text}
                                trading_live = False
                                purchase_price = obs['price']
                        else:
                            trading_live = False
                            msg = f"{coin} - Event Triggered: {vol_field}:{vol_value} - {buy_vol_field}:{buy_vol_value} Live Trading: {TRADING_LIVE}"
                            purchase_price = obs['price']
                        ################################################################
                        logger.info(msg)
                        
                        # get risk management configuration
                        risk_management_configuration = json.dumps(trading_configuration[volatility_coin][event_key])

                        # Start Subprocess for "coin". This will launch a wss connection for getting bid price coin in real time
                        process = subprocess.Popen(["python3", "/tracker/trading/wss-trading.py", coin, id, str(purchase_price), str(timeframe), risk_management_configuration])
                        pid = process.pid
                        
                        # send query to db_trading. for logging
                        doc_db = {'_id': id, 'coin': coin, 'profit': 0, 'purchase_price': purchase_price, 'current_price': purchase_price,
                                   'quantity': quantity,'on_trade': True, 'trading_live': trading_live, 'event': event_key, 'investment_amount': investment_amount,
                                     'exit_timestamp': datetime.fromtimestamp(0).isoformat(), 'risk_configuration': trading_configuration[volatility_coin][event_key], 'pid': pid}
                        
                        db_trading[COLLECTION_TRADING_LIVE].insert_one(doc_db)
                        db_trading[COLLECTION_TRADING_HISTORY].insert_one(doc_db)
                        db_logger[DATABASE_TRADING_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})

    

    def clean_db_trading(logger, db_trading, db_benchmark, db_logger):
        '''
        This function is invoked during start of tracker container.
        This can clean db_trading orders that have not been closed and close orderd in Binance Platoform
        or it can resume the orders that have not been close updating some info in db_trading
        '''
        CLEAN_DB_TRADING = bool(int(os.getenv('CLEAN_DB_TRADING')))
        logger.info(f'Tracker Started in Mode Clean DB Trading: {CLEAN_DB_TRADING}')
        query = {"on_trade": True}
        coins_live = list(db_trading[COLLECTION_TRADING_LIVE].find(query))
        coins_history = list(db_trading[COLLECTION_TRADING_HISTORY].find(query))

        if CLEAN_DB_TRADING:

            deleted_docs_live = 0
            updated_docs_history = 0

            if len(coins_live) == 0:
                logger.info('There are no live coins to clean in db_trading')
            if len(coins_history) == 0:
                logger.info('There are no history coins to clean in db_trading')

            # CLEAN DB_TRADING WITH "on_trade" == True
            for doc in coins_live:

                # Query to find the document you want to delete (in this example, we delete by "_id")
                query = {"_id": doc['_id']}
                trading_live = doc['trading_live']
                coin = doc['coin']
                id = doc['_id']
                
                # IF TRADING LIVE, THEN SEND SELL ORDER TO BINANCE and update the db
                if trading_live:
                    now = datetime.now()
                    update = {'$set': {'on_trade': False,
                        'exit_timestamp': now.isoformat(),
                        }
                    }   
                    quantity = doc['quantity'] 
                    response, status_code = TradingController.create_order(coin=coin, side="SELL", quantity=quantity)
                    # if REQUEST is succesful, then update db
                    if status_code == 200:
                        # get info
                        event_key = doc['event']

                        response = response.json()
                        quantity_executed = float(response["executedQty"])
                        sell_price = float(response["price"])

                        # update database trading_history, and delete record for trading_live
                        update['$set']['current_price'] = sell_price
                        update['$set']['quantity_sell'] = quantity_executed
                        result_history = db_trading[COLLECTION_TRADING_HISTORY].update_one(query, update)
                        result_live = db_trading[COLLECTION_TRADING_LIVE].delete_one({'_id': id})


                        # notify/update dbs
                        msg = f'SELL Order Succeded for {coin}:{id}. origQty: {quantity}, execQty: {quantity_executed}'
                        logger.info(msg)
                        db_logger[DATABASE_TRADING_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})

                        # kill the process

                    # if REQUEST is not succesfull, then notify to db_logger and do not kill the process
                    else:
                        msg_text = f'SELL Order FAILED for {coin}:{id}'
                        msg = {'msg': msg_text, 'error': response.text}
                        logger.info(msg)
                        db_logger[DATABASE_TRADING_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
                        db_logger[DATABASE_TRADING_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})

                # if trading_live is false, just update the db
                else:
                    result_live = db_trading[COLLECTION_TRADING_LIVE].delete_one(query)
                    update_data = {"$set": {"on_trade": False}}
                    result_history = db_trading[COLLECTION_TRADING_HISTORY].update_one(query, update_data)


                if result_history.modified_count == 1:
                    msg = f'{coin}:{id} has been succesfully delete in DB Live. Trading_live {trading_live}'
                    logger.info(msg)
                    updated_docs_history += 1
                else:
                    msg = f'ERROR: {coin}:{id} has NOT been deleted in DB Live. Trading_live {trading_live}'
                    logger.info(msg)
                    db_logger[DATABASE_TRADING_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})

                if result_live.deleted_count == 1:
                    msg = f'{coin}:{id} has been succesfully update in DB History. Trading_live {trading_live}'
                    logger.info(msg)
                    deleted_docs_live += 1
                else:
                    msg = f'ERROR: {coin}:{id} has NOT been updated in DB History. Trading_live {trading_live}'
                    logger.info(msg)
                    db_logger[DATABASE_TRADING_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})

                
            logger.info(f"Total Deleted Documents: {deleted_docs_live} in DB_Trading")
                # TODO: DELETE ORDER IN BINANCE PLATFORM: we need an ORDER_ID to be stored IN DB_TRADING_LIVE

        else:
            for doc in coins_live:
                coin = doc['coin']
                purchase_price = doc['purchase_price']
                event_key = doc['event']
                vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe = getsubstring_fromkey(event_key)

                # volume_coin = list(db_benchmark[coin].find({}, {'_id': 1, 'volume_30_avg': 1, 'volume_30_std': 1, 'Last_30_Trades': 1}))
                # avg_volume_1_month = volume_coin[0]['volume_30_avg']
                # std_volume_1_month = volume_coin[0]['volume_30_std']
                # volatility_coin = str(int(std_volume_1_month / avg_volume_1_month))

                # f = open ('/tracker/riskmanagement/riskmanagement.json', "r")
                # trading_configuration = json.loads(f.read())
                risk_management_configuration = json.dumps(doc['risk_configuration'])
                id = doc['_id']
                logger.info(f'WSS Connection has resumed for {coin}: {id}')
                process = subprocess.Popen(["python3", "/tracker/trading/wss-trading.py", coin, id, str(purchase_price), str(timeframe), risk_management_configuration])
                query = {"_id": id}

                # add this to update the current trades with old configuration
                if "trading_live" not in doc:
                    update_data = {"$set": {"pid": process.pid, "trading_live": False,
                                            "exit_timestamp": datetime.fromtimestamp(0).isoformat(),
                                            "quantity": doc['investment_amount'] / doc['purchase_price']
                                            }}
                else:
                    update_data = {"$set": {"pid": process.pid}}

                db_trading[COLLECTION_TRADING_HISTORY].update_one(query, update_data)
                db_trading[COLLECTION_TRADING_LIVE].update_one(query, update_data)

    @timer_func
    def checkPerformance_test(db_trading, logger):
        '''
        This function has been used to test the performances of trading simulations. It is replace by "checkPerformance" function
        '''
        events_history = list(db_trading[COLLECTION_TRADING_HISTORY].find())


        now = datetime.now()
        one_week_ago = now - timedelta(days=7)
        one_month_ago = now - timedelta(days=30)
        three_months_ago  = now - timedelta(days=90)
        six_months_ago  = now - timedelta(days=180)
        #one_year_ago = now - timedelta(days=365)

        total_performance = 0
        total_performance_list = []
        performance_one_week = []
        performance_one_month = []
        performance_three_month = []
        performance_six_month = []
        total_events = len(events_history)

        #first_datetime_investment = datetime.fromisoformat(events_history[0]['_id'])

        if len(events_history) > 0:

            for event in events_history:
                profit = event['profit']
                investment_amount = float(event['investment_amount'])
                datetime_investment = datetime.fromisoformat(event['_id'])
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
            
            profit_one_week = round_(np.mean(performance_one_week),4)
            profit_one_month = round_(np.mean(performance_one_month),4) 
            profit_three_months = round_(np.mean(performance_three_month),4) 
            profit_six_months = round_(np.mean(performance_six_month),4)
            total_profit = round_(np.mean(total_performance_list),4)
            total_performance = round_(total_performance,2)

            db_doc = {'total_gain': total_performance, 'total_profit': total_profit,
                      'one_week_profit': profit_one_week, 'one_month_profit': profit_one_month, 'three_months_profit': profit_three_months, 'six_months_profit': profit_six_months, 'total_events': total_events}
            
            db_performance = list(db_trading[COLLECTION_TRADING_PERFORMANCE_TESTING].find())

            if len(db_performance) == 0:
                db_trading[COLLECTION_TRADING_PERFORMANCE_TESTING].insert(db_doc)
            else:
                id = db_performance[0]['_id']
                query = {"_id": id}
                update = {"$set":db_doc}
                db_trading[COLLECTION_TRADING_PERFORMANCE_TESTING].update_one(query, update)

    @timer_func
    def checkPerformance(db_trading, logger, TRADING_LIVE):
        '''
        this function check the performance of the trading bot if mode TRADING_LIVE is enabled
        '''
        id = datetime.now().isoformat()
        
        # IF TRADING LIVE fetch only those trades that have been live, and select the DB COLLECTION_PERFORMANCE FOR LIVE_TRADING
        if TRADING_LIVE:
            COLLECTION_PERFORMANCE = COLLECTION_TRADING_PERFORMANCE
            last_record = db_trading[COLLECTION_PERFORMANCE].find_one({}, sort=[("_id", DESCENDING)])
            coins_history = list(db_trading[COLLECTION_TRADING_HISTORY].find({'on_trade': False, 'trading_live': True}).sort("exit_timestamp", DESCENDING))
        # TRADING_LIVE mode is not enabled, then consider all the trades (live and not) and save the performance to the DB COLLECTION_PERFORMANCE for NOT LIVE_TRADING
        else:
            COLLECTION_PERFORMANCE = COLLECTION_TRADING_PERFORMANCE_TESTING
            last_record = db_trading[COLLECTION_PERFORMANCE].find_one({}, sort=[("_id", DESCENDING)])
            coins_history = list(db_trading[COLLECTION_TRADING_HISTORY].find({'on_trade': False}).sort("exit_timestamp", DESCENDING))

        n_coins_history =len(coins_history)

        if last_record == None:
            total_events_recorded = 0
            absolute_profit = 0
            total_investment = 0
            clean_profit = 0
            weighted_performance_percentage = 0
            new_clean_profit = 0
        else:
            total_events_recorded = last_record['total_events_recorded']
            absolute_profit = last_record['absolute_profit']
            total_investment = last_record['total_investment']
            clean_profit = last_record['clean_profit']
            weighted_performance_percentage = last_record['weighted_performance_percentage']
            new_clean_profit = last_record['clean_profit']

        
        if n_coins_history != total_events_recorded:
                new_events_to_register = n_coins_history - total_events_recorded
                logger.info(f'There are {new_events_to_register} events to register in {COLLECTION_TRADING_BALANCE_ACCOUNT} collection')
                for event_to_add in coins_history[:new_events_to_register]:
                    logger.info(f'{event_to_add["coin"]}:{event_to_add["_id"]} has been added')
                    absolute_profit += event_to_add['investment_amount'] * event_to_add['profit']
                    total_investment += event_to_add['investment_amount']
                    weighted_performance_percentage = absolute_profit / total_investment
                    if total_events_recorded != 0:
                        new_clean_profit = ((clean_profit / total_events_recorded) + event_to_add['profit']) / (total_events_recorded + 1)
                    else:
                        clean_profit = event_to_add["profit"]
                        new_clean_profit = event_to_add['profit']
                    total_events_recorded += 1
        else:
            logger.info(f'No events to register the {COLLECTION_PERFORMANCE} performance')
        

        doc_db = {'_id': id, 'absolute_profit': absolute_profit, 'weighted_performance_percentage': weighted_performance_percentage,
                'clean_profit': new_clean_profit, 'total_investment': total_investment, 'total_events_recorded': total_events_recorded,}
        
        db_trading[COLLECTION_PERFORMANCE].insert(doc_db)
        pass
    
    def makeRequest(api_path, method, params):
        API_KEY = os.getenv('API_KEY')
        PRIVATE_KEY_PATH = os.getenv('PRIVATE_KEY_PATH')

        timestamp = int(time() * 1000)
        params['timestamp'] = timestamp

        with open(PRIVATE_KEY_PATH, 'rb') as f:
            private_key = load_pem_private_key(data=f.read(),
                                            password=None)
            
        payload = '&'.join([f'{param}={value}' for param, value in params.items()])
        signature = base64.b64encode(private_key.sign(payload.encode('ASCII')))
        params['signature'] = signature

        if method == 'GET':
            data = requests.request(method=method ,url="https://api.binance.com" + api_path,
                params = params,
                headers = {
                    "X-MBX-APIKEY" : API_KEY,
                }
            )
        else:
            data = requests.request(method=method ,url="https://api.binance.com" + api_path,
                data = params,
                headers = {
                    "X-MBX-APIKEY" : API_KEY,
                }
            )

        status_code = data.status_code
        return data, status_code



    def get_asset_composition():

        # make request for fetching asset composition
        api_path = "/api/v3/account"
        params = {}
        method = 'GET'
        data, status_code = TradingController.makeRequest(api_path=api_path, params=params, method=method)

        if status_code == 200:
            # fetch asset whose balance is different from zero
            data = data.json()
            data = data['balances']
            #print(data)
            current_wallet = {}
            for asset in data:
                if float(asset["free"]) != 0:
                    current_wallet[asset['asset']] = float(asset['free'])


            return current_wallet, status_code
        else:
            msg = f'Status Code: {status_code}: get_asset_composition did not work as expected'
            return msg, status_code
        
    #@timer_func
    def get_balance_account(logger, db_trading):
        # t1 = time()
        now = datetime.now()
        now_isoformat = now.isoformat()
        current_day_of_the_week = now.weekday()

        response, status_code = TradingController.get_asset_composition()
        balance_account = 0
        last_record = db_trading[COLLECTION_TRADING_BALANCE_ACCOUNT].find_one({}, sort=[("_id", DESCENDING)])
        TRADING_LIVE = bool(int(os.getenv('TRADING_LIVE')))

        # the "CREATE_NEW" flag is used to create a new record every week.
        # this record has the goal to re-compute the "investment_amount" based on the past balance account (i.e. last_balance_account)
        CREATE_NEW = False

        # INITIALIZE VARIABLES
        # in case the BALANCE_ACCOUNT_COLLECTION is empy, initialize variables
        if last_record == None:
            CREATE_NEW = True
            last_investment_amount = int(os.getenv('INITIALIZED_INVESTMENT_AMOUNT'))
            last_balance_account = float(os.getenv('INITIALIZED_BALANCE_ACCOUNT'))
        else:
            last_investment_amount = last_record['investment_amount']
            last_balance_account = last_record['balance_account']
            query = {'_id': last_record['_id']}

            # if It is a new week (it is Monday)
            if current_day_of_the_week == 0 and now - datetime.fromisoformat(last_record['_id']) > timedelta(days=6):
                CREATE_NEW = True


        if status_code == 200:
            if "USDT" in response:
                balance_account_usdt = round_(response["USDT"],2)
                balance_account = balance_account_usdt
            else:
                balance_account_usdt = 0
                balance_account = 0

            coins_live = list(db_trading[COLLECTION_TRADING_LIVE].find({'on_trade': True}))

            # get balance account, absolute profit and total investment of only official events
            for coin_live in coins_live:

                # if event is not live (simulated only), then skip

                # if TRADING_LIVE env var is ON, and coin was not started as live, then skip
                if TRADING_LIVE and not coin_live["trading_live"]:
                    continue
                
                # get balance account
                #balance_account += (coin_live["purchase_price"] * coin_live["quantity"]) * coin_live["profit"]
                balance_account += coin_live["current_price"] * coin_live["quantity"]
                
            update_data = {"$set": {}}

            
            # Update the average balance account considering the minutes passed from the beginning of the week
            # balance_account refers to the most updated balanace account
            # average_balance refers to average of balance account in the last week (i.e. from Monday)
            if not CREATE_NEW:
                minutes_passed = ((current_day_of_the_week * 24 + now.hour) * 60 + now.minute)

                last_balance = last_record["average_balance"]
                # logger.info(f'last_record_balance_account: {last_balance}')
                # logger.info(f'minutes passed {minutes_passed}')
                # logger.info(f' balance_account now: {balance_account}')

                average_balance = round_(((last_record["average_balance"] * (minutes_passed - 1)) + balance_account) / minutes_passed,2)
                update_data["$set"]['average_balance'] = average_balance
                update_data["$set"]['balance_account'] = round_(balance_account,2)
                update_data["$set"]['balance_usdt'] = balance_account_usdt
                update_data["$set"]['last_update'] = now_isoformat

                db_trading[COLLECTION_TRADING_BALANCE_ACCOUNT].update_one(query, update_data)
            else:
                # every week a new record is inserted in this collection. 
                # The investment_amount is computed, considering an expected loss of "MAXIMUM_LOSS" which is (e.g. 2) times the investment amount.
                # it also takes account of the scenario where a lot of events are triggered "MAX_EVENTS_SYNCED".
                # this considerations should take the risk of sudden losses and of the reduced balance account
                # for this reason, the investment amount is never reduced, in order to keep the investment as more consistent as possible.
                start_of_week = (now - timedelta(days=current_day_of_the_week)).replace(hour=0, minute=0, second=0, microsecond=0)
                #weekly id
                id = start_of_week.isoformat()
                # maximum synced events (e.g. 15)
                maximum_events_sync = int(os.getenv('MAX_EVENTS_SYNCED'))
                # maximum loss in terms of multiple of investment_amount (e.g. 2)
                maximum_loss = float(os.getenv('MAXIMUM_LOSS'))
                # potential_investment_amount (this is generally a float number), but I want a multiple of 5 to make the investments more consistent
                # the value of the investment_amount depends on the performance / average_balance_account of the last week.
                potential_investment_amount = (last_balance_account / (maximum_events_sync + maximum_loss))
                # maximum between the average balance account of the last week and the last_investment_amount
                investment_amount = max(int(potential_investment_amount - int(potential_investment_amount % 5)), last_investment_amount)
                # finally I initialize the balance account and average balance account with the "new" balance_account
                doc_db = {'_id': id, 'balance_account': round_(balance_account,2), 'balance_usdt': round_(balance_account_usdt,2), 'average_balance': round_(balance_account,2), 'investment_amount': investment_amount, 'last_update': now_isoformat}
                db_trading[COLLECTION_TRADING_BALANCE_ACCOUNT].insert(doc_db)
            

            # t2 = time()
            # time_spent = round_(t2 - t1,2)
            # logger.info(f'Time spent for getting balance account info {time_spent}')
        
        else:
            logger.info(response)
        




    def create_order(coin=None, side=None, usdt=None, quantity=None):
        # only for testing
        if coin == None:
            coin = 'BTCUSDT',
            side = 'BUY',
            quantity = 1


        api_path = "/api/v3/order"
        params = {'symbol': 'BTCUSDT',
                  'side': 'BUY',
                  'type': 'MARKET'}
        
        # Use "quoteOrderQty" for executing BUY orders with USDT
        if quantity == None:
            params['quoteOrderQty'] = str(usdt)
        # Use "quantity" for executing SELL orders with number of crypto units (i.e. quantity)
        else:
            params['quantity'] = str(quantity)
        

        method = 'POST'
        data, status_code = TradingController.makeRequest(api_path=api_path, params=params, method=method)
        return data, status_code
    
    def test_order(coin=None, side=None, quantity=None):

        api_path = "/api/v3/order/test"
        params = {'symbol': 'BTCUSDT',
                  'side': 'BUY',
                  'type': 'MARKET',
                  'quoteOrderQty': str(1)}

        method = 'POST'
        data, status_code = TradingController.makeRequest(api_path=api_path, params=params, method=method)
        return data, status_code

        


                

                            

                    




                





