import os,sys
sys.path.insert(0,'../../..')
from app.Controller.LoggingController import LoggingController
import json
from tracker.constants.constants import *
from datetime import datetime, timedelta
from tracker.app.Helpers.Helpers import round_, timer_func
import numpy as np
from tracker.database.DatabaseConnection import DatabaseConnection
from tracker.app.Helpers.Helpers import getsubstring_fromkey
import requests
import subprocess
from time import time
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import base64



class TradingController:
    
    def __init__(self) -> None:
        pass
    

    async def check_event_triggering(coin, obs, volatility_coin, logger, db_trading, db_logger, trading_coins_list, trading_configuration, investment_amount):
        

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
                        # EVENT TRIGGERED
                        # get id and purchase_price
                        id = datetime.now().isoformat()

                        #TODO: buy Order
                        msg = f"{coin} - Event Triggered: {vol_field}:{vol_value} - {buy_vol_field}:{buy_vol_value}"
                        logger.info(msg)
                        
                        purchase_price = obs['price']

                        # get risk management configuration
                        risk_management_configuration = json.dumps(trading_configuration[volatility_coin][event_key])

                        # Start Subprocess for "coin". This will launch a wss connection for getting bid price coin in real time
                        process = subprocess.Popen(["python3", "/tracker/trading/wss-trading.py", coin, id, str(purchase_price), str(timeframe), risk_management_configuration])
                        pid = process.pid
                        
                        # send query to db_trading. for logging
                        doc_db = {'_id': id, 'coin': coin, 'profit': 0, 'purchase_price': purchase_price, 'current_price': None, 'on_trade': True, 'event': event_key, 'investment_amount': investment_amount, 'risk_configuration': trading_configuration[volatility_coin][event_key], 'pid': pid}
                        db_trading[COLLECTION_TRADING_LIVE].insert_one(doc_db)
                        db_trading[COLLECTION_TRADING_HISTORY].insert_one(doc_db)
                        
                        db_logger[DATABASE_TRADING_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})

                            # insert doc

    
    def clean_db_trading(logger, db_trading, db_benchmark):
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

                # Delete the first matching document that matches the query
                result = db_trading[COLLECTION_TRADING_LIVE].delete_one(query)

                
                if result.deleted_count == 1:
                    deleted_docs_live += 1
                else:
                    logger.info(f"Document not found or not deleted in DB_Trading. id: {doc['_id']}")
                
            logger.info(f"Total Deleted Documents: {deleted_docs_live} in DB_Trading")
                # TODO: DELETE ORDER IN BINANCE PLATFORM: we need an ORDER_ID to be stored IN DB_TRADING_LIVE


            # UPDATE DB_TRADING WITH "on_trade"
            for doc in coins_history:
                # Query to find the document you want to update (in this example, we update by "_id")
                query = {"_id": doc['_id']}

                # Update fields in the document using the $set operator
                update_data = {"$set": {"on_trade": False}}

                # Update the first matching document that matches the query
                result = db_trading[COLLECTION_TRADING_HISTORY].update_one(query, update_data)

                if result.modified_count == 1:
                    updated_docs_history += 1
                else:
                    logger.info(f"Document not found or not updated in DB_History. id: {doc['_id']}")

            logger.info(f"Total Updated Documents: {updated_docs_history} in DB_History")

        else:
            for doc in coins_live:
                coin = doc['coin']
                purchase_price = doc['purchase_price']
                event_key = doc['event']
                vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe = getsubstring_fromkey(event_key)

                volume_coin = list(db_benchmark[coin].find({}, {'_id': 1, 'volume_30_avg': 1, 'volume_30_std': 1, 'Last_30_Trades': 1}))
                avg_volume_1_month = volume_coin[0]['volume_30_avg']
                std_volume_1_month = volume_coin[0]['volume_30_std']
                volatility_coin = str(int(std_volume_1_month / avg_volume_1_month))

                # f = open ('/tracker/riskmanagement/riskmanagement.json', "r")
                # trading_configuration = json.loads(f.read())
                risk_management_configuration = json.dumps(doc['risk_configuration'])
                id = doc['_id']
                logger.info(f'WSS Connection has resumed for {coin}: {id}')
                process = subprocess.Popen(["python3", "/tracker/trading/wss-trading.py", coin, id, str(purchase_price), str(timeframe), risk_management_configuration])
                query = {"_id": id}
                update_data = {"$set": {"pid": process.pid}}
                result = db_trading[COLLECTION_TRADING_HISTORY].update_one(query, update_data)
                result = db_trading[COLLECTION_TRADING_LIVE].update_one(query, update_data)


    def checkPerformance(db_trading, logger):
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
            
            db_performance = list(db_trading[COLLECTION_TRADING_PERFORMANCE].find())

            if len(db_performance) == 0:
                db_trading[COLLECTION_TRADING_PERFORMANCE].insert(db_doc)
            else:
                id = db_performance[0]['_id']
                query = {"_id": id}
                update = {"$set":db_doc}
                db_trading[COLLECTION_TRADING_PERFORMANCE].update_one(query, update)

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

        #print(data.status_code)
        return data.json()



    def check_asset_composition():
        
        # initialize db
        db = DatabaseConnection()
        db_trading = db.get_db(DATABASE_TRADING)

        query = {"on_trade": True}
        coins_live = list(db_trading[COLLECTION_TRADING_LIVE].find(query))

        # make request for fetching asset composition
        api_path = "/api/v3/account"
        params = {}
        method = 'GET'
        data = TradingController.makeRequest(api_path=api_path, params=params, method=method)

        # fetch asset whose balance is different from zero
        data = data['balances']
        current_wallet = {}
        for asset in data:
            if float(asset["free"]) != 0:
                current_wallet[asset['asset']] = asset['free']

        return current_wallet


    def make_order():

        api_path = "/api/v3/order"

        params = {'symbol': 'BTCUSDT',
                  'side': 'BUY',
                  'type': 'MARKET',
                  'quantity': str(1)}
        

        method = 'POST'
        data = TradingController.makeRequest(api_path=api_path, params=params, method=method)


        return data
                

                            

                    




                





