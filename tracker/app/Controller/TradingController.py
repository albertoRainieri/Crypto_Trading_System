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


class TradingController:
    
    def __init__(self) -> None:
        pass
    

    async def check_event_triggering(coin, obs, volatility_coin, logger, db_trading, db_logger, trading_coins_list, trading_configuration):
        

        # STRUCTURE TRADING CONFIGURATION
        # {
        #     '<volatility_1>' : {
        #         '<key_1>': {
        #             'riskmanagement_conf': {
        #                 'golden_zone': '<GOLDEN_ZONE>',
        #                 'step': '<STEP>'
        #             }
        #         }
        #     }
        # }
        if volatility_coin in trading_configuration:
            coins_on_trade = list(db_trading[COLLECTION_TRADING_LIVE].find())
            
            for event_key in trading_configuration[volatility_coin]:

                # check if coin is already on trade for this specific event, if True, pass
                #logger.info(coins_on_trade)
                COIN_ON_TRADING = False
    
                vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe = getsubstring_fromkey(event_key)

                if obs[vol_field] == None or obs[buy_vol_field] == None:
                    continue

                if obs[vol_field] >= int(vol_value) and obs[buy_vol_field] >= float(buy_vol_value):

                    if len(coins_on_trade) != 0:
                        for coin_on_trade in coins_on_trade:
                            if coin_on_trade['coin'] == coin and coin_on_trade['event'] == event_key:
                                COIN_ON_TRADING = True
                                logger.info(f'{coin} is already on trade for {event_key} skipping')
                    
                    
                    if not COIN_ON_TRADING:
                        # EVENT TRIGGERED
                        # get id and purchase_price
                        id = datetime.now().isoformat()

                        #TODO: buy Order
                        purchase_price = obs['price']

                        # get risk management configuration
                        risk_management_configuration = json.dumps(trading_configuration[volatility_coin][event_key])

                        # Start Subprocess for "coin". This will launch a wss connection for getting bid price coin in real time
                        process = subprocess.Popen(["python3", "/tracker/trading/wss-trading.py", coin, id, str(purchase_price), str(timeframe), risk_management_configuration])
                        pid = process.pid
                        
                        # send query to db_trading. for logging
                        doc_db = {'_id': id, 'coin': coin, 'profit': None, 'purchase_price': purchase_price, 'current_price': None, 'on_trade': True, 'event': event_key, 'pid': pid}
                        db_trading[COLLECTION_TRADING_LIVE].insert_one(doc_db)
                        db_trading[COLLECTION_TRADING_HISTORY].insert_one(doc_db)
                        msg = f"{coin} - Event Triggered: {vol_field}:{vol_value} - {buy_vol_field}:{buy_vol_value}"
                        logger.info(msg)
                        db_logger[DATABASE_TRADING_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})

                            # insert doc
                            

                    




                





