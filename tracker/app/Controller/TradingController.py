import os,sys
sys.path.insert(0,'../../..')
from app.Controller.LoggingController import LoggingController
import json
from tracker.constants.constants import *
from datetime import datetime, timedelta
from tracker.app.Helpers.Helpers import round_, timer_func
import numpy as np
from tracker.database.DatabaseConnection import DatabaseConnection
from analysis.Functions import getsubstring_fromkey
from tracker.constants.constants import *
import requests
import subprocess


class TradingController:
    
    def __init__(self) -> None:
        pass
    

    @staticmethod
    def check_event_triggering(coin, obs, volatility_coin, logger, db_trading, db_logger, trading_coins_list):
        

        # get the most performing events for volatility related
        # events_list = json_[volatility_coin]
        events_list = ['buy_vol_5m:0.65/vol_60m:4/timeframe:4320/vlty:1',
                       'buy_vol_5m:0.75/vol_5m:20/timeframe:360/vlty:1',
                       
                       ]
        
        # check if coin is already on trade, if True, pass
        coins_on_trade = list(db_trading[COLLECTION_TRADING_LIVE].find())
        if len(coins_on_trade) != 0:
            for coin_on_trade in coins_on_trade:
                if coin_on_trade['coin'] == coin:
                    # coin is already on_trade
                    pass

        for event_key in events_list:
          
            volatility_key = int(event_key.split('vlty:')[-1])
            vol_field, vol_value, buy_vol_field, buy_vol_value, timeframe = getsubstring_fromkey(event_key)



            if obs[vol_field] >= int(vol_value) and obs[buy_vol_field] >= int(buy_vol_field) and volatility_coin == volatility_key:

                # get id and purchase_price
                id = datetime.now().isoformat()
                purchase_price = obs['price']

                #TODO: buy Order
                


                # Start Subprocess for "coin". This will launch a wss connection for getting bid price coin in real time
                process = subprocess.Popen(["python3", "/tracker/trading/wss-trading.py", coin, id, purchase_price])
                pid = process.pid
                
                # send query to db_trading. for logging
                doc_db = {'_id': id, 'coin': coin, 'profit': None, 'purchase_price': purchase_price, 'current_price': None, 'on_trade': True, 'event': event_key, 'pid': pid}
                db_trading[COLLECTION_TRADING_LIVE].insert_one(doc_db)
                db_trading[COLLECTION_TRADING_STRATEGY1].insert_one(doc_db)
                msg = f"{coin} - Event Triggered: {vol_field}:{vol_value} - {buy_vol_field}:{buy_vol_value}"
                logger.info(msg)
                db_logger[DATABASE_TRADING_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})

                # insert doc
                    

              




              





