import os,sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import websocket
import json
from datetime import datetime, timedelta
import os
from time import sleep
from constants.constants import *
from app.Helpers.Helpers import round_, timer_func
import math


class RiskManagement:
    def __init__(self, id, coin, purchase_price, strategy, db_trading, db_benchmark, logger) -> None:
        self.id = id
        self.purchase_price = purchase_price #float
        self.logger = logger #instance obj
        self.coin = coin.upper() #str
        self.current_minute = datetime.now().minute
        self.db_trading = db_trading
        self.db_benchmark = db_benchmark
        

        if strategy == 'trading_steps':
            #get current volatility of the coin
            cursor_benchmark = list(db_benchmark[self.coin].find())
            print(cursor_benchmark)

            volume_30_avg = cursor_benchmark[0]['volume_30_avg']
            volume_30_std = cursor_benchmark[0]['volume_30_std']
            self.volatility = volume_30_std / volume_30_avg
            # target, stoploss and step have a fixed amount
            # + variable component based on volatility of the coin in the last 30days
            volatility_component = (math.log(self.volatility)) / 100
            self.next_target = 0.04 + volatility_component
            self.stop_loss = -0.04 + volatility_component
            self.move_target = 0.04 + volatility_component
            self.logger.info(f'Trading coin {self.coin}. Current Price {purchase_price}. Volatility: {self.volatility}')
        
    def updateProfit(self, bid_price):
        self.profit = (bid_price - self.purchase_price) / self.purchase_price

    def isCloseOrder(self):
        # if current profit (in percentage is above target, then the target and stop_loss will be moved upward by "move_target")
        if self.profit >= self.next_target:
            self.next_target += self.move_target
            self.stop_loss += self.move_target
            self.logger.info(f'{self.coin}: Next Target: {self.next_target}. Stop Loss: {self.stop_loss}')
            return False

        # if current profit goes below or equal stop loss than close the order.
        elif self.profit <= self.stop_loss:
            # CLOSE ORDER
            percentage_profit = round_(self.profit * 100,2)
            self.logger.info(f'{self.coin} Order closed. Profit {percentage_profit} %')
            return True
        
        return False
    
    def saveToDb(self, bid_price):
        # every minute, update the db
        minute_now = datetime.now().minute

        if minute_now != self.current_minute:

            # Specify the filter for the document to update
            filter = {'_id': self.id}

            # Specify the update operation. update current_bid_price and profit
            update = {'$set': {'current_price': bid_price, 'profit': round_(self.profit,4)}}

            # Perform the update operation
            self.db_trading[COLLECTION_TRADING_STRATEGY1].update_one(filter, update)
            self.db_trading[COLLECTION_TRADING_LIVE].update_one(filter, update)
            self.current_minute = minute_now
            self.logger.info(f'Current Profit for {self.coin}: {self.profit}')

    def logging(self):
        #self.logger.info(f'Profit ')
        self.logger.info(f'nexttarget {self.next_target}:{self.profit}')
        self.logger.info(f'stoploss {self.stop_loss}:{self.profit}')
    

    

            
    