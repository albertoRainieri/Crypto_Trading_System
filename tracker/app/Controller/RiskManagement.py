import os,sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import websocket
import json
from datetime import datetime, timedelta
import os
from time import sleep
from constants.constants import *
from app.Helpers.Helpers import round_, timer_func
from app.Helpers.Helpers import getsubstring_fromkey
import math
import subprocess



class RiskManagement:
    def __init__(self, id, coin, purchase_price, timeframe, riskmanagement_configuration, db_trading, logger) -> None:
        self.id = id
        self.purchase_price = purchase_price #float
        self.logger = logger #instance obj
        self.coin = coin #str
        self.current_minute = datetime.now().minute
        self.db_trading = db_trading
        self.GOLDEN_ZONE = riskmanagement_configuration['riskmanagement_conf']['golden_zone']
        self.GOLDEN_ZONE_BOOL = False
        self.SELL = False
        self.STEP = riskmanagement_configuration['riskmanagement_conf']['step']
        self.GOLDEN_ZONE_LB = self.GOLDEN_ZONE - self.STEP
        self.GOLDEN_ZONE_UB = self.GOLDEN_ZONE + self.STEP
        self.LB_THRESHOLD = None
        self.UB_THRESHOLD = None
        # after this datetime, the transaction can be closed according to next_target and stop loss
        self.ending_timewindow = datetime.fromisoformat(id) + timedelta(minutes=int(timeframe))
        # at this datetime, the transaction will be closed in case it was not already completed
        self.close_timewindow = self.ending_timewindow + timedelta(minutes=int(timeframe)/3)


    def updateProfit(self, bid_price, now):
        self.profit = (bid_price - self.purchase_price) / self.purchase_price

        if self.GOLDEN_ZONE_BOOL:
            self.manageGoldenZoneChanges()

        elif self.profit > self.GOLDEN_ZONE:
            self.manageGoldenZoneChanges()

        elif now > self.close_timewindow:
            self.manageUsualPriceChanges()

        elif now > self.ending_timewindow:
            self.SELL = True
        
    def manageGoldenZoneChanges(self):
        self.GOLDEN_ZONE_BOOL = True
        self.SELL = False

        if self.profit > self.GOLDEN_ZONE_UB:
            self.GOLDEN_ZONE_UB += self.STEP
            self.GOLDEN_ZONE_LB += self.STEP

        elif self.profit <= self.GOLDEN_ZONE_LB:
            self.SELL = True
        
    def manageUsualPriceChanges(self):
        self.SELL = False

        if self.LB_THRESHOLD == None:
            self.LB_THRESHOLD = self.profit - self.STEP
            self.UB_THRESHOLD = self.profit + self.STEP
        
        if self.profit > self.UB_THRESHOLD:
            self.UB_THRESHOLD += self.STEP
            self.LB_THRESHOLD += self.STEP
        
        elif self.profit <= self.LB_THRESHOLD:
            self.SELL = True
        
        
    
    # def isCloseOrder(self):
    #     # if current profit (in percentage is above target, then the target and stop_loss will be moved upward by "move_target")
    #     if self.profit >= self.next_target:
    #         self.next_target += self.move_target
    #         self.stop_loss += self.move_target
    #         self.logger.info(f'{self.coin}: Next Target: {self.next_target}. Stop Loss: {self.stop_loss}')
    #         return False

    #     # if current profit goes below or equal stop loss than close the order.
    #     elif self.profit <= self.stop_loss:
    #         # CLOSE ORDER
    #         percentage_profit = round_(self.profit * 100,2)
    #         self.logger.info(f'{self.coin} Order closed. Profit {percentage_profit} %')
    #         return True
        
    #     return False
    
    
    def saveToDb(self, bid_price, now):
        minute_now = now.minute

        if minute_now != self.current_minute:
            #self.logger.info('Saving to DB')

            # Specify the filter for the document to update
            filter = {'_id': self.id}

            # Specify the update operation. update current_bid_price and profit
            update = {'$set': {'current_price': bid_price, 'profit': round_(self.profit,4)}}

            # Perform the update operation
            self.db_trading[COLLECTION_TRADING_HISTORY].update_one(filter, update)
            self.db_trading[COLLECTION_TRADING_LIVE].update_one(filter, update)
            self.current_minute = minute_now
            #self.logger.info(f'Current Profit for {self.coin}: {self.profit}')

    def logging(self):
        #self.logger.info(f'Profit ')
        self.logger.info(f'nexttarget {self.next_target}:{self.profit}')
        self.logger.info(f'stoploss {self.stop_loss}:{self.profit}')

    
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

                f = open ('/tracker/trading/trading_configuration.json', "r")
                trading_configuration = json.loads(f.read())
                risk_management_configuration = json.dumps(trading_configuration[volatility_coin][event_key])
                id = doc['_id']
                logger.info(f'WSS Connection has resumed for {coin}: {id}')
                process = subprocess.Popen(["python3", "/tracker/trading/wss-trading.py", coin, id, str(purchase_price), str(timeframe), risk_management_configuration])
                query = {"_id": id}
                update_data = {"$set": {"pid": process.pid}}
                result = db_trading[COLLECTION_TRADING_HISTORY].update_one(query, update_data)
                result = db_trading[COLLECTION_TRADING_LIVE].update_one(query, update_data)
            
        

                
        