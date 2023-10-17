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
    def __init__(self, id, coin, purchase_price, timeframe, riskmanagement_configuration, logger) -> None:
        self.id = id
        self.purchase_price = purchase_price #float
        self.logger = logger #instance obj
        self.coin = coin #str
        self.current_minute = datetime.now().minute
        self.GOLDEN_ZONE = float(riskmanagement_configuration['riskmanagement_conf']['golden_zone'])
        self.GOLDEN_ZONE_BOOL = False
        self.SELL = False
        self.STEP = float(riskmanagement_configuration['riskmanagement_conf']['step_golden'])
        self.STEP_NOGOLDEN = float(riskmanagement_configuration['riskmanagement_conf']['step_nogolden'])
        self.EXTRA_TIMEFRAME = float(riskmanagement_configuration['riskmanagement_conf']['extra_timeframe'])
        self.GOLDEN_ZONE_LB = self.GOLDEN_ZONE - self.STEP
        self.GOLDEN_ZONE_UB = self.GOLDEN_ZONE + self.STEP
        self.LB_THRESHOLD = None
        self.UB_THRESHOLD = None
        self.timeframe = timeframe
        # after this datetime, the transaction can be closed according to next_target and stop loss
        self.ending_timewindow = datetime.fromisoformat(id) + timedelta(minutes=int(timeframe))
        # at this datetime, the transaction will be closed in case it was not already completed
        self.close_timewindow = self.ending_timewindow + timedelta(minutes=int(timeframe*self.EXTRA_TIMEFRAME))


    def updateProfit(self, bid_price, now):
        self.profit = (bid_price - self.purchase_price) / self.purchase_price
        #self.logger.info(self.profit)

        if self.GOLDEN_ZONE_BOOL:
            self.manageGoldenZoneChanges()
            if now > self.close_timewindow:
                self.SELL = True

        elif self.profit > self.GOLDEN_ZONE:
            self.manageGoldenZoneChanges()
            if now > self.close_timewindow:
                self.SELL = True

        elif now > self.close_timewindow:
            self.SELL = True

        elif now > self.ending_timewindow:
            self.manageUsualPriceChanges()
        
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
            self.LB_THRESHOLD = self.profit - self.STEP_NOGOLDEN
            self.UB_THRESHOLD = self.profit + self.STEP_NOGOLDEN
        
        if self.profit > self.UB_THRESHOLD:
            self.UB_THRESHOLD += self.STEP_NOGOLDEN
            self.LB_THRESHOLD += self.STEP_NOGOLDEN
        
        elif self.profit <= self.LB_THRESHOLD:
            self.SELL = True
    
    
    def saveToDb(self, bid_price, db):

        f = open ('/tracker/user_configuration/userconfiguration.json', "r")
        user_configuration = json.loads(f.read())

        for user in user_configuration:
            db_name = DATABASE_TRADING + '_' + user
            db_trading = db.get_db(db_name)

            minute_now = datetime.now().minute
            if minute_now != self.current_minute:
                #self.logger.info('Saving to DB')

                # Specify the filter for the document to update
                filter = {'_id': self.id}

                # Specify the update operation. update current_bid_price and profit
                update = {'$set': {'current_price': bid_price, 'profit': round_(self.profit,4)}}

                # Perform the update operation
                db_trading[COLLECTION_TRADING_HISTORY].update_one(filter, update)
                db_trading[COLLECTION_TRADING_LIVE].update_one(filter, update)
                self.current_minute = minute_now
                #self.logger.info(f'Current Profit for {self.coin}: {self.profit}')

    def logging(self):
        #self.logger.info(f'Profit ')
        self.logger.info(f'nexttarget {self.next_target}:{self.profit}')
        self.logger.info(f'stoploss {self.stop_loss}:{self.profit}')
            
        

                
        