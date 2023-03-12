import os,sys
sys.path.insert(0,'../..')
from app.Controller.LoggingController import LoggingController
import json
from tracker.constants.constants import *
from datetime import datetime, timedelta
from tracker.app.Helpers.Helpers import round_, timer_func
import numpy as np
from tracker.database.DatabaseConnection import DatabaseConnection
from tracker.constants.constants import *


class TrackerController:
    def __init__(self) -> None:
        pass
      

    def getData(db_trades, logger):
        # f = open ('/tracker/json/most_traded_coin_list.json', "r")
        # data = json.loads(f.read())
        # coin_list = data["most_traded_coin_list"][:NUMBER_COINS_TO_TRADE*SLICES+COINS_PRIORITY]
        db_tracker = DatabaseConnection()
        db_tracker = db_tracker.get_db(database=DATABASE_TRACKER)
        TrackerController.db_operations(db_trades=db_trades, db_tracker=db_tracker, logger=logger)
        #logger.info(collection_list)

        pass
      
    @timer_func
    def db_operations(db_trades, db_tracker, logger):
        now = datetime.now()
        reference_60m = (now - timedelta(hours=1)).isoformat()
        reference_30m = (now - timedelta(minutes=30)).isoformat()
        reference_15m = (now - timedelta(minutes=15)).isoformat()
        reference_5m =  (now - timedelta(minutes=5)).isoformat()

        price_change_60m = {}
        price_change_30m = {}
        price_change_15m = {}
        price_change_5m = {}


        volumes_60m = {}
        volumes_30m = {}
        volumes_15m = {}
        volumes_5m = {}

        buy_volume_perc_60m = {}
        buy_volume_perc_30m = {}
        buy_volume_perc_15m = {}
        buy_volume_perc_5m = {}

        buy_trades_perc_60m = {}
        buy_trades_perc_30m = {}
        buy_trades_perc_15m = {}
        buy_trades_perc_5m = {}

        coins_list = db_trades.list_collection_names()
        #coins_list = ['All_Trades.BTC_USD']

        for coin in coins_list:
            
            price_change_60m[coin] = []
            price_change_30m[coin] = []
            price_change_15m[coin] = []
            price_change_5m[coin] = []


            volumes_60m[coin] = []
            volumes_30m[coin] = []
            volumes_15m[coin] = []
            volumes_5m[coin] = []

            buy_volume_perc_60m[coin] = []
            buy_volume_perc_30m[coin] = []
            buy_volume_perc_15m[coin] = []
            buy_volume_perc_5m[coin] = []

            buy_trades_perc_60m[coin] = []
            buy_trades_perc_30m[coin] = []
            buy_trades_perc_15m[coin] = []
            buy_trades_perc_5m[coin] = []

        # iterate through each coin
        for coin in coins_list:
            
            docs = db_trades[coin].find({"_id": {"$gte": reference_60m}})
            # iterate thrugh each doc (trade)
            for doc in docs:
                #logger.info(doc)
                volumes_60m[coin].append(doc['volume'])
                buy_volume_perc_60m[coin].append(doc['buy_volume'] / doc['volume'])
                buy_trades_perc_60m[coin].append(doc['buy_n'] / doc['n_trades'])

                timestamp_trade = doc['_id']
                # average volume 5m
                if timestamp_trade < reference_5m:

                    volumes_5m[coin].append(doc['volume'])
                    volumes_15m[coin].append(doc['volume'])
                    volumes_30m[coin].append(doc['volume'])

                    buy_volume_perc_5m[coin].append(doc['buy_volume'] / doc['volume'])
                    buy_volume_perc_15m[coin].append(doc['buy_volume'] / doc['volume'])
                    buy_volume_perc_30m[coin].append(doc['buy_volume'] / doc['volume'])

                    buy_trades_perc_5m[coin].append(doc['buy_n'] / doc['n_trades'])
                    buy_trades_perc_15m[coin].append(doc['buy_n'] / doc['n_trades'])
                    buy_trades_perc_30m[coin].append(doc['buy_n'] / doc['n_trades'])

                    continue

                elif timestamp_trade < reference_15m:
                    volumes_15m[coin].append(doc['volume'])
                    volumes_30m[coin].append(doc['volume'])

                    buy_volume_perc_15m[coin].append(doc['buy_volume'] / doc['volume'])
                    buy_volume_perc_30m[coin].append(doc['buy_volume'] / doc['volume'])

                    buy_trades_perc_15m[coin].append(doc['buy_n'] / doc['n_trades'])
                    buy_trades_perc_30m[coin].append(doc['buy_n'] / doc['n_trades'])

                    continue

                elif timestamp_trade < reference_30m:
                    volumes_30m[coin].append(doc['volume'])
                    buy_volume_perc_30m[coin].append(doc['buy_volume'] / doc['volume'])
                    buy_trades_perc_30m[coin].append(doc['buy_n'] / doc['n_trades'])

                    continue


            volumes_60m[coin] = np.mean(volumes_60m[coin])
            volumes_30m[coin] = np.mean(volumes_30m[coin])
            volumes_15m[coin] = np.mean(volumes_15m[coin])
            volumes_5m[coin] = np.mean(volumes_5m[coin])

            buy_volume_perc_60m[coin] = np.mean(buy_volume_perc_60m[coin])
            buy_volume_perc_30m[coin] = np.mean(buy_volume_perc_30m[coin])
            buy_volume_perc_15m[coin] = np.mean(buy_volume_perc_15m[coin])
            buy_volume_perc_5m[coin] = np.mean(buy_volume_perc_5m[coin])

            buy_trades_perc_60m[coin] = np.mean(buy_trades_perc_60m[coin])
            buy_trades_perc_30m[coin] = np.mean(buy_trades_perc_30m[coin])
            buy_trades_perc_15m[coin] = np.mean(buy_trades_perc_15m[coin])
            buy_trades_perc_5m[coin] = np.mean(buy_trades_perc_5m[coin])

            doc_db = {'vol_5m': volumes_5m[coin], 'buy_vol_5m': buy_volume_perc_5m[coin], 'buy_trd_5m': buy_trades_perc_5m[coin], 
                      'vol_15m': volumes_15m[coin], 'buy_vol_15m': buy_volume_perc_15m[coin], 'buy_trd_15m': buy_trades_perc_15m[coin],
                      'vol_30m': volumes_30m[coin], 'buy_vol_30m': buy_volume_perc_30m[coin], 'buy_trd_30m': buy_trades_perc_30m[coin],
                      'vol_60m': volumes_60m[coin], 'buy_vol_60m': buy_volume_perc_60m[coin], 'buy_trd_60m': buy_trades_perc_60m[coin],
                      }
            
            coin_s = coin.split('.')[-1]
            #logger.info(f'STRING: {coin_s}')
            #logger.info(doc_db)

            db_tracker[coin_s].insert(doc_db)

            
            #SAVE DATA TO DB.
        for coin in coins_list:

               

                    
                    
                    
            pass
        