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
      
    # Retrieve data from db and compute statistics
    def getData(db_trades, logger):
        # f = open ('/tracker/json/most_traded_coin_list.json', "r")
        # data = json.loads(f.read())
        # coin_list = data["most_traded_coin_list"][:NUMBER_COINS_TO_TRADE*SLICES+COINS_PRIORITY]
        db = DatabaseConnection()
        db_tracker = db.get_db(database=DATABASE_TRACKER)
        db_benchmark = db.get_db(database=DATABASE_BENCHMARK)
        TrackerController.db_operations(db_trades=db_trades, db_tracker=db_tracker, db_benchmark=db_benchmark, logger=logger)
        #logger.info(collection_list)

        pass
      
    @timer_func
    def db_operations(db_trades, db_tracker, db_benchmark, logger):
        now = datetime.now()
        reference_1day_datetime = now - timedelta(days=1)
        reference_60m_datetime = now - timedelta(hours=1)
        reference_30m_datetime = now - timedelta(minutes=30)
        reference_15m_datetime = now - timedelta(minutes=15)
        reference_5m_datetime =  now - timedelta(minutes=5)
        
        reference_1day = reference_1day_datetime.isoformat()

        coins_list = db_trades.list_collection_names()
        f = open ('/tracker/json/most_traded_coin_list.json', "r")
        data = json.loads(f.read())
        coin_list_subset = data["most_traded_coin_list"][:NUMBER_COINS_TO_TRADE*SLICES+COINS_PRIORITY]
        # logger.info(coin_list_subset)
        # logger.info(len(coin_list_subset))
        
        # iterate through each coin
        for coin in coins_list:

            if coin not in coin_list_subset:
                continue
            
            volume_coin = list(db_benchmark[coin].find({}, {'_id': 1, 'volume_30_avg': 1, 'volume_30_std': 1}))
            #print(volume_coin)

            # if benchmark exists, fetch it and use it to compute the relative volume wrt to average,
            #  otherwise I am going to compute only the absolute value of the volume
            if 'volume_30_avg' in volume_coin[0] and 'volume_30_std' in volume_coin[0]:
                avg_volume_1_month = volume_coin[0]['volume_30_avg']
                std_volume_1_month = volume_coin[0]['volume_30_std']
            else:
                avg_volume_1_month = 1
                std_volume_1_month = 1

            # logger.info(f'{coin}: {avg_volume_1_month}')
            # initialize these variables list for each coin

            volumes_24h_list = []
            volumes_60m_list = []
            volumes_30m_list = []
            volumes_15m_list = []
            volumes_5m_list = []

            buy_volume_perc_24h_list = []
            buy_volume_perc_60m_list = []
            buy_volume_perc_30m_list = []
            buy_volume_perc_15m_list = []
            buy_volume_perc_5m_list = []

            buy_trades_perc_24h_list = []
            buy_trades_perc_60m_list = []
            buy_trades_perc_30m_list = []
            buy_trades_perc_15m_list = []
            buy_trades_perc_5m_list = []

            
            docs = db_trades[coin].find({"_id": {"$gte": reference_1day}})
            

            i = 1
            for doc in docs:
                if i == 1:
                    price_1d = doc['price']
                    i += 1
      

                #logger.info(doc)
                doc_vol = doc['volume']
                doc_buy_vol = doc['buy_volume'] / doc['volume']
                doc_buy_trd = doc['buy_n'] / doc['n_trades']

                volumes_24h_list.append(doc_vol)
                buy_volume_perc_24h_list.append(doc_buy_vol)
                buy_trades_perc_24h_list.append(doc_buy_trd)

                timestamp_trade = datetime.fromisoformat(doc['_id'])
                #logger.info(timestamp_trade)
                # average volume 5m
                if timestamp_trade > reference_5m_datetime:
                    #logger.info(f'{timestamp_trade} --- {reference_5m_datetime}')
                    volumes_5m_list.append(doc_vol)
                    volumes_15m_list.append(doc_vol)
                    volumes_30m_list.append(doc_vol)
                    volumes_60m_list.append(doc_vol)

                    buy_volume_perc_5m_list.append(doc_buy_vol)
                    buy_volume_perc_15m_list.append(doc_buy_vol)
                    buy_volume_perc_30m_list.append(doc_buy_vol)
                    buy_volume_perc_60m_list.append(doc_buy_vol)

                    buy_trades_perc_5m_list.append(doc_buy_trd)
                    buy_trades_perc_15m_list.append(doc_buy_trd)
                    buy_trades_perc_30m_list.append(doc_buy_trd)
                    buy_trades_perc_60m_list.append(doc_buy_trd)

                    continue

                elif timestamp_trade > reference_15m_datetime:
                    volumes_15m_list.append(doc_vol)
                    volumes_30m_list.append(doc_vol)
                    volumes_60m_list.append(doc_vol)

                    buy_volume_perc_15m_list.append(doc_buy_vol)
                    buy_volume_perc_30m_list.append(doc_buy_vol)
                    buy_volume_perc_60m_list.append(doc_buy_vol)

                    buy_trades_perc_15m_list.append(doc_buy_trd)
                    buy_trades_perc_30m_list.append(doc_buy_trd)
                    buy_trades_perc_60m_list.append(doc_buy_trd)

                    continue

                elif timestamp_trade > reference_30m_datetime:
                    volumes_30m_list.append(doc_vol)
                    volumes_60m_list.append(doc_vol)

                    buy_volume_perc_30m_list.append(doc_buy_vol)
                    buy_volume_perc_60m_list.append(doc_buy_vol)

                    buy_trades_perc_30m_list.append(doc_buy_trd)
                    buy_trades_perc_60m_list.append(doc_buy_trd)

                    continue

                elif timestamp_trade > reference_60m_datetime:
                    
                    volumes_60m_list.append(doc_vol)
                    buy_volume_perc_60m_list.append(doc_buy_vol)
                    buy_trades_perc_60m_list.append(doc_buy_trd)

                    continue
                
            #logger.info(f"{coin}: {doc['_id']}")
            
            price_now = doc['price']
            price_variation = (price_now - price_1d) / price_1d
            
            

            if len(volumes_24h_list) != 0:
                volumes_24h = round_(np.mean(volumes_24h_list) / avg_volume_1_month, 2) 
                buy_volume_perc_24h = round_(np.mean(buy_volume_perc_24h_list),2)
                buy_trades_perc_24h = round_(np.mean(buy_trades_perc_24h_list),2)
                volumes_24h_std = round_(np.std(volumes_24h_list) / std_volume_1_month, 2)
                buy_volume_perc_24h_std = round_(np.std(buy_volume_perc_24h_list),2)
                buy_trades_perc_24h_std = round_(np.std(buy_trades_perc_24h_list),2)
            else:
                volumes_24h = None
                buy_volume_perc_24h = None
                buy_trades_perc_24h = None
                volumes_24h_std = None
                buy_volume_perc_24h_std = None
                buy_trades_perc_24h_std = None


            if len(volumes_60m_list) != 0:
                volumes_60m = round_(np.mean(volumes_60m_list) / avg_volume_1_month, 2)
                buy_volume_perc_60m = round_(np.mean(buy_volume_perc_60m_list),2)
                buy_trades_perc_60m = round_(np.mean(buy_trades_perc_60m_list),2)
                volumes_60m_std = round_(np.std(volumes_60m_list) / std_volume_1_month, 2)
                buy_volume_perc_60m_std = round_(np.std(buy_volume_perc_60m_list),2)
                buy_trades_perc_60m_std = round_(np.std(buy_trades_perc_60m_list),2)
            else:
                volumes_60m = None
                buy_volume_perc_60m = None
                buy_trades_perc_60m = None
                volumes_60m_std = None
                buy_volume_perc_60m_std = None
                buy_trades_perc_60m_std = None
            

            if len(volumes_30m_list) != 0:
                volumes_30m = round_(np.mean(volumes_30m_list) / avg_volume_1_month, 2)
                buy_volume_perc_30m = round_(np.mean(buy_volume_perc_30m_list),2)
                buy_trades_perc_30m = round_(np.mean(buy_trades_perc_30m_list),2)
                volumes_30m_std = round_(np.std(volumes_30m_list) / std_volume_1_month, 2)
                buy_volume_perc_30m_std = round_(np.std(buy_volume_perc_30m_list),2)
                buy_trades_perc_30m_std = round_(np.std(buy_trades_perc_30m_list),2)
            else:
                volumes_30m = None
                buy_volume_perc_30m = None
                buy_trades_perc_30m = None
                volumes_30m_std = None
                buy_volume_perc_30m_std = None
                buy_trades_perc_30m_std = None


            if len(volumes_15m_list) != 0:
                volumes_15m = round_(np.mean(volumes_15m_list) / avg_volume_1_month, 2)
                buy_volume_perc_15m = round_(np.mean(buy_volume_perc_15m_list),2)
                buy_trades_perc_15m = round_(np.mean(buy_trades_perc_15m_list),2)
                volumes_15m_std = round_(np.std(volumes_15m_list) / std_volume_1_month, 2)
                buy_volume_perc_15m_std = round_(np.std(buy_volume_perc_15m_list),2)
                buy_trades_perc_15m_std = round_(np.std(buy_trades_perc_15m_list),2)
            else:
                volumes_15m = None
                buy_volume_perc_15m = None
                buy_trades_perc_15m = None
                volumes_15m_std = None
                buy_volume_perc_15m_std = None
                buy_trades_perc_15m_std = None

            if len(volumes_5m_list) != 0:
                volumes_5m = round_(np.mean(volumes_5m_list) / avg_volume_1_month, 2)
                buy_volume_perc_5m = round_(np.mean(buy_volume_perc_5m_list),2)
                buy_trades_perc_5m = round_(np.mean(buy_trades_perc_5m_list),2)
                volumes_5m_std = round_(np.std(volumes_5m_list) / std_volume_1_month, 2)
                buy_volume_perc_5m_std = round_(np.std(buy_volume_perc_5m_list),2)
                buy_trades_perc_5m_std = round_(np.std(buy_trades_perc_5m_list),2)
            else:
                volumes_5m = None
                buy_volume_perc_5m = None
                buy_trades_perc_5m = None
                volumes_5m_std = None
                buy_volume_perc_5m_std = None
                buy_trades_perc_5m_std = None



            doc_db = {'_id': now.isoformat(), "price_%" : round_(price_variation,4), 
                      'vol_5m': volumes_5m, 'vol_5m_std': volumes_5m_std, 'buy_vol_5m': buy_volume_perc_5m, 'buy_vol_5m_std': buy_volume_perc_5m_std,'buy_trd_5m': buy_trades_perc_5m, 'buy_trd_5m_std': buy_trades_perc_5m_std,
                      'vol_15m': volumes_15m, 'vol_15m_std': volumes_15m_std, 'buy_vol_15m': buy_volume_perc_15m, 'buy_vol_15m_std': buy_volume_perc_15m_std,'buy_trd_15m': buy_trades_perc_15m, 'buy_trd_15m_std': buy_trades_perc_15m_std,
                      'vol_30m': volumes_30m, 'vol_30m_std': volumes_30m_std, 'buy_vol_30m': buy_volume_perc_30m, 'buy_vol_30m_std': buy_volume_perc_30m_std,'buy_trd_30m': buy_trades_perc_30m, 'buy_trd_30m_std': buy_trades_perc_30m_std,
                      'vol_60m': volumes_60m, 'vol_60m_std': volumes_60m_std, 'buy_vol_60m': buy_volume_perc_60m, 'buy_vol_60m_std': buy_volume_perc_60m_std,'buy_trd_60m': buy_trades_perc_60m, 'buy_trd_60m_std': buy_trades_perc_60m_std,
                      'vol_24h': volumes_24h, 'vol_24h_std': volumes_24h_std, 'buy_vol_24h': buy_volume_perc_24h, 'buy_vol_24h_std': buy_volume_perc_24h_std,'buy_trd_24h': buy_trades_perc_24h, 'buy_trd_24h_std': buy_trades_perc_24h_std,

                      }
        
            #logger.info(doc_db)

            db_tracker[coin].insert(doc_db)

        