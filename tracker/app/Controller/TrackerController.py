import os,sys
sys.path.insert(0,'../..')
from app.Controller.LoggingController import LoggingController
import json
from tracker.constants.constants import *
from datetime import datetime, timedelta
from tracker.app.Helpers.Helpers import round_, timer_func
import numpy as np
from tracker.database.DatabaseConnection import DatabaseConnection
from tracker.app.Controller.TradingController import TradingController
from tracker.constants.constants import *
import asyncio


class TrackerController:
    def __init__(self) -> None:
        pass
      
    # Retrieve data from db and compute statistics
    def getData(db_trades, logger, db_logger):
        # f = open ('/tracker/json/most_traded_coin_list.json', "r")
        # data = json.loads(f.read())
        db = DatabaseConnection()
        db_tracker = db.get_db(database=DATABASE_TRACKER)
        db_benchmark = db.get_db(database=DATABASE_BENCHMARK)
        db_trading = db.get_db(database=DATABASE_TRADING)
        #TrackerController.db_operations(db_trades=db_trades, db_tracker=db_tracker, db_benchmark=db_benchmark, logger=logger)

        # try:
        asyncio.run(TrackerController.db_operations(db_trades=db_trades, db_tracker=db_tracker, db_benchmark=db_benchmark, db_trading=db_trading, logger=logger, db_logger=db_logger))
        # except Exception as e:
        #     logger.error(e)
        #     logger.error('Something Wrong Happened. Check the logs above')

        pass

    def isEqualToReferenceDatetime(actual_timestamp, reference_year_x_ago, reference_month_x_ago, reference_day_x_ago, reference_hour_x_ago, reference_minute_x_ago, logger):
        '''
        This function check if actual timestamp is equal to reference datetime (1h, 3h, 6h ago)
        '''
        actual_datetime = datetime.fromisoformat(actual_timestamp)
        #logger.info(f'Actual Datetime: {actual_datetime}')

        year_x_ago = actual_datetime.year
        month_x_ago = actual_datetime.month
        day_x_ago = actual_datetime.day
        hour_x_ago = actual_datetime.hour
        minute_x_ago =  actual_datetime.minute

        if year_x_ago == reference_year_x_ago and month_x_ago == reference_month_x_ago and day_x_ago == reference_day_x_ago and hour_x_ago == reference_hour_x_ago and minute_x_ago == reference_minute_x_ago:
            return True
        else:
            return False


    #@timer_func
    async def db_operations(db_trades, db_tracker, db_benchmark, db_trading, logger, db_logger):
        now = datetime.now()
        now_isoformat = now.isoformat()
        reference_1day_datetime = now - timedelta(days=1)
        reference_6h_datetime = now - timedelta(hours=6)
        reference_3h_datetime = now - timedelta(hours=3)
        reference_1h_datetime = now - timedelta(hours=1)
        reference_30m_datetime = now - timedelta(minutes=30)
        reference_15m_datetime = now - timedelta(minutes=15)
        reference_5m_datetime =  now - timedelta(minutes=5)
        reference_10s_datetime = now - timedelta(seconds=10)

        
        reference_1day = reference_1day_datetime.isoformat()

        year_1h_ago = reference_1h_datetime.year
        month_1h_ago = reference_1h_datetime.month
        day_1h_ago = reference_1h_datetime.day
        hour_1h_ago = reference_1h_datetime.hour
        minute_1h_ago =  reference_1h_datetime.minute

        year_3h_ago = reference_3h_datetime.year
        month_3h_ago = reference_3h_datetime.month
        day_3h_ago = reference_3h_datetime.day
        hour_3h_ago = reference_3h_datetime.hour
        minute_3h_ago =  reference_3h_datetime.minute

        year_6h_ago = reference_6h_datetime.year
        month_6h_ago = reference_6h_datetime.month
        day_6h_ago = reference_6h_datetime.day
        hour_6h_ago = reference_6h_datetime.hour
        minute_6h_ago =  reference_6h_datetime.minute

        coins_list = db_trades.list_collection_names()
        trading_coins_list = db_trading.list_collection_names()

        #download list of most_traded_coins
        f = open ('/tracker/json/most_traded_coins.json', "r")
        data = json.loads(f.read())

        f = open ('/tracker/trading/trading_configuration.json', "r")
        trading_configuration = json.loads(f.read())

        #coin_list_subset = data["most_traded_coins"][:NUMBER_COINS_TO_TRADE_WSS]
        coin_list_subset = data["most_traded_coins"]
        # logger.info(coin_list_subset)
        # logger.info(len(coin_list_subset))
        
        # iterate through each coin
        for coin in coins_list:

            if coin not in coin_list_subset:
                continue
            

            
            volume_coin = list(db_benchmark[coin].find({}, {'_id': 1, 'volume_30_avg': 1, 'volume_30_std': 1, 'Last_30_Trades': 1}))
            #print(volume_coin)

            # if benchmark exists, fetch it and use it to compute the relative volume wrt to average,
            #  otherwise I am going to skip the computation
            
            if len(volume_coin) != 0:

                avg_volume_1_month = volume_coin[0]['volume_30_avg']
                std_volume_1_month = volume_coin[0]['volume_30_std']
                

                # in case there is something wrong with "volume_30_avg" then skip
                if avg_volume_1_month == None or avg_volume_1_month == 0:
                    if now.hour == 0 and now.minute == 5: 
                        msg = f'{coin} has a volume average == None or equal to zero. Computation in tracker will be skipped. Position: {data["most_traded_coins"].index(coin)}'
                        logger.info(msg)
                        db_logger[DATABASE_TRACKER_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
                    continue

                volatility_coin = str(int(std_volume_1_month / avg_volume_1_month))

                # here, the coin must pass some checks.
                # In particular, it is required that the coin has been in the "most_traded_coins" in the last consecutive "benchmark_days".
                # if this condition is not met, then the coin will not be analyzed and nothing will be saved to tracker.
                benchmark_days = 21 # how many days the coin must have been in the "most_traded_coins"

                try:
                    list_last_2w_trades = volume_coin[0]['Last_30_Trades']['list_last_30_trades'][-benchmark_days:]
                    
                    if sum(list_last_2w_trades) < benchmark_days:
                        if now.hour == 0 and now.minute == 5: 
                            msg = f'{coin} has not always been in most_traded_coins in the last {benchmark_days} days. Position: {data["most_traded_coins"].index(coin)}'
                            logger.info(msg)
                            db_logger[DATABASE_TRACKER_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
                        continue
                    else:
                        if now.hour == 0 and now.minute == 5: 
                            logger.info(f'{coin}, : Success. Position: {data["most_traded_coins"].index(coin)}')
                except:
                    if now.hour == 0 and now.minute == 5: 
                        msg = f'{coin}: There are not enough observations in db_benchmark. Position: {data["most_traded_coins"].index(coin)}'
                        logger.info(msg)
                        db_logger[DATABASE_TRACKER_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
                    continue


                
            # skip the computation. volume_average does not exist
            else:
                if now.hour == 0 and now.minute == 5:
                    msg = f'{coin} does not have a volume average. Computation in tracker will be skipped. Position: {data["most_traded_coins"].index(coin)}'
                    logger.info(msg)
                    db_logger[DATABASE_TRACKER_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
                continue
            


            
                # avg_volume_1_month = 1
                # std_volume_1_month = 1

            # logger.info(f'{coin}: {avg_volume_1_month}')
            # initialize these variables list for each coin

            volumes_24h_list = []
            volumes_6h_list = []
            volumes_3h_list = []
            volumes_60m_list = []
            volumes_30m_list = []
            volumes_15m_list = []
            volumes_5m_list = []

            buy_volume_24h_list = []
            buy_volume_6h_list = []
            buy_volume_3h_list = []
            buy_volume_60m_list = []
            buy_volume_30m_list = []
            buy_volume_15m_list = []
            buy_volume_5m_list = []

            buy_trades_24h_list = []
            buy_trades_6h_list = []
            buy_trades_3h_list = []
            buy_trades_60m_list = []
            buy_trades_30m_list = []
            buy_trades_15m_list = []
            buy_trades_5m_list = []

            trades_24h_list = []
            trades_6h_list = []
            trades_3h_list = []
            trades_60m_list = []
            trades_30m_list = []
            trades_15m_list = []
            trades_5m_list = []

            price_1d_ago = None
            price_6h_ago = None
            price_3h_ago = None
            price_1h_ago = None

            price_variation_1d = None
            price_variation_6h = None
            price_variation_3h = None
            price_variation_1h = None

            
            docs = list(db_trades[coin].find({"_id": {"$gte": reference_1day}}))
            #docs_copy = docs.copy()

            #check if it is available an observation in "Market_Trades" in the last minute
            if len(docs) == 0:
                continue
            last_timestamp = docs[-1]['_id']
            last_price = docs[-1]['price']
            last_datetime = datetime.fromisoformat(last_timestamp)

            if last_price == None:
                continue

            if last_datetime > reference_10s_datetime:
                
                i = 1
                for doc in docs:
                    if i == 1:
                        i += 1

                        # check how far in the past is the first timestamp of docs. This is needed to compute or not compute the various price changes
                        max_timedelta_first_last_timestamp = now - datetime.fromisoformat(doc['_id'])
                        if max_timedelta_first_last_timestamp > timedelta(hours=23, minutes=58):
                            PRICE_6H_AGO = True
                            PRICE_3H_AGO = True
                            PRICE_1H_AGO = True
                            price_1d_ago = doc['price']
                        elif max_timedelta_first_last_timestamp > timedelta(hours=5, minutes=58):
                            PRICE_6H_AGO = True
                            PRICE_3H_AGO = True
                            PRICE_1H_AGO = True
                        elif max_timedelta_first_last_timestamp > timedelta(hours=2, minutes=58):
                            PRICE_6H_AGO = False
                            PRICE_3H_AGO = True
                            PRICE_1H_AGO = True
                        elif max_timedelta_first_last_timestamp > timedelta(minutes=58):
                            PRICE_6H_AGO = False
                            PRICE_3H_AGO = False
                            PRICE_1H_AGO = True
                        else:
                            PRICE_6H_AGO = False
                            PRICE_3H_AGO = False
                            PRICE_1H_AGO = False

                    
                    if PRICE_6H_AGO:
                        if TrackerController.isEqualToReferenceDatetime(doc['_id'], year_6h_ago, month_6h_ago, day_6h_ago, hour_6h_ago, minute_6h_ago, logger):
                            price_6h_ago = doc['price']
                            PRICE_6H_AGO = False
                    if PRICE_3H_AGO:        
                        if TrackerController.isEqualToReferenceDatetime(doc['_id'], year_3h_ago, month_3h_ago, day_3h_ago, hour_3h_ago, minute_3h_ago, logger):
                            price_3h_ago = doc['price']
                            PRICE_3H_AGO = False
                    if PRICE_1H_AGO:        
                        if TrackerController.isEqualToReferenceDatetime(doc['_id'], year_1h_ago, month_1h_ago, day_1h_ago, hour_1h_ago, minute_1h_ago, logger):
                            price_1h_ago = doc['price']
                            PRICE_1H_AGO = False
                    



        

                    #logger.info(doc)
                    doc_vol = doc['volume']
                    doc_buy_vol = doc['buy_volume']
                    # if doc['volume'] != 0:
                    #     doc_buy_vol = doc['buy_volume'] / doc['volume']
                    # else:
                    #     doc_buy_vol = 0.5
                    
                    doc_buy_trd = doc['buy_n']
                    doc_trades = doc['n_trades']

                    # if doc['n_trades'] != 0:
                    #     doc_buy_trd = doc['buy_n'] / doc['n_trades']
                    # else:
                    #     doc_buy_trd = 0.5

                    volumes_24h_list.append(doc_vol)
                    buy_volume_24h_list.append(doc_buy_vol)
                    buy_trades_24h_list.append(doc_buy_trd)
                    trades_24h_list.append(doc_trades)

                    timestamp_trade = datetime.fromisoformat(doc['_id'])
                    #logger.info(timestamp_trade)
                    # average volume 5m

                    # if timestamp trade is not older than 5 minutes
                    if timestamp_trade > reference_5m_datetime:
                        #logger.info(f'{timestamp_trade} --- {reference_5m_datetime}')
                        volumes_5m_list.append(doc_vol)
                        volumes_15m_list.append(doc_vol)
                        volumes_30m_list.append(doc_vol)
                        volumes_60m_list.append(doc_vol)

                        trades_5m_list.append(doc_trades)
                        trades_15m_list.append(doc_trades)
                        trades_30m_list.append(doc_trades)
                        trades_60m_list.append(doc_trades)
                        trades_3h_list.append(doc_trades)
                        trades_6h_list.append(doc_trades)

                        buy_volume_5m_list.append(doc_buy_vol)
                        buy_volume_15m_list.append(doc_buy_vol)
                        buy_volume_30m_list.append(doc_buy_vol)
                        buy_volume_60m_list.append(doc_buy_vol)

                        buy_trades_5m_list.append(doc_buy_trd)
                        buy_trades_15m_list.append(doc_buy_trd)
                        buy_trades_30m_list.append(doc_buy_trd)
                        buy_trades_60m_list.append(doc_buy_trd)

                        volumes_6h_list.append(doc_vol)
                        buy_volume_6h_list.append(doc_buy_vol)
                        buy_trades_6h_list.append(doc_buy_trd)
                        
                        volumes_3h_list.append(doc_vol)
                        buy_volume_3h_list.append(doc_buy_vol)
                        buy_trades_3h_list.append(doc_buy_trd)

                        continue
                    
                    # if timestamp trade is not older than 15 minutes
                    elif timestamp_trade > reference_15m_datetime:
                        volumes_15m_list.append(doc_vol)
                        volumes_30m_list.append(doc_vol)
                        volumes_60m_list.append(doc_vol)

                        trades_15m_list.append(doc_trades)
                        trades_30m_list.append(doc_trades)
                        trades_60m_list.append(doc_trades)
                        trades_3h_list.append(doc_trades)
                        trades_6h_list.append(doc_trades)

                        buy_volume_15m_list.append(doc_buy_vol)
                        buy_volume_30m_list.append(doc_buy_vol)
                        buy_volume_60m_list.append(doc_buy_vol)

                        buy_trades_15m_list.append(doc_buy_trd)
                        buy_trades_30m_list.append(doc_buy_trd)
                        buy_trades_60m_list.append(doc_buy_trd)

                        volumes_6h_list.append(doc_vol)
                        buy_volume_6h_list.append(doc_buy_vol)
                        buy_trades_6h_list.append(doc_buy_trd)
                        
                        volumes_3h_list.append(doc_vol)
                        buy_volume_3h_list.append(doc_buy_vol)
                        buy_trades_3h_list.append(doc_buy_trd)

                        continue
                    
                    # if timestamp trade is not older than 30 minutes
                    elif timestamp_trade > reference_30m_datetime:
                        volumes_30m_list.append(doc_vol)
                        volumes_60m_list.append(doc_vol)

                        trades_30m_list.append(doc_trades)
                        trades_60m_list.append(doc_trades)
                        trades_3h_list.append(doc_trades)
                        trades_6h_list.append(doc_trades)

                        buy_volume_30m_list.append(doc_buy_vol)
                        buy_volume_60m_list.append(doc_buy_vol)

                        buy_trades_30m_list.append(doc_buy_trd)
                        buy_trades_60m_list.append(doc_buy_trd)

                        volumes_6h_list.append(doc_vol)
                        buy_volume_6h_list.append(doc_buy_vol)
                        buy_trades_6h_list.append(doc_buy_trd)
                        
                        volumes_3h_list.append(doc_vol)
                        buy_volume_3h_list.append(doc_buy_vol)
                        buy_trades_3h_list.append(doc_buy_trd)

                        continue
                    
                    # if timestamp trade is not older than 60 minutes
                    elif timestamp_trade > reference_1h_datetime:
                        
                        volumes_60m_list.append(doc_vol)
                        buy_volume_60m_list.append(doc_buy_vol)
                        buy_trades_60m_list.append(doc_buy_trd)
                        
                        trades_60m_list.append(doc_trades)
                        trades_3h_list.append(doc_trades)
                        trades_6h_list.append(doc_trades)

                        volumes_6h_list.append(doc_vol)
                        buy_volume_6h_list.append(doc_buy_vol)
                        buy_trades_6h_list.append(doc_buy_trd)
                        
                        volumes_3h_list.append(doc_vol)
                        buy_volume_3h_list.append(doc_buy_vol)
                        buy_trades_3h_list.append(doc_buy_trd)

                        continue
                    
                    # if timestamp trade is not older than 3 hours
                    elif timestamp_trade > reference_3h_datetime:
                        
                        volumes_6h_list.append(doc_vol)
                        buy_volume_6h_list.append(doc_buy_vol)
                        buy_trades_6h_list.append(doc_buy_trd)

                        volumes_3h_list.append(doc_vol)
                        buy_volume_3h_list.append(doc_buy_vol)
                        buy_trades_3h_list.append(doc_buy_trd)

                        trades_3h_list.append(doc_trades)
                        trades_6h_list.append(doc_trades)

                        continue
                    
                    # if timestamp trade is not older than 6 hours
                    elif timestamp_trade > reference_6h_datetime:
                        
                        volumes_6h_list.append(doc_vol)
                        buy_volume_6h_list.append(doc_buy_vol)
                        buy_trades_6h_list.append(doc_buy_trd)
                        trades_6h_list.append(doc_trades)

                        continue
                    
                # Compute the volume statistics of the last minute. Since we reach the end of the "docs" variable

                price_now = doc['price']
                #logger.info(doc)
                volume_1m = round_(doc['volume'] / avg_volume_1_month,2)
                if doc['volume'] != 0:
                    buy_volume_perc_1m = round_(doc['buy_volume'] / doc['volume'],2)
                else:
                    buy_volume_perc_1m = 0.5
                
                if doc['n_trades'] != 0:
                    buy_trades_perc_1m = round_(doc['buy_n'] / doc['n_trades'],2)
                else:
                    buy_trades_perc_1m = 0.5

                # Compute price changes if 
                if price_1h_ago is not None:
                    price_variation_1h = round_((price_now - price_1h_ago) / price_1h_ago, 4)
                if price_3h_ago is not None:
                    price_variation_3h = round_((price_now - price_3h_ago) / price_3h_ago, 4)
                if price_6h_ago is not None:
                    price_variation_6h = round_((price_now - price_6h_ago) / price_6h_ago, 4)
                if price_1d_ago is not None:
                    price_variation_1d = round_((price_now - price_1d_ago) / price_1d_ago, 4)

                    

                if sum(volumes_24h_list) != 0 and sum(trades_24h_list) != 0:
                    volumes_24h = round_(np.mean(volumes_24h_list) / avg_volume_1_month, 2) 
                    buy_volume_perc_24h = round_(sum(buy_volume_24h_list)/sum(volumes_24h_list),2)
                    buy_trades_perc_24h = round_(sum(buy_trades_24h_list)/sum(trades_24h_list),2)
                    volumes_24h_std = round_(np.std(volumes_24h_list) / std_volume_1_month, 2)
                else:
                    volumes_24h = None
                    buy_volume_perc_24h = None
                    buy_trades_perc_24h = None
                    volumes_24h_std = None

                
                if sum(volumes_6h_list) != 0 and sum(trades_6h_list) != 0:
                    volumes_6h = round_(np.mean(volumes_6h_list) / avg_volume_1_month, 2) 
                    buy_volume_perc_6h = round_(sum(buy_volume_6h_list)/sum(volumes_6h_list),2)
                    buy_trades_perc_6h = round_(sum(buy_trades_6h_list)/sum(trades_6h_list),2)
                    volumes_6h_std = round_(np.std(volumes_6h_list) / std_volume_1_month, 2)
                else:
                    volumes_6h = None
                    buy_volume_perc_6h = None
                    buy_trades_perc_6h = None
                    volumes_6h_std = None

                if sum(volumes_3h_list) != 0 and sum(trades_3h_list) != 0:
                    volumes_3h = round_(np.mean(volumes_3h_list) / avg_volume_1_month, 2) 
                    buy_volume_perc_3h = round_(sum(buy_volume_3h_list)/sum(volumes_3h_list),2)
                    buy_trades_perc_3h = round_(sum(buy_trades_3h_list)/sum(trades_3h_list),2)
                    volumes_3h_std = round_(np.std(volumes_3h_list) / std_volume_1_month, 2)
                else:
                    volumes_3h = None
                    buy_volume_perc_3h = None
                    buy_trades_perc_3h = None
                    volumes_3h_std = None


                if sum(volumes_60m_list) != 0 and sum(trades_60m_list) != 0:
                    volumes_60m = round_(np.mean(volumes_60m_list) / avg_volume_1_month, 2) 
                    buy_volume_perc_60m = round_(sum(buy_volume_60m_list)/sum(volumes_60m_list),2)
                    buy_trades_perc_60m = round_(sum(buy_trades_60m_list)/sum(trades_60m_list),2)
                    volumes_60m_std = round_(np.std(volumes_60m_list) / std_volume_1_month, 2)
                else:
                    volumes_60m = None
                    buy_volume_perc_60m = None
                    buy_trades_perc_60m = None
                    volumes_60m_std = None
                

                if sum(volumes_30m_list) != 0 and sum(trades_30m_list) != 0:
                    volumes_30m = round_(np.mean(volumes_30m_list) / avg_volume_1_month, 2) 
                    buy_volume_perc_30m = round_(sum(buy_volume_30m_list)/sum(volumes_30m_list),2)
                    buy_trades_perc_30m = round_(sum(buy_trades_30m_list)/sum(trades_30m_list),2)
                    volumes_30m_std = round_(np.std(volumes_30m_list) / std_volume_1_month, 2)
                else:
                    volumes_30m = None
                    buy_volume_perc_30m = None
                    buy_trades_perc_30m = None
                    volumes_30m_std = None


                if sum(volumes_15m_list) != 0 and sum(trades_15m_list) != 0:
                    volumes_15m = round_(np.mean(volumes_15m_list) / avg_volume_1_month, 2) 
                    buy_volume_perc_15m = round_(sum(buy_volume_15m_list)/sum(volumes_15m_list),2)
                    buy_trades_perc_15m = round_(sum(buy_trades_15m_list)/sum(trades_15m_list),2)
                    volumes_15m_std = round_(np.std(volumes_15m_list) / std_volume_1_month, 2)
                else:
                    volumes_15m = None
                    buy_volume_perc_15m = None
                    buy_trades_perc_15m = None
                    volumes_15m_std = None

                if sum(volumes_5m_list) != 0 and sum(trades_5m_list) != 0:
                    volumes_5m = round_(np.mean(volumes_5m_list) / avg_volume_1_month, 2) 
                    buy_volume_perc_5m = round_(sum(buy_volume_5m_list)/sum(volumes_5m_list),2)
                    buy_trades_perc_5m = round_(sum(buy_trades_5m_list)/sum(trades_5m_list),2)
                    volumes_5m_std = round_(np.std(volumes_5m_list) / std_volume_1_month, 2)
                else:
                    volumes_5m = None
                    buy_volume_perc_5m = None
                    buy_trades_perc_5m = None
                    volumes_5m_std = None



                doc_db = {'_id': now_isoformat,"price": price_now, "price_%_1d" : price_variation_1d, "price_%_6h" : price_variation_6h, "price_%_3h" : price_variation_3h, "price_%_1h" : price_variation_1h,
                        'vol_1m': volume_1m, 'buy_vol_1m': buy_volume_perc_1m, 'buy_trd_1m': buy_trades_perc_1m,
                        'vol_5m': volumes_5m, 'vol_5m_std': volumes_5m_std, 'buy_vol_5m': buy_volume_perc_5m, 'buy_trd_5m': buy_trades_perc_5m,
                        'vol_15m': volumes_15m, 'vol_15m_std': volumes_15m_std, 'buy_vol_15m': buy_volume_perc_15m, 'buy_trd_15m': buy_trades_perc_15m,
                        'vol_30m': volumes_30m, 'vol_30m_std': volumes_30m_std, 'buy_vol_30m': buy_volume_perc_30m, 'buy_trd_30m': buy_trades_perc_30m,
                        'vol_60m': volumes_60m, 'vol_60m_std': volumes_60m_std, 'buy_vol_60m': buy_volume_perc_60m, 'buy_trd_60m': buy_trades_perc_60m,
                        'vol_3h': volumes_3h, 'vol_3h_std': volumes_3h_std, 'buy_vol_3h': buy_volume_perc_3h, 'buy_trd_3h': buy_trades_perc_3h,
                        'vol_6h': volumes_6h, 'vol_6h_std': volumes_6h_std, 'buy_vol_6h': buy_volume_perc_6h, 'buy_trd_6h': buy_trades_perc_6h,
                        'vol_24h': volumes_24h, 'vol_24h_std': volumes_24h_std, 'buy_vol_24h': buy_volume_perc_24h,'buy_trd_24h': buy_trades_perc_24h,
                        }
                
                # CHECK IF THE EVENT CAN TRIGGER A BUY ORDER
                asyncio.create_task(TradingController.check_event_triggering(coin, doc_db, volatility_coin, logger, db_trading, db_logger, trading_coins_list, trading_configuration))
            
                #logger.info(doc_db)

                db_tracker[coin].insert(doc_db)

            # if Bitcoin was not retrieved 
            elif coin == 'BTCUSDT':
                msg = 'No trade was saved in Market_Trades in the last minute for list1'
                logger.info(msg)
                db_logger[DATABASE_API_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
            
            

        