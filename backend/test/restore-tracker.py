import os,sys
sys.path.insert(0,'..')

import pymongo
from datetime import datetime, timedelta
from database.DatabaseConnection import DatabaseConnection
from constants.constants import *

'''
This script updates records in the Tracker Database.
Inputs are "start_time" and "end_time"
'''

# Define the time range for deletion (midnight UTC until 05:28 UTC)
start_time = datetime.utcnow().replace(year=2023, month=5, day=7, hour=0, minute=1, second=0, microsecond=0)
start_time_market = start_time - timedelta(days=1)
end_time = datetime.utcnow().replace(year=2023, month=5, day=10, hour=23, minute=59, second=50, microsecond=0)

# Connect to MongoDB
client = DatabaseConnection()
db_tracker = client.get_db(DATABASE_TRACKER)
db_market = client.get_db(DATABASE_MARKET)

# Get the collection names
coin_collection_tracker = db_tracker.list_collection_names()
coin_collection_market = db_market.list_collection_names()


for coin in coin_collection_tracker:
    docs_tracker = list(db_tracker[coin].find({"_id": {"$gte": start_time, "$lte": end_time}}))
    docs_market = list(db_market[coin].find({"_id": {"$gte": start_time, "$lte": end_time}}))

    for doc in docs_tracker:
        id_tracker = doc['_id']

client.close()
        




def compute_tracker_data(docs_market):

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

    for doc in docs_market:
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



    doc_db = {'_id': now.isoformat(),"price": price_now, "price_%_1d" : price_variation_1d, "price_%_6h" : price_variation_6h, "price_%_3h" : price_variation_3h, "price_%_1h" : price_variation_1h,
            'vol_1m': volume_1m, 'buy_vol_1m': buy_volume_perc_1m, 'buy_trd_1m': buy_trades_perc_1m,
            'vol_5m': volumes_5m, 'vol_5m_std': volumes_5m_std, 'buy_vol_5m': buy_volume_perc_5m, 'buy_trd_5m': buy_trades_perc_5m,
            'vol_15m': volumes_15m, 'vol_15m_std': volumes_15m_std, 'buy_vol_15m': buy_volume_perc_15m, 'buy_trd_15m': buy_trades_perc_15m,
            'vol_30m': volumes_30m, 'vol_30m_std': volumes_30m_std, 'buy_vol_30m': buy_volume_perc_30m, 'buy_trd_30m': buy_trades_perc_30m,
            'vol_60m': volumes_60m, 'vol_60m_std': volumes_60m_std, 'buy_vol_60m': buy_volume_perc_60m, 'buy_trd_60m': buy_trades_perc_60m,
            'vol_3h': volumes_3h, 'vol_3h_std': volumes_3h_std, 'buy_vol_3h': buy_volume_perc_3h, 'buy_trd_3h': buy_trades_perc_3h,
            'vol_6h': volumes_6h, 'vol_6h_std': volumes_6h_std, 'buy_vol_6h': buy_volume_perc_6h, 'buy_trd_6h': buy_trades_perc_6h,
            'vol_24h': volumes_24h, 'vol_24h_std': volumes_24h_std, 'buy_vol_24h': buy_volume_perc_24h,'buy_trd_24h': buy_trades_perc_24h,
            }

    #logger.info(doc_db)

    db_tracker[coin].insert(doc_db)


