import os,sys
sys.path.insert(0,'../..')

from time import time, sleep
from random import randint
import json
from constants.constants import *
import requests
import numpy as np
from datetime import datetime, timedelta, timezone
from operator import itemgetter
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from backend.app.Helpers.Helpers import round_, timer_func
import numpy as np
from binance import AsyncClient


import httpx
import asyncio


class BinanceController:

    def __init__(self) -> None:
        self.db = DatabaseConnection()

    def get_db(self, db_name):
        '''
        Establish connectivity with db
        '''
        # Retrieve Database DATABASE_DATA_STORAGE
        database = DatabaseConnection()
        db = database.get_db(db_name)
        return db
    


    # async def get_async(url):
    #     #header = CryptoController.getHeader()
    #     async with httpx.AsyncClient() as client:
    #         return await client.get(url)


    #@timer_func
    def launch(coin_list, logger, limit=1000):
        trades = asyncio.run(BinanceController.getTrades(coin_list=coin_list, limit=limit, logger=logger))
        return trades
    
    @staticmethod
    async def getTrades(coin_list, logger, limit):
        urls= []
        trades = {}
            
        for coin in coin_list:
            urls.append(BINANCE_ENDPOINT + 'trades?symbol=' + coin + '&limit=' + str(limit[coin]))
        
        #logger.info(coin_list)
        try: 
            resps = await asyncio.gather(*map(BinanceController.get_async, urls))
            if not resps:
                logger.error('Error Binance API. function getAsync')
                return False
        except:
            logger.error('Error Binance API. function getTrades')
            return False


        data = [json.loads(resp.text) for resp in resps]

        for coin,info in zip(coin_list, data):
            trades[coin] = info


        return trades

    async def get_async(url):
        #header = CryptoController.getHeader()
        async with httpx.AsyncClient() as client:
            try:
                return await client.get(url)
            except:
                return False
    

    @staticmethod
    def getStatistics_onTrades(coin_list=["BTCUSDT"], database=None, db_logger=None, logger=None, start_minute=datetime.now().minute, sleep_seconds=SLEEP_SECONDS):
        '''
        This API returns the last 150 transactions for a particular instrument
        '''
        resp = {}
        doc_db = {}
        prices = {}
        trades_sorted = {}
        n_trades = {}
        limit = {}

        # JSON file
        # Overwrite coin_list variable
        f = open ('/backend/json/most_traded_coins.json', "r")
        data = json.loads(f.read())
        coin_list = data["most_traded_coins"][:NUMBER_COINS_TO_TRADE*SLICES+COINS_PRIORITY]
        #coin_list[0]
        
        range_limits = [(range(0,10), 1000), (range(10,25),900), (range(25,50), 800), (range(50,100), 700), (range(100,200), 600), (range(200,500), 500)]

        for instrument_name, n_instrument in zip(coin_list, range(len(coin_list))):
            resp[instrument_name] = []
            doc_db[instrument_name] = {"_id": None, "price": None, "n_trades": 0,"volume": 0 , "buy_volume": 0, "sell_volume": 0, "buy_n": 0, "sell_n": 0, "quantity": 0}
            prices[instrument_name] = None
            trades_sorted[instrument_name] = []
            n_trades[instrument_name] = 0

            for range_limit in range_limits:
                if n_instrument in range_limit[0]:
                    limit[instrument_name] = range_limit[1]
                    break

            # n_trades_p_s_dict[instrument_name] = []
        
        slice_i=0
        errors = 0
        attempts = 0

        while start_minute == datetime.now().minute:
            
            slice_coins_to_trade = (slice_i % SLICES)+1
            #logger.info(f'Slice: {slice_coins_to_trade}/{SLICES}  ---  Fetched {COINS_PRIORITY} priority coins + {NUMBER_COINS_TO_TRADE} regular coins')

            if slice_coins_to_trade == 1:
                coin_list = data["most_traded_coins"][:COINS_PRIORITY] + data["most_traded_coins"][COINS_PRIORITY:NUMBER_COINS_TO_TRADE*slice_coins_to_trade+COINS_PRIORITY]
            else:
                coin_list = data["most_traded_coins"][:COINS_PRIORITY] + data["most_traded_coins"][NUMBER_COINS_TO_TRADE*(slice_coins_to_trade-1)+COINS_PRIORITY:NUMBER_COINS_TO_TRADE*slice_coins_to_trade+COINS_PRIORITY]
            
            try:
                attempts += 1
                trades = BinanceController.launch(coin_list=coin_list, logger=logger, limit=limit)

                # if something goes wrong
                if not trades:
                    errors += 1
                    #logger.error('Trades is False')
                    sleep(sleep_seconds)
                    continue

            except Exception as e:
                #logger.error(f'Binance API Request Error. function getStatisticOnTrades')
                errors += 1
                sleep(sleep_seconds)
                continue

            
            instrument_names = list(trades.keys())
            
            for instrument_name in instrument_names:
                if len(trades[instrument_name]) == 0:
                    pass

                # use LAST_TRADE_TIMESTAMP to fetch only the new trades
                LAST_TRADE_TIMESTAMP = os.getenv(f"LAST_TRADE_TIMESTAMP_{instrument_name}")

                if LAST_TRADE_TIMESTAMP == None:
                    now = datetime.now()
                    seconds = now.second
                    LAST_TRADE_TIMESTAMP = (datetime.now() - timedelta(seconds=seconds)).isoformat()
                
                
                # if db is None
                if database == None:
                    for trade in trades[instrument_name]:
                        datetime_trade = datetime.fromtimestamp(trade["t"]/1000)
                        if datetime_trade > datetime.fromisoformat(LAST_TRADE_TIMESTAMP):
                            doc = {"order":trade["s"], "price": trade["p"], "quantity": trade["q"], "timestamp": datetime_trade, "trade_id": trade["d"]}
                            resp[instrument_name].append(doc)

                # if db in not None. save the data
                else:
                    timestamps=[]
                    current_n_trades = 0
                    for trade in trades[instrument_name]:
                        try:
                            datetime_trade = datetime.fromtimestamp(trade["time"]/1000)
                        except:
                            #print('ERROR DATETIme: ', trade)
                            break
                        if datetime_trade > datetime.fromisoformat(LAST_TRADE_TIMESTAMP):
                            timestamps.append(datetime_trade)
                            n_trades[instrument_name] += 1
                            current_n_trades += 1
                            if trade['isBuyerMaker']:
                                order = 'SELL'
                            else:
                                order = 'BUY'
                            
                            doc = {"order":order, "price": trade["price"], "quantity": trade["qty"], "volume": trade["quoteQty"], "timestamp": datetime_trade, "trade_id": trade["id"]}
                            resp[instrument_name].append(doc)

                            # ANALYZE STATISTICS TO SAVE TO DB

                            prices[instrument_name] = float(doc["price"])

                            doc_db[instrument_name]["quantity"] += float(doc["quantity"])
                            
                            if doc["order"] == "BUY":
                                doc_db[instrument_name]["buy_n"] += 1
                                doc_db[instrument_name]["buy_volume"] += float(doc["volume"])
                            else:
                                doc_db[instrument_name]["sell_n"] += 1
                                doc_db[instrument_name]["sell_volume"] += float(doc["volume"])


                    if current_n_trades != 0:
                        doc_db[instrument_name]["n_trades"] += current_n_trades
                        
                        resp[instrument_name] = sorted(resp[instrument_name], key=itemgetter('timestamp'), reverse=False)
                        os.environ[f"LAST_TRADE_TIMESTAMP_{instrument_name}"] = resp[instrument_name][-1]['timestamp'].isoformat()

                        #logger.error(f'{instrument_name}: {current_n_trades}/{limit[instrument_name]}')
                        if current_n_trades >= limit[instrument_name]:
                            position = data["most_traded_coins"].index(instrument_name)
                            logger.error(f'CRITICAL: Limit of {limit[instrument_name]} trades for {instrument_name}; Position Coin: {position}')
                            # save log to db{instrument_name} has been reached; Position Coin: {data["most_traded_coins"].index(instrument_name)}
                            BinanceController.save_logs_to_db(instrument_name, position, range_limits, db_logger)
                        elif current_n_trades >= limit[instrument_name] * 0.8:
                            position = data["most_traded_coins"].index(instrument_name)
                            logger.error(f'Number of trades for {instrument_name} are more than the 80% of the capacity limit {limit[instrument_name]}; Position Coin: {position}')
                        # elif current_n_trades >= limit[instrument_name] * 0.6:
                        #     logger.error(f'Number of trades for {instrument_name} are more than the 60% of the capacity limit {limit[instrument_name]}') 

            #logger.info(f'Sleep Time: {sleep_seconds}s')
            sleep(sleep_seconds)
            slice_i += 1
        
        pairs_traded = 0
        pairs_not_traded = 0
        for instrument_name in list(n_trades.keys()):                
            if n_trades[instrument_name] != 0:
                # logger.info({'instrument_name': instrument_name, 'price': prices[instrument_name], 'n_trades': doc_db[instrument_name]["n_trades"], 'volume': round_(doc_db[instrument_name]["volume"],2), 'quantity': round_(doc_db[instrument_name]["quantity"],2)})
                pairs_traded += 1
            else:
                pairs_not_traded += 1
        
        if pairs_not_traded == 0:
            logger.info('All pairs have been traded in the last minute')
            
            if errors != 0:
                msg = f'{errors}/{attempts} errors in the last minute for reaching Binance API'
                logger.error(msg)
                db_logger[DATABASE_API_ERROR].insert({'_id': datetime.now().isoformat(), 'msg': msg})

        elif pairs_traded == 0:
            msg = 'SUPER CRITICAL: NO PAIRS HAS BEEN TRADED, POSSIBLE IP BAN'
            logger.error(msg)
            db_logger[DATABASE_API_ERROR].insert({'_id': datetime.now().isoformat(), 'msg': msg})

        else:
            total_traded = pairs_traded + pairs_not_traded
            logger.info(f'{pairs_traded}/{total_traded} have been traded in the last minute')
            if errors != 0:
                msg = f'{errors}/{attempts} errors in the last minute for reaching Binance API'
                logger.error(msg)
                db_logger[DATABASE_API_ERROR].insert({'_id': datetime.now().isoformat(), 'msg': msg})
        
        if database != None:
            trades_sorted = BinanceController.saveTrades_toDB(n_trades, prices, doc_db, database, trades_sorted, resp)
        else:
            for instrument_name in prices:
                trades_sorted[instrument_name]= sorted(resp[instrument_name], key=itemgetter('timestamp'), reverse=False)
        return slice_i

    #@timer_func
    def saveTrades_toDB(n_trades, prices, doc_db, database, trades_sorted, resp):
        for instrument_name in n_trades:
            if n_trades[instrument_name] != 0:
                #doc_db[instrument_name]["n_trades_p_s"]= round_(np.mean(n_trades_p_s_dict[instrument_name]),2)
                doc_db[instrument_name]["price"]=prices[instrument_name]
                doc_db[instrument_name]["quantity"] = round_(doc_db[instrument_name]["quantity"],2)
                doc_db[instrument_name]["volume"] =  round_(doc_db[instrument_name]["buy_volume"] + doc_db[instrument_name]["sell_volume"],2)
                doc_db[instrument_name]["sell_volume"] = round(doc_db[instrument_name]["sell_volume"],2)
                doc_db[instrument_name]["buy_volume"] = round(doc_db[instrument_name]["buy_volume"],2)
                doc_db[instrument_name]['_id']= datetime.now().isoformat()
                database[instrument_name].insert(doc_db[instrument_name])
                trades_sorted[instrument_name]= sorted(resp[instrument_name], key=itemgetter('timestamp'), reverse=False)

        return trades_sorted
    

    def start_live_trades(self, coin_list, logger=LoggingController.start_logging()):
        
        now = datetime.now()

        sleep_seconds = BinanceController.isIncreaseSleepSeconds(now, logger)
        current_minute = now.minute
        db = self.get_db(DATABASE_MARKET)
        db_logger = self.get_db(DATABASE_LOGGING)
        BinanceController.getStatistics_onTrades(coin_list=coin_list, database=db, db_logger=db_logger, logger=logger, start_minute=current_minute, sleep_seconds=sleep_seconds)

    def save_logs_to_db(instrument_name, position, range_limits, db_logger):
        
        
        now = datetime.now(tz=timezone.utc)
        id = str(now.day) + "-" + str(now.month) + "-" + str(now.year)
        last_id = list(db_logger[DATABASE_MARKET].find({'_id': id}))
        #print(last_id)
        # initialize range based on var "rabge_limits"
        range_dict = {'Top_0_2': 0}
        print(instrument_name, position)

        # update current record
        if last_id :
            #last_id = list(last_id)
            #print('last_id[0]: ', last_id[0])
            # update keys of the current record
            last_id[0]['total_errors'] += 1
            if instrument_name in last_id[0]['errors_per_coin']:
                last_id[0]['errors_per_coin'][instrument_name] += 1
            else:
                last_id[0]['errors_per_coin'][instrument_name] = 1

            # update info on errors by position
            errors_by_position = last_id[0]['errors_by_position']

            #if instrument is bitcoin or etheurem
            if instrument_name == 'BTCUSDT' or instrument_name == 'ETHUSDT':
                last_id[0]['errors_by_position']['Top_0_2'] += 1
            else:
                for range_error in list(errors_by_position.keys()):
                    range_split = range_error.split('_')
                    min_range = range_split[1]
                    max_range = range_split[2]
                    current_range = range(int(min_range), int(max_range))
                    if position in current_range:
                        last_id[0]['errors_by_position'][range_error] += 1
                        break
            
            last_id[0].pop('_id')
            db_logger[DATABASE_MARKET].update_one({'_id': id}, {"$set": last_id[0]})
        
        else:
            # initialize doc_id
            range_dict = {'Top_0_2': 0}
            for range_error in range_limits:
                min_range = str(range_error[0][0])
                max_range = str(range_error[0][-1] + 1)
                key = 'Top_' + min_range + '_' + max_range
                range_dict[key] = 0

            if instrument_name == 'BTCUSDT' or instrument_name == 'ETHUSDT':
                range_dict['Top_0_2'] += 1
            else:
                for range_error in list(range_dict.keys()):
                    range_split = range_error.split('_')
                    #print(range_split)
                    min_range = int(range_split[1])
                    max_range = int(range_split[2])
                    #print(min_range)
                    #print(max_range)
                    current_range = range(min_range, max_range)
                    #print(current_range)
                    if position in current_range:
                        range_dict[range_error] += 1
                        break

            doc_ = {"_id": id, "total_errors": 1, "errors_per_coin": {instrument_name: 1}, "errors_by_position": range_dict}
            
            db_logger[DATABASE_MARKET].insert_one(doc_)
            


        
        pass


    def isIncreaseSleepSeconds(now, logger):
        '''
        This function increases the sleep seconds during midnight for
        allowing the update of the instrument in the Tracker Controller.
        This is done to not incur over IP Ban

        It return the variable sleep_seconds
        '''

        hour = now.hour
        minute = now.minute
        
        if hour == 23 and minute >= 58:
            sleep_seconds = 3
            logger.info(f'Sleep seconds variable is switched to {sleep_seconds}')
        else:
            sleep_seconds = SLEEP_SECONDS
            #logger.info(f'Sleep seconds variable is switched to {sleep_seconds}')

        return sleep_seconds