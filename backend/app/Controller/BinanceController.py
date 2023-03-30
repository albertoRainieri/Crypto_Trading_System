import os,sys
sys.path.insert(0,'../..')

from time import time, sleep
from random import randint
import json
from constants.constants import *
import requests
import numpy as np
from datetime import datetime, timedelta
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
    def getStatistics_onTrades(coin_list=["BTCUSDT"], database=None, logger=None, start_minute=datetime.now().minute):
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
        
        range_limits = [(range(0,3), 1000), (range(3,10), 800), (range(10,20), 700), (range(20,30), 600), (range(30,50), 500), (range(50,100), 400), (range(100,200), 300), (range(20,500), 200)]

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
                    sleep(SLEEP_SECONDS)
                    continue

            except Exception as e:
                logger.error(f'Binance API Request Error. function getStatisticOnTrades')
                errors += 1
                sleep(SLEEP_SECONDS)
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
                            
                            doc = {"order":order, "price": trade["price"], "quantity": trade["qty"], "volume": trade["quoteQty"], "timestamp": datetime_trade, "trade_id": trade["id"], }
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
                            
                            logger.error(f'Limit of {limit[instrument_name]} trades for {instrument_name} has been reached')
                        elif current_n_trades >= limit[instrument_name] * 0.8:
                            logger.error(f'Number of trades for {instrument_name} are more than the 80% of the capacity limit {limit[instrument_name]}')
                        elif current_n_trades >= limit[instrument_name] * 0.6:
                            logger.error(f'Number of trades for {instrument_name} are more than the 60% of the capacity limit {limit[instrument_name]}') 

            #logger.info(f'Sleep Time: {SLEEP_SECONDS}s')
            sleep(SLEEP_SECONDS)
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
                logger.error(f'{errors}/{attempts} errors/attempts in the last minute for reaching Binance API')
        else:
            total_traded = pairs_traded + pairs_not_traded
            logger.info(f'{pairs_traded}/{total_traded} have been traded in the last minute')
            if errors != 0:
                logger.error(f'{errors}/{attempts} errors in the last minute for reaching Binance API')
        
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
    

    def start_live_trades(self, coin_list=["BTC_USD"], logger=LoggingController.start_logging()):

        current_minute = datetime.now().minute
        db = self.get_db('Market_Trades')
        BinanceController.getStatistics_onTrades(coin_list=coin_list, database=db, logger=logger, start_minute=current_minute)
