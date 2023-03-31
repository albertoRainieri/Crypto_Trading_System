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
from tracker.app.Helpers.Helpers import round_, timer_func
import numpy as np
from binance import AsyncClient


import httpx
import asyncio


class BinanceController:

    def __init__(self) -> None:
        self.db = DatabaseConnection()

    def get_db(db_name):
        '''
        Establish connectivity with db
        '''
        # Retrieve Database DATABASE_DATA_STORAGE
        database = DatabaseConnection()
        db = database.get_db(db_name)
        return db
    
    

    async def get_async(url):
        #header = CryptoController.getHeader()
        async with httpx.AsyncClient() as client:
            try:
                return await client.get(url)
            except:
                return False
        
    async def gather_total(coins_list, logger, interval='1d', limit=1):

        total_data = []
        for slice_coin_list in coins_list:
            data = await BinanceController.gather(slice_coin_list,logger, interval, limit)
            if not data:
                return False
            for data_i in data:
                total_data.append(data_i)

        return total_data



    async def gather(coins_list, logger, interval='1d', limit=1):
        urls = []


        for coin in coins_list:
            urls.append(BINANCE_ENDPOINT + 'klines?symbol=' + coin + '&interval=' + interval + '&limit=' + str(limit))

        try:
            resps = await asyncio.gather(*map(BinanceController.get_async, urls))
            if not resps:
                return False
            sleep(1)
            print(f'Succesfull request fo coin slice {coins_list} ')
        except:
            sleep(1)
            return False

        data = []
        for resp in resps:

            if type(resp) != bool:
                data.append(json.loads(resp.text))
            else:
                return False

        return data


    async def exchange_info():
        '''
        This function call "ExchangeInfo" Binance API for all the available pairs
        '''
        client = await AsyncClient.create()

        # fetch exchange info
        try:
            res = await client.get_exchange_info()
            await client.close_connection()
        except:
            sleep(5)
            return False

        return res

    def sort_pairs_by_volume(res, logger=LoggingController.start_logging(), tries=0):
        '''
        This function takes the list of pair (from "ExchangeInfo" function) and gathers information for each one of them.
        Finally it creates a sorted list of pair by volume
        '''

        #print('res', res)
        all_usdt_coins = []
        for coin in res['symbols']:
            if coin['symbol'][-4:] == 'USDT' and coin['symbol'] != 'USDCUSDT' and coin['symbol'] != 'TUSDUSDT':
                all_usdt_coins.append(coin['symbol'])
        
        #all_usdt_coins = all_usdt_coins[:10]
        number_coins = len(all_usdt_coins)
        print(f'Number of coins: {number_coins}')

        # let's slice this list in 10 pieces to avoid http connection timeout to the binance server
        number_slices = 5
        slice = int(number_coins / number_slices)
        slice_usdt_coin_list = []
        slice_usdt_coin_list.append(all_usdt_coins[:slice])

        for i in range(1,number_slices):
            if i != number_slices - 1:
                slice_usdt_coin_list.append(all_usdt_coins[slice*i:slice*(i+1)])
            else:
                slice_usdt_coin_list.append(all_usdt_coins[slice*i:])

        #total_pairs = asyncio.run(BinanceController.gather(coins_list=all_usdt_coins))
        total_pairs = asyncio.run(BinanceController.gather_total(coins_list=slice_usdt_coin_list, logger=logger))

        if not total_pairs:
            if tries < 2:
                tries += 1
                print(f'{tries} tries')
                result = BinanceController.sort_pairs_by_volume(res, logger, tries)
            else:
                logger.error('Update of the list of instruments has failed')
                
                

        if not total_pairs:
            return
        
        list_volumes = []
        most_traded_coins_list = []

        for coin,trade in zip(all_usdt_coins, total_pairs):
            #print(trade)
            dict_volume = {'coin': coin, 'volume': float(trade[0][7])}
            list_volumes.append(dict_volume)

        list_volumes.sort(key=lambda x: x['volume'], reverse=True)

        for obj in list_volumes:
            most_traded_coins_list.append(obj['coin'])


        sorted_volumes = {'most_traded_coins': list_volumes}
        with open("/tracker/json/sorted_instruments.json", "w") as outfile_volume:
            outfile_volume.write(json.dumps(sorted_volumes))

        most_traded_coins = {'most_traded_coins': most_traded_coins_list}
        with open("/tracker/json/most_traded_coins.json", "w") as outfile_volume:
            outfile_volume.write(json.dumps(most_traded_coins))

        
        # UPDATE DB BENCHMARK. THE INFO UPDATED IS ABOUT THE FREQUENCY OF THE COIN TRADED.
        # IF A COIN FALLS OUT OF THE BEST POSITIONS, IT WILL GET A NEGATIVE SCORE.

        db_benchmark = BinanceController.get_db(DATABASE_BENCHMARK)
        list_coins_benchmark = db_benchmark.list_collection_names()

        # iterate through each pair in the db
        for coin in list_coins_benchmark:
            # update benchmark db --> (field: best trades)
            cursor_benchmark = list(db_benchmark[coin].find())
            id_benchmark = cursor_benchmark[0]['_id']

            # if the coin from db is not present in the list updated "most_traded_coins_list"
            if coin in most_traded_coins_list[:NUMBER_COINS_TO_TRADE*SLICES+COINS_PRIORITY]:
                
                # In case it is not the first time to compute this statistics on db benchmark
                if 'Best_Trades' in cursor_benchmark[0]:
                    trade_score = cursor_benchmark[0]['Best_Trades']
                    trade_score_split = trade_score.split('/')
                    new_score = int(trade_score_split[0]) + 1
                    new_total = int(trade_score_split[-1]) + 1
                    new_trade_score = str(new_score) + '/' + str(new_total)

                    last_30_trades = cursor_benchmark[0]['Last_30_Trades']

                    # if len of this list is greater than 30 (1 month) I delete the oldest one (first element of the list)
                    if len(last_30_trades['list_last_30_trades']) == 30:
                        last_30_trades['list_last_30_trades'].append(1)
                        last_30_trades['list_last_30_trades'] = last_30_trades['list_last_30_trades'][1:]
                    else:
                        last_30_trades['list_last_30_trades'].append(1)
                    last_30_trades['score_last_30_trades'] = sum(last_30_trades['list_last_30_trades']) / len(last_30_trades['list_last_30_trades'])

                    db_benchmark[coin].update_one({"_id": id_benchmark}, {"$set": {"Best_Trades": new_trade_score, 'Last_30_Trades': last_30_trades}})
                # this is the first time that this statistics is computed
                else:
                    new_score = 1
                    new_total = 1
                    new_trade_score = str(new_score) + '/' + str(new_total)
                    last_30_trades = {'list_last_30_trades': [1], 'score_last_30_trades': 1}

                    db_benchmark[coin].update_one({"_id": id_benchmark}, {"$set": {"Best_Trades": new_trade_score, 'Last_30_Trades': last_30_trades}})
            else:
                if 'Best_Trades' in cursor_benchmark[0]:
                    trade_score = cursor_benchmark[0]['Best_Trades']
                    trade_score_split = trade_score.split('/')
                    score = trade_score_split[0]
                    new_total = int(trade_score_split[-1]) + 1
                    new_trade_score = score + '/' + str(new_total)

                    last_30_trades = cursor_benchmark[0]['Last_30_Trades']

                    # if len of this list is greater than 30 (1 month) I delete the oldest one (first element of the list)
                    if len(last_30_trades['list_last_30_trades']) == 30:
                        last_30_trades['list_last_30_trades'].append(0)
                        last_30_trades['list_last_30_trades'] = last_30_trades['list_last_30_trades'][1:]
                    else:
                        last_30_trades['list_last_30_trades'].append(0)
                    last_30_trades['score_last_30_trades'] = sum(last_30_trades['list_last_30_trades']) / len(last_30_trades['list_last_30_trades'])

                    db_benchmark[coin].update_one({"_id": id_benchmark}, {"$set": {"Best_Trades": new_trade_score, 'Last_30_Trades': last_30_trades}})
                
                else:
                    new_score = 0
                    new_total = 1
                    new_trade_score = str(new_score) + '/' + str(new_total)
                    last_30_trades = {'list_last_30_trades': [0], 'score_last_30_trades': 0}

                    db_benchmark[coin].update_one({"_id": id_benchmark}, {"$set": {"Best_Trades": new_trade_score, 'Last_30_Trades': last_30_trades}})



    def main_sort_pairs_list(logger=LoggingController.start_logging()):
        loop = asyncio.get_event_loop()
        info = loop.run_until_complete(BinanceController.exchange_info())
        if not info:
            logger.error('Something went wrong with BinanceController.exchange_info')
            info = loop.run_until_complete(BinanceController.exchange_info())
        BinanceController.sort_pairs_by_volume(info, logger)