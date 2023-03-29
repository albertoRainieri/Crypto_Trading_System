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

    def get_db(self, db_name):
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
            return await client.get(url)


    async def gather(coins_list, interval='1d', limit=1):
        urls = []

        for coin in coins_list:
            urls.append(BINANCE_ENDPOINT + 'klines?symbol=' + coin + '&interval=' + interval + '&limit=' + str(limit))

        
        resps = await asyncio.gather(*map(BinanceController.get_async, urls))
        data = [json.loads(resp.text) for resp in resps]

        return data


    async def exchange_info():
        '''
        This function call "ExchangeInfo" Binance API for all the available pairs
        '''
        client = await AsyncClient.create()

        # fetch exchange info
        res = await client.get_exchange_info()
        await client.close_connection()

        return res

    def sort_pairs_by_volume(res):
        '''
        This function takes the list of pair (from "ExchangeInfo" function) and gathers information for each one of them.
        Finally it creates a sorted list of pair by volume
        '''

        #print('res', res)
        all_usdt_coins = []
        for coin in res['symbols']:
            if coin['symbol'][-4:] == 'USDT' and coin['symbol'] != 'USDCUSDT' and coin['symbol'] != 'TUSDUSDT':
                all_usdt_coins.append(coin['symbol'])
        
        number_coins = len(all_usdt_coins)
        print(f'Number of coins: {number_coins}')


        trades = asyncio.run(BinanceController.gather(coins_list=all_usdt_coins))
        tries = 0
        try:
            if tries < 10:
                tries += 1
                trades = asyncio.run(BinanceController.gather(coins_list=all_usdt_coins))
        except:
            BinanceController.sort_pairs_by_volume()


        list_volumes = []
        most_traded_coins_list = []

        for coin,trade in zip(all_usdt_coins, trades):
            print(trade)
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

    def main_sort_pairs_list():
        loop = asyncio.get_event_loop()
        info = loop.run_until_complete(BinanceController.exchange_info())
        BinanceController.sort_pairs_by_volume(info)