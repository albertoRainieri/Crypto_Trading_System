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
from app.Controller.TradingController import TradingController
from tracker.app.Helpers.Helpers import round_, timer_func
import numpy as np
from binance import AsyncClient


import httpx
import asyncio


class BinanceController:


    async def get_async(url):
        #header = CryptoController.getHeader()
        async with httpx.AsyncClient() as client:
            try:
                return await client.get(url)
            except:
                return False
        
    async def gather_total(coins_list, logger, interval, limit=1):

        total_data = []
        for slice_coin_list in coins_list:
            data = await BinanceController.gather(slice_coin_list,logger, interval, limit)
            if not data:
                return False
            for data_i in data:
                total_data.append(data_i)

        return total_data



    async def gather(coins_list, logger, interval, limit=1):
        urls = []


        for coin in coins_list:
            urls.append(BINANCE_ENDPOINT + 'klines?symbol=' + coin + '&interval=' + interval + '&limit=' + str(limit))
            #print(BINANCE_ENDPOINT + 'klines?symbol=' + coin + '&interval=' + interval + '&limit=' + str(limit))

        try:
            resps = await asyncio.gather(*map(BinanceController.get_async, urls))
            if not resps:
                return False
            sleep(1)
            #print(f'Succesfull request fo coin slice {coins_list} ')
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


    async def exchange_info(logger):
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

        logger.info('The request to the API "ExchangeInfo" was succesfull')
        return res

    def sort_pairs_by_volume(client, res, logger=LoggingController.start_logging(), tries=0, db_logger=None):
        '''
        This function takes the list of pair (from "ExchangeInfo" function) and gathers information for each one of them.
        Finally it creates a sorted list of pair by volume
        '''

        logger.info("function 'sort_pairs_by_volume' has started.")
        interval = '1w'
        #print('res', res)
        all_usdt_coins = []
        all_extra_pairs = []
        unique_coins = []
        for coin in res['symbols']:
            if coin['symbol'][-4:] == 'USDT' and coin['symbol'] != 'USDCUSDT' and coin['symbol'] != 'TUSDUSDT' and coin['symbol'] != 'USDPUSDT' and coin['symbol'] != 'BUSDUSDT' and coin['symbol'] != 'USTUSDT' and coin['symbol'] != 'EURUSDT':
                all_usdt_coins.append(coin['symbol'])
                unique_coin=coin['symbol'][:-4]
                if unique_coin not in unique_coins:
                    unique_coins.append(unique_coin)
        
        for coin in res['symbols']:
            if coin['symbol'][-3:] == 'BTC' or  coin['symbol'][-3:] == 'ETH':
                if coin['symbol'][:-3] in unique_coins:
                    all_extra_pairs.append(coin['symbol'])

        n_usdt = len(all_usdt_coins)
        n_other = len(all_extra_pairs)
        logger.info(f'There are {n_usdt} usdt coins')
        logger.info(f'There are {n_other} extra pairs for all <coin>USDT')
        
        #all_usdt_coins = all_usdt_coins[:10]
        # number_coins = len(all_usdt_coins)
        # print(f'Number of coins: {number_coins}')

        # # let's slice this list in 10 pieces to avoid http connection timeout to the binance server
        # number_slices = 5
        # slice = int(number_coins / number_slices)
        # slice_usdt_coin_list = []
        # slice_usdt_coin_list.append(all_usdt_coins[:slice])

        # for i in range(1,number_slices):
        #     if i != number_slices - 1:
        #         slice_usdt_coin_list.append(all_usdt_coins[slice*i:slice*(i+1)])
        #     else:
        #         slice_usdt_coin_list.append(all_usdt_coins[slice*i:])

        # #total_pairs = asyncio.run(BinanceController.gather(coins_list=all_usdt_coins))
        # total_pairs = asyncio.run(BinanceController.gather_total(coins_list=slice_usdt_coin_list, logger=logger, interval=interval))

        # if not total_pairs:
        #     if tries < 2:
        #         tries += 1
        #         print(f'{tries} tries')
        #         result = BinanceController.sort_pairs_by_volume(res=res, logger=logger,tries=tries)
        #     else:
        #         msg = "VOLUMES UPDATE: async calls to Binance API failed"
        #         logger.error(msg)
        #         db_logger[DATABASE_API_ERROR].insert({'_id': datetime.now().isoformat(), 'msg': msg})
                
                

        # if not total_pairs:
        #     return
        
        # list_volumes = []
        # most_traded_coins_list = []

        # for coin,trade in zip(all_usdt_coins, total_pairs):
        #     try:
        #         dict_volume = {'coin': coin, 'volume': float(trade[0][7])}
        #     except:
        #         logger.error(trade)
        #         logger.error(type(trade))
        #         logger.error(coin)
        #         dict_volume = {'coin': coin, 'volume': 0}

        #     list_volumes.append(dict_volume)

        # list_volumes.sort(key=lambda x: x['volume'], reverse=True)

        # for obj in list_volumes:
        #     most_traded_coins_list.append(obj['coin'])


        # sorted_volumes = {'most_traded_coins': list_volumes}
        # with open("/tracker/json/sorted_instruments.json", "w") as outfile_volume:
        #     outfile_volume.write(json.dumps(sorted_volumes))

        most_traded_coins = {'most_traded_coins': all_usdt_coins, 'most_traded_coins_extra': all_extra_pairs}
        with open("/tracker/json/most_traded_coins.json", "w") as outfile_volume:
            outfile_volume.write(json.dumps(most_traded_coins))

        
        # UPDATE DB BENCHMARK. THE INFO UPDATED IS ABOUT THE FREQUENCY OF THE COIN TRADED.
        # IF A COIN FALLS OUT OF THE BEST POSITIONS, IT WILL GET A NEGATIVE SCORE.

        db_benchmark = client.get_db(DATABASE_BENCHMARK)
        list_coins_benchmark = db_benchmark.list_collection_names()

        # iterate through each pair in the db
        for coin in list_coins_benchmark:
            # update benchmark db --> (field: best trades)
            cursor_benchmark = list(db_benchmark[coin].find())
            id_benchmark = cursor_benchmark[0]['_id']

            # if the coin from db is present in the list updated "most_traded_coins_list", then it will be marked as a live coin
            #if coin in most_traded_coins_list[:NUMBER_COINS_TO_TRADE_WSS]:
            if coin in all_usdt_coins:
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
            # in this case the coin is not between the best coins. it will be marked as non-live coin
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
        

    def main_sort_pairs_list(client=DatabaseConnection(), logger=LoggingController.start_logging(), db_logger=None):
        loop = BinanceController.get_or_create_eventloop()
        
        info = loop.run_until_complete(BinanceController.exchange_info(logger))

        if not info:
            msg = 'VOLUMES UPDATE: Something went wrong with BinanceController.exchange_info'
            logger.error(msg)
            db_logger[DATABASE_API_ERROR].insert({'_id': datetime.now().isoformat(), 'msg': msg})
            info = loop.run_until_complete(BinanceController.exchange_info())

        BinanceController.sort_pairs_by_volume(client=client, res=info, logger=logger, db_logger=db_logger)


    def extract_substring_excluding_usdc(input_string, usd):
        """
        Extracts the substring from a string, excluding "usdc".

        Args:
            input_string: The input string.

        Returns:
            The substring excluding "usdc", or None if "usdc" is not found.
        """
        if usd in input_string:
            parts = input_string.split(usd) #splits the string using usdc as a delimiter
            if len(parts) > 1: # if usdc is found, parts will have more than one element.
                return "".join(parts[:1] + parts[1:]) #rejoins all parts excluding the delimiter.
            else:
                return parts[0] #usdc was at the end of the string
        else:
            return None  # "usdc" not found
        
    def get_common_elements_set(list1, list2):
        """
        Returns a list of common elements using sets.

        Args:
            list1: The first list.
            list2: The second list.

        Returns:
            A list of common elements.
        """
        set1 = set(list1)
        set2 = set(list2)
        common_elements = list(set1.intersection(set2))
        only_list1 = set1.difference(set2)
        only_list2 = set1.difference(set1)
        return common_elements, only_list1, only_list2

    async def binance_exchange_info():
        client = await AsyncClient.create()

        # fetch exchange info
        try:
            res = await client.get_exchange_info()
            await client.close_connection()
            return res
        except:
            sleep(5)
            return False
    
    async def get_exchange_info(logger=None):
        '''
        This function call "ExchangeInfo" Binance API for all the available pairs
        '''

        ### GET EXCHANGE INFO, LOAD IF EXISTS, OR DOWNLOAD FROM BINANCE OTHERWISE, OR LOAD LAST ONE

        # LOAD
        if logger == None:
            logger = LoggingController.start_logging()
        exchange = False
        dir_info = '/tracker/json'
        dir_exchange = f'{dir_info}/exchange'
        if not os.path.exists(dir_exchange):
            os.mkdir(dir_exchange)
        day = (datetime.now() + timedelta(minutes=1)).strftime("%d")
        path_exchange = f'{dir_exchange}/exchange-{day}.json'
        if os.path.exists(path_exchange):
            with open(path_exchange, 'r') as file:
                exchange = json.load(file)
        else:
             
            # DOWNLOAD
            exchange = await BinanceController.binance_exchange_info()

            # GET LAST ONE AVAILABLE
            if not isinstance(exchange, dict):
                logger.info('WARNING: GET EXCHANGE INFO WAS NOT SUCCESFULL')
                exchange_files = [f for f in dir_exchange.glob("exchange*") if f.is_file()]
                latest_path_exchange = max(exchange_files, key=lambda f: f.stat().st_mtime)
                print(F'LAST PATH EXCHANGE: {latest_path_exchange}')
                with open(f'{dir_exchange}/{latest_path_exchange}', 'r') as file:
                    exchange = json.load(file)
            else:
                print('The request to the API "ExchangeInfo" was succesfull')
                with open(path_exchange, 'w') as f: # 'w' opens the file for writing.
                    json.dump(exchange, f, indent=4)
        
        # GET USDT COINS AND USDC COINS
        usdc_coins = []
        usdc_coin_complete = []
        usdt_coins = []
        usdt_coin_complete = []
        for symbol in exchange["symbols"]:
            if "USDC" in symbol["symbol"] and 'USDT' not in symbol:
                usdc_coins.append(BinanceController.extract_substring_excluding_usdc(symbol["symbol"], "USDC"))
                usdc_coin_complete.append(symbol["symbol"])

            elif symbol["symbol"][-4:] == 'USDT' and 'USDC' not in symbol:
                usdt_coins.append(BinanceController.extract_substring_excluding_usdc(symbol["symbol"], "USDT"))
                usdt_coin_complete.append(symbol["symbol"])
        
        common_usd, only_usdc, only_usdt = BinanceController.get_common_elements_set(usdc_coins, usdt_coins)

        # GET USDC THAT ARE ALSO TRADED IN USDT (USDC IS A SUBSET OF USDT)
        list_usdt_common_usdc = [coin + 'USDT' for coin in common_usd]
        list_usdc_common_usdt = []
        for coin in list_usdt_common_usdc:
            coin_w_usdt = BinanceController.extract_substring_excluding_usdc(coin, "USDT")
            if coin_w_usdt + 'USDC' in usdc_coin_complete:
                list_usdc_common_usdt.append(coin_w_usdt + 'USDC')
            elif 'USDC' + coin_w_usdt in usdc_coin_complete:
                list_usdc_common_usdt.append('USDC' + coin_w_usdt)
            
        coins_list = {'usdt_coins': usdt_coin_complete, 'usdc_coins': usdc_coin_complete,
                      'list_usdt_common_usdc': list_usdt_common_usdc, 'list_usdc_common_usdt':list_usdc_common_usdt}

        # GET OFFICIALE PAIR TRADE IN USDT AND USDC, FOR EASY CONVERSION
        convert_usdc_usdt = {}
        for coin in list_usdt_common_usdc + list_usdc_common_usdt:
            if 'USDC' in coin:
                twin_coin = BinanceController.extract_substring_excluding_usdc(coin, "USDC") + 'USDT'
            else:
                coin_w_usdt = BinanceController.extract_substring_excluding_usdc(coin, "USDT")
                if coin_w_usdt + 'USDC' in list_usdc_common_usdt:
                    twin_coin = coin_w_usdt + 'USDC'
                else:
                    twin_coin = 'USDC' + coin_w_usdt
            
            convert_usdc_usdt[coin] = twin_coin

        TradingController.get_minimal_notional_value(logger=logger)

        with open(f'{dir_info}/coins_list.json', 'w') as f: # 'w' opens the file for writing.
            json.dump(coins_list, f, indent=4) 
        with open(f'{dir_info}/convert_usdc_usdt.json', 'w') as f: # 'w' opens the file for writing.
            json.dump(convert_usdc_usdt, f, indent=4)  
              

    def get_or_create_eventloop():
        try:
            return asyncio.get_event_loop()
        except RuntimeError as ex:
            if "There is no current event loop in thread" in str(ex):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                return asyncio.get_event_loop()