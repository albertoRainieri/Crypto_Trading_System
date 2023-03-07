from time import time
from random import randint
import json
import os,sys
from constants import *
import requests
import numpy as np
from datetime import datetime, timedelta
from operator import itemgetter
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from app.Helpers import round_, timer_func
import numpy as np

sys.path.insert(0,'../..')


class CryptoController:

    def __init__(self) -> None:
        self.db = DatabaseConnection()
        self.db_btc_trades = os.getenv("BTC_TRADES")

    def getHeader():
        header= {"Content Type": "application/json"}
        return header

    def getBody(**kwargs):
        body = {}
        for key,value in kwargs.items():
            body[key] = value
        body = json.dumps(body)
        return body
    
    def get_db(self, db_name):
        '''
        Establish connectivity with db
        '''
        # Retrieve Database DATABASE_DATA_STORAGE
        database = DatabaseConnection()
        db = database.get_db(db_name)
        return db
    

    def getMostTradedCoins(self):
        '''
        This function outputs the list of the pairs that will be traded
        The list does not cointain redundant pairs (e.g. BTC_USD and BTC_USDT, but only one (the most traded))
        '''

        # JSON file
        # get all the pairs traded
        # f = open ('/backend/json/coin-list.json', "r")
        # data = json.loads(f.read())

        list_instruments = CryptoController.getAllinstruments()

        data = {'coin_list': list_instruments}
        #subset = {'coin_list': data['coin_list'][:10]}

        # prepare variables
        all_tickets = []
        all_info_coins_daily = {}
        most_traded_coin_list = {'most_traded_coin_list': []}
        coins_traded = []
        pairs_traded = []
        pairs_traded_nrd = [] #pairs traded non-redundant

        # get info on tickers from Crypto.COM API
        for coin in data['coin_list']:
            coin_ticker = CryptoController.getTicker(coin)
            doc_= coin_ticker['result']['data'][0]
            doc_['vv'] = float(doc_['vv'])
            all_tickets.append(doc_)
            
        # SORT pairs (list and info) BY VOLUME
        sorted_tickets = sorted(all_tickets, key=itemgetter('vv'), reverse=True)
        for coin in sorted_tickets:
            all_info_coins_daily[coin['i']] = coin
            pairs_traded.append(coin['i'])

        # FROM HERE I get subset of non-redundant pairs traded
        # get list of non-redundant pairs
        for coin in pairs_traded:
            coin_string = coin.split('_')
            coin_0 = coin_string[0]
            coin_1 = coin_string[1]

            if coin_0 not in coins_traded and coin_1 != 'BTC' and coin_0 != 'USDT':
                coins_traded.append(coin_0)
                pairs_traded_nrd.append(coin)
                most_traded_coin_list['most_traded_coin_list'].append(coin)
        
        all_info_coins_daily_new = {}

        # with the "pairs_traded" (NON-redundant) I get a subset of "all_info_coins_daily"
        for coin_info in list(all_info_coins_daily.keys()):
            if all_info_coins_daily[coin_info]["i"] in pairs_traded_nrd:

                all_info_coins_daily_new[all_info_coins_daily[coin_info]['i']] = all_info_coins_daily[coin_info]

        
        all_info_coins_daily_new_json = json.dumps(all_info_coins_daily_new)
        most_traded_coin_list_json = json.dumps(most_traded_coin_list)


        with open('/backend/json/all_info_coins_daily.json', 'w') as outfile:
            outfile.write(all_info_coins_daily_new_json)

        with open('/backend/json/most_traded_coin_list.json', 'w') as outfile:
            outfile.write(most_traded_coin_list_json)

        
        return all_info_coins_daily_new, len(all_info_coins_daily_new)
    

    @staticmethod
    def authenticate():
        pass
    
    @staticmethod
    def getInstruments():
        method="public/get-instruments"
        nonce=int(time()*1000)
        id=randint(0,10**9)

        body=CryptoController.getBody(method=method, nonce=nonce, id=id)
        header=CryptoController.getHeader()
        print(body)

        response=requests.get(url=REST_API_ENDPOINT + method, data=body, headers=header)
        return json.loads(response.text)
    
    @staticmethod
    def getAllinstruments():
        dict_instruments = CryptoController.getInstruments()
        list_instruments = []
        for instrument in dict_instruments['result']['instruments']:
            list_instruments.append(instrument['instrument_name'])

        dict_ = {'coin_list': list_instruments}
        json_string = json.dumps(dict_)

        with open('/backend/json/coin-list.json', 'w') as outfile:
            outfile.write(json_string)

        #np.savetxt("instruments.py", list_instruments)
        return list_instruments
            
    @staticmethod
    def getBook():
        header=CryptoController.getHeader()
        instrument_name="BTC_USDT"
        depth="10"
        method="public/get-book"

        url=REST_API_ENDPOINT + method + f"?instrument_name={instrument_name}&depth={depth}"
        #body=CryptoController.getBody(instrument_name=instrument_name, depth=depth)

        response=requests.get(url=url, headers=header)

        return json.loads(response.text)

    
    @staticmethod
    def getCandlestick():
        header=CryptoController.getHeader()
        instrument_name="ETH_USD"
        timeframe=DAY_1
        method="public/get-candlestick"

        url=REST_API_ENDPOINT + method + f"?instrument_name={instrument_name}&timeframe={timeframe}"

        response=requests.get(url=url, headers=header)

        return json.loads(response.text)

    
    @staticmethod
    @timer_func
    def getTicker(instrument_name='BTC_USD'):
        header=CryptoController.getHeader()

        #instrument_name="BTC_USDT"
        method="public/get-ticker"

        url=REST_API_ENDPOINT + method + f"?instrument_name={instrument_name}"

        response=requests.get(url=url, headers=header)

        return json.loads(response.text)
    
    @staticmethod
    @timer_func
    def getTrades(instrument_name):
        header=CryptoController.getHeader()
        method="public/get-trades"
        url=REST_API_ENDPOINT + method + f"?instrument_name={instrument_name}"
        response=requests.get(url=url, headers=header)
        trades=json.loads(response.text)
        return trades


    @staticmethod
    def getStatistics_onTrades(coin_list=["BTC_USD"], database=None, logger=None, start_minute=datetime.now().minute):
        '''
        This API returns the last 150 transactions for a particular instrument
        '''
        resp = {}
        doc_db = {}
        prices = {}
        trades_sorted = {}
        n_trades = {}
        n_trades_p_s_dict = {}

        # JSON file
        # Overwrite coin_list variable
        f = open ('/backend/json/most_traded_coin_list.json', "r")
        data = json.loads(f.read())
        coin_list = data["most_traded_coin_list"][:5]

        for instrument_name in coin_list:
            resp[instrument_name] = []
            doc_db[instrument_name] = {"_id": None, "price_average": None, "n_trades": 0,"volume": 0 , "buy_volume": 0, "sell_volume": 0, "buy_n": 0, "sell_n": 0, "quantity": 0, "n_trades_p_s": []}
            prices[instrument_name] = []
            trades_sorted[instrument_name] = []
            n_trades[instrument_name] = 0
            n_trades_p_s_dict[instrument_name] = []
         
        while start_minute == datetime.now().minute:


            for instrument_name in coin_list:
                # make the request to Crypto.Com API
                trades = CryptoController.getTrades(instrument_name)

                if len(trades["result"]["data"]) == 0:
                    pass 

                # use LAST_TRADE_TIMESTAMP to fetch only the new trades
                LAST_TRADE_TIMESTAMP = os.getenv(f"LAST_TRADE_TIMESTAMP_{instrument_name}")

                if LAST_TRADE_TIMESTAMP == None:
                    LAST_TRADE_TIMESTAMP = (datetime.now() - timedelta(seconds=5)).isoformat()
                
                
                # if db is None
                if database == None:
                    print("ok")
                    for trade in trades["result"]["data"]:
                        datetime_trade = datetime.fromtimestamp(trade["t"]/1000)
                        if datetime_trade > datetime.fromisoformat(LAST_TRADE_TIMESTAMP):
                            doc = {"order":trade["s"], "price": trade["p"], "quantity": trade["q"], "timestamp": datetime_trade, "trade_id": trade["d"]}
                            resp[instrument_name].append(doc)
                # if db in not None. save the data
                else:
                    # number of seconds between each trade
                    n_trades_p_s = 0

                    #print("N_SEC_P_TRADE: ", datetime.now() - datetime.fromisoformat(LAST_TRADE_TIMESTAMP))
                    
                    timestamps=[]
                    current_n_trades = 0
                    for trade in trades["result"]["data"]:
                        datetime_trade = datetime.fromtimestamp(trade["t"]/1000)
                        if datetime_trade > datetime.fromisoformat(LAST_TRADE_TIMESTAMP):
                            timestamps.append(datetime_trade)
                            n_trades[instrument_name] += 1
                            current_n_trades += 1
                            
                            doc = {"order":trade["s"], "price": trade["p"], "quantity": trade["q"], "timestamp": datetime_trade, "trade_id": trade["d"], }
                            resp[instrument_name].append(doc)

                            # ANALYZE STATISTICS TO SAVE TO DB

                            prices[instrument_name].append(float(doc["price"]))

                            doc_db[instrument_name]["quantity"] += float(doc["quantity"])
                            
                            if doc["order"] == "BUY":
                                doc_db[instrument_name]["buy_n"] += 1
                                doc_db[instrument_name]["buy_volume"] += float(doc["quantity"]) * float(doc["price"])
                            else:
                                doc_db[instrument_name]["sell_n"] += 1
                                doc_db[instrument_name]["sell_volume"] += float(doc["quantity"]) * float(doc["price"])

                    if current_n_trades != 0:
                        doc_db[instrument_name]["n_trades"] += current_n_trades
                        n_trades_p_s_datetime = datetime.now() - datetime.fromisoformat(LAST_TRADE_TIMESTAMP)
                        n_trades_p_s = n_trades_p_s_datetime.seconds + n_trades_p_s_datetime.microseconds*10**(-6)
                        n_trades_p_s = current_n_trades / n_trades_p_s
                        
                        resp[instrument_name] = sorted(resp[instrument_name], key=itemgetter('timestamp'), reverse=False)
                        os.environ[f"LAST_TRADE_TIMESTAMP_{instrument_name}"] = resp[instrument_name][-1]['timestamp'].isoformat()
                        n_trades_p_s_dict[instrument_name].append(n_trades_p_s)
                                        
                if logger != None:
                    logger.info(f'{instrument_name}: {doc_db[instrument_name]}')


        for instrument_name in prices:
            price_average = np.mean(prices[instrument_name])
            doc_db[instrument_name]["n_trades_p_s"]= round_(np.mean(n_trades_p_s_dict[instrument_name]),2)
            doc_db[instrument_name]["price_average"]=round_(price_average,4)
            doc_db[instrument_name]["quantity"] = round_(doc_db[instrument_name]["quantity"],2)
            doc_db[instrument_name]["volume"] =  int(doc_db[instrument_name]["buy_volume"] + doc_db[instrument_name]["sell_volume"])
            doc_db[instrument_name]["sell_volume"] = int(doc_db[instrument_name]["sell_volume"])
            doc_db[instrument_name]["buy_volume"] = int(doc_db[instrument_name]["buy_volume"])
            doc_db[instrument_name]['_id']= datetime.now().isoformat()
            database[COLLECTION_ALL_TRADES][instrument_name].insert(doc_db[instrument_name])
            trades_sorted[instrument_name]= sorted(resp[instrument_name], key=itemgetter('timestamp'), reverse=False)
            
                
        #logger.info(f'Final log - Trades Sorted: {trades_sorted}')        
        #if len(trades_sorted) != 0:
        #    logger.info(f'{instrument_name}: {timestamps}')
        
        return trades_sorted
                
    
    def getTrades_BTC_over_Q(self, coin_list=["BTC_USD"], logger=LoggingController.start_logging()):

        limit=0.5
        resp = {}
        current_minute = datetime.now().minute
        
        db = self.get_db('Market_Trades')
        trades = CryptoController.getStatistics_onTrades(coin_list=coin_list, database=db, logger=logger, start_minute=current_minute)

        for instrument_name in coin_list:

            if len(trades) == 0:
                return

            for instrument_name in trades:
                for trade in trades[instrument_name]:
                    if float(trade['quantity']) > limit:
                        doc = {"order":trade["order"], "price": trade["price"], "quantity": trade["quantity"],
                                "timestamp": trade["timestamp"].isoformat(), "trade_id": trade["trade_id"]}
                        
                        resp[instrument_name] = doc
                        db[COLLECTION_TRADES_OVER_Q][instrument_name].insert(doc)
                
        return resp

    
    # private
    def setCancelOnDIsconnect():
        pass

    def getCancelOnDisconnect():
        pass

