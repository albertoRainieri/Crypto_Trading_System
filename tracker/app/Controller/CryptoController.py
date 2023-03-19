import os,sys
sys.path.insert(0,'../..')


import json
from constants.constants import *
from operator import itemgetter
from database.DatabaseConnection import DatabaseConnection
from time import time, sleep
from random import randint
import requests
from tracker.app.Helpers.Helpers import round_, timer_func



class CryptoController:

    def __init__(self) -> None:
        self.db = DatabaseConnection()

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


        with open('/tracker/json/all_info_coins_daily.json', 'w') as outfile:
            outfile.write(all_info_coins_daily_new_json)

        with open('/tracker/json/most_traded_coin_list.json', 'w') as outfile:
            outfile.write(most_traded_coin_list_json)

        
        return all_info_coins_daily_new, len(all_info_coins_daily_new)
    
    
    @staticmethod
    def getAllinstruments():
        dict_instruments = CryptoController.getInstruments()
        list_instruments = []
        for instrument in dict_instruments['result']['instruments']:
            list_instruments.append(instrument['instrument_name'])

        dict_ = {'coin_list': list_instruments}
        json_string = json.dumps(dict_)

        with open('/tracker/json/coin-list.json', 'w') as outfile:
            outfile.write(json_string)

        #np.savetxt("instruments.py", list_instruments)
        return list_instruments
    

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
    @timer_func
    def getTicker(instrument_name='BTC_USD'):
        header=CryptoController.getHeader()

        #instrument_name="BTC_USDT"
        method="public/get-ticker"
        url=REST_API_ENDPOINT + method + f"?instrument_name={instrument_name}"
        response=requests.get(url=url, headers=header)
        return json.loads(response.text)

