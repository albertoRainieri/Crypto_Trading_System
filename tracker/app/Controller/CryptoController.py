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
        self.production = bool(int(os.getenv('PRODUCTION')))

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

        db_benchmark = self.get_db(DATABASE_BENCHMARK)
        list_coins_benchmark = db_benchmark.list_collection_names()
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
        i = 0
        len_coin_list = len(data['coin_list'])
        for coin in data['coin_list']:
            i += 1
            if not self.production:
                print(str(i)+ '/' + str(len_coin_list))
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
        

        for coin in list_coins_benchmark:
            # update benchmark db --> (field: best trades)
            cursor_benchmark = list(db_benchmark[coin].find())
            id_benchmark = cursor_benchmark[0]['_id']

            if coin in pairs_traded_nrd[:NUMBER_COINS_TO_TRADE*SLICES+COINS_PRIORITY]:

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
        #print(body)

        response=requests.get(url=REST_API_ENDPOINT + method, data=body, headers=header)
        return json.loads(response.text)
    
    @staticmethod
    def getTicker(instrument_name='BTC_USD'):
        header=CryptoController.getHeader()

        #instrument_name="BTC_USDT"
        method="public/get-ticker"
        url=REST_API_ENDPOINT + method + f"?instrument_name={instrument_name}"
        response=requests.get(url=url, headers=header)
        return json.loads(response.text)

