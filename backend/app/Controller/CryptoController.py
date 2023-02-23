from time import time
from random import randint
import json
import os,sys
from constants import *
import requests
import numpy as np
from datetime import datetime
from operator import itemgetter

sys.path.insert(0,'../..')


class CryptoController:

    def getHeader():
        header= {"Content Type": "application/json"}
        return header

    def getBody(**kwargs):
        body = {}
        for key,value in kwargs.items():
            body[key] = value
        body = json.dumps(body)
        return body


    # public
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
    
    def getAllinstruments():
        dict_instruments = CryptoController.getInstruments()
        list_instruments = []
        for instrument in dict_instruments['result']['instruments']:
            list_instruments.append(instrument['instrument_name'])

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
        instrument_name="BTC_USDT"
        timeframe=MINUTE_1
        method="public/get-candlestick"

        url=REST_API_ENDPOINT + method + f"?instrument_name={instrument_name}&timeframe={timeframe}"

        response=requests.get(url=url, headers=header)

        return json.loads(response.text)

    @staticmethod
    def getTicker():
        header=CryptoController.getHeader()

        instrument_name="BTC_USDT"
        method="public/get-ticker"

        url=REST_API_ENDPOINT + method + f"?instrument_name={instrument_name}"

        response=requests.get(url=url, headers=header)

        return json.loads(response.text)

    @staticmethod
    def getTrades(instrument_name="BTC_USDT"):
        header=CryptoController.getHeader()
        
        instrument_name=instrument_name
        method="public/get-trades"

        url=REST_API_ENDPOINT + method + f"?instrument_name={instrument_name}"

        response=requests.get(url=url, headers=header)

        trades=json.loads(response.text)
        resp=[]
        for trade in trades["result"]["data"]:
            resp.append({"order":trade["s"], "quantity": trade["q"], "timestamp": trade["t"], "unix": datetime.fromtimestamp(trade["t"]/1000)})
        
        trades_sorted= sorted(resp, key=itemgetter('timestamp'), reverse=True)
        return trades_sorted

    @staticmethod
    def getTrades_BTC_over_Q():
        limit=0.1
        resp = []
        
        trades = CryptoController.getTrades(instrument_name="BTC_USDT")
        for trade in trades["result"]["data"]:
            if float(trade['q']) > limit:
                resp.append({"order":trade["s"], "quantity": trade["q"], "timestamp": datetime.fromtimestamp(trade["t"]/1000)})
        
        #return resp
        # SORT "RESP" BY QUANTITY "Q"
        trades_sorted= sorted(resp, key=itemgetter('quantity'), reverse=True)
        return trades_sorted

    

    # private
    def setCancelOnDIsconnect():
        pass

    def getCancelOnDisconnect():
        pass

