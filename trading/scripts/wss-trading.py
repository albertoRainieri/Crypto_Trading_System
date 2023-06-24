import os,sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import websocket
import json
from datetime import datetime, timedelta
import os
from time import sleep
from constants.constants import *
from app.Helpers.Helpers import round_, timer_func
from app.Controller.RiskManagement import RiskManagement
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from signal import SIGKILL
import logging
import threading
import sys


def on_error(ws, error):
    pass

def on_close(*args):
    pass


def test_close_connection():
    '''
    compute total seconds until next wss restart.
    This function restarts the wss connection every 24 hours
    '''

    sleep(240)
    terminate_process()



def on_message(ws, message):
    global prices
    global doc_db
    global coin_list
    global n_list_coins
    #sleep(0.5)
    
    data = json.loads(message)
    current_bid_price = float(data['data']['b'])
    
    # RISK MANAGEMENT
    
    riskmanagement.updateProfit(current_bid_price)
    if riskmanagement.isCloseOrder():
        #TODO: send order close to Binance


        # terminate process
        terminate_process()

    # every minute I update the record in the db
    riskmanagement.saveToDb(current_bid_price)




def terminate_process():
    # Terminate the process
    docs = list(db_trading[COLLECTION_TRADING_LIVE].find({}))

    for doc in docs:
        if doc['_id'] == id:
            db_trading[COLLECTION_TRADING_LIVE].delete_one({'_id': id})

    logger.info(f'record for {coin}:{id} has been deleted in Live Trading')
    
    pid = os.getpid()
    logger.info(f'Terminating process {pid}')
    os.kill(pid, SIGKILL)

def initializeVariables(coin_list):
    pass

def get_db(db_name):
    '''
    Establish connectivity with db
    '''
    database = DatabaseConnection()
    db = database.get_db(db_name)
    return db

def define_on_open(coin):
    def on_open(ws, coin=coin):

        msg = f'on_open: Wss Started for trading'
        logger.info(msg)

        parameters = [coin + '@bookTicker']
        print("### Opened ###")
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": parameters,
            "id": 1
        }
        ws.send(json.dumps(subscribe_message))

    return on_open

def launch_wss_session(coin):

    on_open = define_on_open(coin)
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/stream?streams=",
                                on_open = on_open,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close
                                )
    threading.Thread(target=test_close_connection).start()
    ws.run_forever()


if __name__ == "__main__":
    db_logger = get_db(DATABASE_LOGGING)
    db_trading = get_db(DATABASE_TRADING)
    db_benchmark = get_db(DATABASE_BENCHMARK)
    logger = LoggingController.start_logging()
    

    coin = sys.argv[1]
    id = sys.argv[2]
    purchase_price = float(sys.argv[3])

    strategy = 'trading_steps'

    logger.info(f'Start Trading on {coin}')
    riskmanagement = RiskManagement(id, coin, purchase_price, strategy,
                                     db_trading, db_benchmark, logger)

    launch_wss_session(coin)


