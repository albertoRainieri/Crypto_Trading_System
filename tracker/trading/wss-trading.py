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
    msg = str(error)
    if msg != "\'data\'":
        db_logger[DATABASE_TRADING_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
        logger.info(error)


def on_close(*args):
    logger.info(f'closing connection {coin}:{id}. Now Restarting...')
    riskmanagement = RiskManagement(id, coin, purchase_price, timeframe, 
                                    riskmanagement_configuration,
                                    db_trading, logger)

    launch_wss_session(coin)


def test_close_connection():
    '''
    compute total seconds until next wss restart.
    This function restarts the wss connection every 24 hours
    '''

    sleep(300)
    terminate_process()

def terminate_all_trading_process():
    pass



def on_message(ws, message):

    global riskmanagement

    #sleep(0.5)
    #logger.info(message)
    
    data = json.loads(message)
    current_bid_price = float(data['data']['b'])
    now = datetime.now()
    
    # RISK MANAGEMENT
    riskmanagement.updateProfit(current_bid_price, now)

    if riskmanagement.SELL:
        #TODO: send order close to Binance

        # terminate process
        riskmanagement.saveToDb(current_bid_price, now)
        terminate_process()

    # every minute I update the record in the db
    riskmanagement.saveToDb(current_bid_price, now)




def terminate_process():
    # Terminate the process
    docs = list(db_trading[COLLECTION_TRADING_LIVE].find({}))

    for doc in docs:
        if doc['_id'] == id:
            db_trading[COLLECTION_TRADING_LIVE].delete_one({'_id': id})


            # Specify the filter for the document to update
            filter = {'_id': id}
            # Specify the update operation. update current_bid_price and profit
            update = {'$set': {'on_trade': False}}
            # Perform the update operation
            db_trading[COLLECTION_TRADING_HISTORY].update_one(filter, update)

    logger.info(f'record for {coin}:{id} has been deleted in Live Trading')
    
    pid = os.getpid()
    logger.info(f'Terminating process {pid}')
    os.kill(pid, SIGKILL)

def get_db(db_name):
    '''
    Establish connectivity with db
    '''
    database = DatabaseConnection()
    db = database.get_db(db_name)
    return db

def define_on_open(coin):
    def on_open(ws, coin=coin):

        # msg = f'on_open: Wss Started for trading'
        # logger.info(msg)

        coin_lower = coin.lower()
        parameters = [coin_lower + '@bookTicker']
        #print("### Opened ###")
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": parameters,
            "id": 1
        }

        #logger.info(subscribe_message)
        ws.send(json.dumps(subscribe_message))

    return on_open

def launch_wss_session(coin):

    #logger.info('hello!')
    on_open = define_on_open(coin)
    ws = websocket.WebSocketApp("wss://stream.binance.com:9443/stream?streams=",
                                on_open = on_open,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close
                                )
    # logger.info('you there?')
    # logger.info(ws)
    #threading.Thread(target=test_close_connection).start()
    ws.run_forever()


if __name__ == "__main__":
    global riskmanagement

    db_logger = get_db(DATABASE_LOGGING)
    db_trading = get_db(DATABASE_TRADING)
    logger = LoggingController.start_logging()
    #logger.info('Started')
    
    coin = sys.argv[1]
    id = sys.argv[2]
    purchase_price = float(sys.argv[3])
    timeframe = int(sys.argv[4])
    riskmanagement_configuration = json.loads(sys.argv[5])

    riskmanagement = RiskManagement(id, coin, purchase_price, timeframe, 
                                    riskmanagement_configuration,
                                    db_trading, logger)
    
    # logger.info(riskmanagement.GOLDEN_ZONE)
    # logger.info(type(riskmanagement.GOLDEN_ZONE))

    launch_wss_session(coin)


