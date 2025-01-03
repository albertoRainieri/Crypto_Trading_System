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
from app.Controller.TradingController import TradingController
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
                                    riskmanagement_configuration, logger)

    launch_wss_session(coin)


def test_close_connection():
    '''
    compute total seconds until next wss restart.
    This function restarts the wss connection every 24 hours
    '''
    sleep(60)
    current_bid_price = purchase_price
    terminate_process(current_bid_price, purchase_price, client)

def terminate_all_trading_process():
    pass



def on_message(ws, message):

    global riskmanagement

    #sleep(0.5)
    #logger.info(message)
    
    data = json.loads(message)
    current_bid_price = float(data['data']['b'])
    now = datetime.now()

    # for attribute, value in vars(riskmanagement).items():
    #     logger.info(f"Attribute: {attribute}, Value: {value}")
    
    # RISK MANAGEMENT
    riskmanagement.updateProfit(current_bid_price, now)

    if riskmanagement.SELL:
        #TODO: send order close to Binance

        # terminate process
        terminate_process(current_bid_price, purchase_price, client)

    # every minute I update the record in the db
    riskmanagement.saveToDb(current_bid_price, client)




def terminate_process(current_bid_price, purchase_price, client):

    f = open ('/tracker/user_configuration/userconfiguration.json', "r")
    user_configuration = json.loads(f.read())

    for user in user_configuration:
        db_name = DATABASE_TRADING + '_' + user
        db_trading = client.get_db(db_name)

        now = datetime.now()
        # Terminate the process
        docs = list(db_trading[COLLECTION_TRADING_LIVE].find({}))
        # Specify the query for the document to update
        query = {'_id': id}
        profit = (current_bid_price - purchase_price) / purchase_price
        # Specify the update operation. 
        update = {'$set': {'on_trade': False,
                            'exit_timestamp': now.isoformat(),
                            'current_price': current_bid_price,
                            'profit': profit}
                }

        for doc in docs:
            if doc['_id'] == id:
                # SEND SELL ORDER IF TRADING_LIVE
                trading_live = doc['trading_live']
                if trading_live:
                    quantity = doc['quantity'] 
                    response, status_code = TradingController.create_order(coin=coin, side="SELL", quantity=quantity)
                    # if REQUEST is succesful, then update db
                    if status_code == 200:
                        # get info
                        event_key = doc['event']

                        response = response.json()
                        quantity_executed = float(response["executedQty"])
                        sell_price = float(response["price"])

                        # update database trading_history, and delete record for trading_live
                        update['$set']['current_price'] = sell_price
                        update['$set']['quantity_sell'] = quantity_executed
                        update['$set']['quantity_sell'] = (sell_price - purchase_price) / purchase_price

                        db_trading[COLLECTION_TRADING_HISTORY].update_one(query, update)
                        db_trading[COLLECTION_TRADING_LIVE].delete_one({'_id': id})

                        # notify/update dbs
                        msg = f'SELL Order Succeded for {coin}:{id}. origQty: {quantity}, execQty: {quantity_executed}. user: {user}'
                        logger.info(msg)
                        db_logger[DATABASE_TRADING_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})

                        # kill the process
                        pid = os.getpid()
                        logger.info(f'Terminating process {pid}')
                        os.kill(pid, SIGKILL)

                    # if REQUEST is not succesfull, then notify to db_logger and do not kill the process
                    else:
                        msg_text = f'SELL Order FAILED for {coin}:{id} for {user}'
                        msg = {'msg': msg_text, 'error': response.text}
                        logger.info(msg)
                        db_logger[DATABASE_TRADING_INFO].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
                        db_logger[DATABASE_TRADING_ERROR].insert_one({'_id': datetime.now().isoformat(), 'msg': msg})
                else:
                    # update database trading_history, and delete record for trading_live
                    db_trading[COLLECTION_TRADING_HISTORY].update_one(query, update)
                    db_trading[COLLECTION_TRADING_LIVE].delete_one({'_id': id})
                    
                    
                    logger.info(f'Record for {coin}:{id} has been deleted. Trading_live: {trading_live} for user {user}')
        
                    pid = os.getpid()
                    logger.info(f'Terminating process {pid}')

    client.close()
    os.kill(pid, SIGKILL)


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
    # threading.Thread(target=test_close_connection).start()
    ws.run_forever()


if __name__ == "__main__":
    global riskmanagement

    logger = LoggingController.start_logging()
    #logger.info('Started')
    
    coin = sys.argv[1]
    id = sys.argv[2]
    purchase_price = float(sys.argv[3])
    timeframe = int(sys.argv[4])
    riskmanagement_configuration = json.loads(sys.argv[5])

    riskmanagement = RiskManagement(id, coin, purchase_price, timeframe, 
                                    riskmanagement_configuration, logger)
    
    client = DatabaseConnection()
    db_logger = client.get_db(DATABASE_LOGGING)
    #close_timewindow = riskmanagement.close_timewindow.isoformat()
    #logger.info(f'Trade {coin} started at {id} will be running no later then {close_timewindow}')
    
    # logger.info(riskmanagement.GOLDEN_ZONE)
    # logger.info(type(riskmanagement.GOLDEN_ZONE))

    launch_wss_session(coin)


