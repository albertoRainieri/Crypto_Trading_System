import os,sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))

import websocket
import json
from datetime import datetime, timedelta
import os
from time import sleep
import logging
import threading
import sys
from LoggingController import LoggingController


binance_TIME = "T"
binance_PRICE = "p"
binance_QUANTITY = "q"
binance_ORDER = "m"

crypto_TIME = "t"
crypto_PRICE = "p"
crypto_QUANTITY = "q"
crypto_ORDER = "s"





def on_error(ws, error):
    error = str(error)    

    
    logger.info(error)



def on_close(*args):

    threading.Thread(target=restart_connection).start()
    binance_ws.run_forever()
    crypto_com_ws.run_forever()

def restart_connection(ws_app):
    '''
    compute total seconds until next wss restart.
    This function restarts the wss connection every 24 hours
    '''

    while True:
        if not ws_app.sock or not ws_app.sock.connected:
            ws_app.run_forever()
        time.sleep(1)  # Add a small delay to avoid tight loop

    # now = datetime.now()
    # current_hour = now.hour
    # current_minute = now.minute

    # # compute how many seconds until next restart
    # HOURS = 24 - current_hour
    # remaining_seconds = 60 - now.second + 1
    # minutes_remaining = 59 - current_minute
    # hours_remaining = HOURS - 1

    # # total seconds until next wss restart
    # total_remaining_seconds = remaining_seconds + minutes_remaining*60 + hours_remaining*60*60

    # #ONLY FOR TESTING
    # #total_remaining_seconds = 240 + remaining_seconds
    
    # # define timestamp for next wss restart
    # wss_restart_timestamp = (now + timedelta(seconds=total_remaining_seconds)).isoformat()
    # logger.info(f'on_restart_connection: Next wss restart {wss_restart_timestamp}')
    
    
    # sleep(total_remaining_seconds)
    # #sleep(70)
    # binance_ws.close()
    

def on_open(ws):

    global crypto_ask_price
    global crypto_bid_price
    global binance_ask_price
    global binance_bid_price

    crypto_ask_price = None
    crypto_bid_price = None
    binance_ask_price = None
    binance_bid_price = None

    msg = f'Binance Wss Started'
    logger.info(msg)
    binance_parameters = ['ethusdt@depth10@100ms']
    crypto_com_parameters = {
    "channels": ['book.ETH_USDT']
  }

    print("### Opened ###")
    binance_subscribe_message = {
        "method": "SUBSCRIBE",
        "params": binance_parameters,
        "id": 1
    }
    crypto_com_subscribe_message = {
        "method": "subscribe",
        "params": crypto_com_parameters,
        "id": 1
    }
    binance_ws.send(json.dumps(binance_subscribe_message))
    crypto_com_ws.send(json.dumps(crypto_com_subscribe_message))

def on_message(ws, message):

    global crypto_ask_price
    global crypto_bid_price
    global binance_ask_price
    global binance_bid_price
    global binance_url
    global crypto_com_url

    data = json.loads(message)

    if ws.url == crypto_com_url:
        crypto_data = data.copy()
        #logger.info(crypto_data)
        if 'result' in crypto_data:
            crypto_ask_price = float(crypto_data['result']['data'][0]['asks'][0][0])
            crypto_bid_price = float(crypto_data['result']['data'][0]['bids'][0][0])
        else:
            logger.info(crypto_data)
            pass
        
    else:
        binance_data = data.copy()
        #logger.info(binance_data['data'])
        if 'data' in binance_data:
            binance_ask_price = float(binance_data['data']['asks'][0][0])
            binance_bid_price = float(binance_data['data']['bids'][0][0])
        else:
            #logger.info(binance_data)
            pass

    
    investment = 100
    if binance_bid_price and crypto_ask_price and crypto_bid_price and binance_ask_price:
        if binance_bid_price - crypto_ask_price > crypto_bid_price - binance_ask_price:
            best_strategy = 'BUY_BINANCE'
            profit = ((binance_bid_price - crypto_ask_price)/binance_bid_price)*100 - ((0.0075*100)*4)
            logger.info(f'{best_strategy}: profit = {profit} --  BUY binance: {binance_bid_price} SELL crypto: {crypto_ask_price}')
        elif  binance_bid_price - crypto_ask_price < crypto_bid_price - binance_ask_price:
            best_strategy = 'BUY_CRYPTO'
            profit = ((crypto_bid_price - binance_ask_price)/crypto_bid_price)*100 - ((0.0075*100)*4)
            logger.info(f'{best_strategy}: profit = {profit} --  BUY crypto: {crypto_bid_price} SELL binance: {binance_ask_price}' )
    else:
        logger.info(f'binance_bid_price: {binance_bid_price}')
        logger.info(f'crypto_ask_price: {crypto_ask_price}')
        logger.info(f'crypto_bid_price: {crypto_bid_price}')
        logger.info(f'binance_ask_price: {binance_ask_price}')





    #get data and symbol
    
    # data = data['data']
    # instrument_name = data['s']
    # quantity  = data[QUANTITY]
    # price =  data[PRICE]
    # now = datetime.now()
    # msg = f'BINANCE: btcusdt: {price}'
    


def get_db(db_name):
    '''
    Establish connectivity with db
    '''
    database = DatabaseConnection()
    db = database.get_db(db_name)
    return db

if __name__ == "__main__":
    global binance_url
    global crypto_com_url

    logger = LoggingController.start_logging()

    binance_url = "wss://stream.binance.com:9443/stream?streams="
    crypto_com_url = "wss://stream.crypto.com/v2/market"  # Replace with the actual Crypto.com WebSocket URL
    # binance_queue = queue.Queue()
    # crypto_com_queue = queue.Queue()

    binance_ws = websocket.WebSocketApp(binance_url,
                                on_open = on_open,
                                on_message = on_message,
                                on_error = on_error,
                                on_close = on_close
                                )
    crypto_com_ws = websocket.WebSocketApp(
                                    crypto_com_url,  # Replace with the actual Crypto.com WebSocket URL
                                    on_open=on_open,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close
                                )
    # Create and start threads for each WebSocket connection
    binance_thread = threading.Thread(target=restart_connection, args=(binance_ws,))
    crypto_com_thread = threading.Thread(target=restart_connection, args=(crypto_com_ws,))

    binance_thread.start()
    crypto_com_thread.start()

    # Create and start threads to process messages from queues
    # binance_processor_thread = threading.Thread(target=process_queue, args=(binance_queue, "Binance"))
    # crypto_com_processor_thread = threading.Thread(target=process_queue, args=(crypto_com_queue, "Crypto.com"))

    # binance_processor_thread.start()
    # crypto_com_processor_thread.start()

    # Optional: Join threads if you want the main program to wait for threads to complete
    binance_thread.join()
    crypto_com_thread.join()
    # binance_processor_thread.join()
    # crypto_com_processor_thread.join()




