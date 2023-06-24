import os,sys
import subprocess
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from database.DatabaseConnection import DatabaseConnection
from constants.constants import *


from app.Controller.LoggingController import LoggingController
from time import sleep

if __name__ == "__main__":
    logger = LoggingController.start_logging()
    logger.info('trading container started')
    db = DatabaseConnection()
    db_trading = db.get_db(DATABASE_TRADING)



    while True:
        # Query every SECONDS_SLEEP if there is a new request for buy_order. The input is sent from the TrackerController through db
        
        SECONDS_SLEEP = 0.5
        sleep(SECONDS_SLEEP)

        docs = list(db_trading[COLLECTION_TRADING_LIVE].find({}))
        for doc in docs:
            if doc['on_trade'] == False:
                id = doc['_id']
                purchase_price = str(doc['purchase_price'])
                coin = doc['coin'].lower()

                #TODO: BUY ORDER
                

                # Start Subprocess for "coin". This will launch a wss connection for getting bid price coin in real time
                process = subprocess.Popen(["python3", "/trading/scripts/wss-trading.py", coin, id, purchase_price])
                
                pid = process.pid
                doc['pid'] = pid
                doc['on_trade'] = True

                db_trading[COLLECTION_TRADING_LIVE].update_one({'_id': id}, {"$set": doc})

