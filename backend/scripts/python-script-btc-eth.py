import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from time import sleep
from app.Controller.LoggingController import LoggingController
from database.DatabaseConnection import DatabaseConnection
from app.Controller.CryptoController import CryptoController
from app.Controller.BinanceController import BinanceController
from datetime import datetime
from constants.constants import *

logger = LoggingController.start_logging()

def main(db, crypto_instance, coin_list, logger):

  
  while True:
    crypto_instance.start_live_trades(coin_list=coin_list, logger=logger, sleep_seconds=SLEEP_SECONDS)
    


if __name__ == '__main__':
    sleep(1)

    logger.info('Python Script Started')
    db = DatabaseConnection()
    #crypto = CryptoController()
    binance = BinanceController()

    # these are the coins that will be traded in this process
    coin_list = ["BTCUSDT", "ETHUSDT"]
    
    main(db=db, crypto_instance=binance, coin_list=coin_list, logger=logger)