import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from time import sleep
from app.Controller.LoggingController import LoggingController
from database.DatabaseConnection import DatabaseConnection
from app.Controller.CryptoController import CryptoController
from datetime import datetime

logger = LoggingController.start_logging()

def main(db, crypto_instance, coin_list, logger):
  while True:
    #each minute
    
    crypto_instance.getTrades_BTC_over_Q(coin_list=coin_list, logger=logger)

    


if __name__ == '__main__':
    sleep(1)

    logger.info('Python Script Started')
    db = DatabaseConnection()
    crypto = CryptoController()
    coin_list = ["BTC_USD", "ETH_USD"]
    
    main(db=db, crypto_instance=crypto, coin_list=coin_list, logger=logger)