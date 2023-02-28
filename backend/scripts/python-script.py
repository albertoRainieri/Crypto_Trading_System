import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from time import sleep
from app.Controller.LoggingController import LoggingController
from database.DatabaseConnection import DatabaseConnection
from app.Controller.CryptoController import CryptoController
import requests

logger = LoggingController.start_logging()

def main(db, crypto_instance):
  while True:
      crypto_instance.getTrades_BTC_over_Q()

      


if __name__ == '__main__':
    sleep(1)

    logger.info('Python Script Started')
    db = DatabaseConnection()
    crypto = CryptoController()
    
    main(db=db, crypto_instance=crypto)