
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from time import sleep
from app.Controller.LoggingController import LoggingController
from database.DatabaseConnection import DatabaseConnection
from app.Controller.CryptoController import CryptoController
from datetime import datetime


def main(db, logger):
    crypto = CryptoController()
    while True:
        now=datetime.now()
        minute = now.minute
        second = now.second
        hour = now.hour

        if minute == 0 and second == 0 and hour == 0:
            crypto.getMostTradedCoins()
            logger.info("list of instruments updated")
        pass

        
        sleep(0.8)

    

    

if __name__ == '__main__':
    db = DatabaseConnection()
    logger = LoggingController.start_logging()
    db = db.get_db('Market_Trades')
    sleep(2)
    main(db=db, logger=logger)