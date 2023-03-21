
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from time import sleep
from app.Controller.LoggingController import LoggingController
from database.DatabaseConnection import DatabaseConnection
from app.Controller.CryptoController import CryptoController
from app.Controller.BenchmarkController import Benchmark
from datetime import datetime


def main(db, logger):
    crypto = CryptoController()
    while True:
        now=datetime.now()
        minute = now.minute
        second = now.second
        hour = now.hour

        if minute == 0 and second == 5 and hour == 0:
            Benchmark.computeVolumeAverage(db=db)
            logger.info("volumes have been updated")

        
        if minute == 15 and second == 0 and hour == 0:
            crypto.getMostTradedCoins()
            logger.info("list of instruments updated")
        
        sleep(0.8)

    

    

if __name__ == '__main__':
    db = DatabaseConnection()
    logger = LoggingController.start_logging()
    
    sleep(2)
    main(db=db, logger=logger)