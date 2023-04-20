
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from time import sleep
from app.Controller.LoggingController import LoggingController
from database.DatabaseConnection import DatabaseConnection
from app.Controller.BinanceController import BinanceController
from app.Controller.BenchmarkController import Benchmark
from constants.constants import *
from datetime import datetime


def main(db, logger, db_logger):
    while True:
        now=datetime.now()
        minute = now.minute
        second = now.second
        hour = now.hour

        if minute == 0 and second == 15 and hour == 0:
            Benchmark.computeVolumeAverage(db=db)
            logger.info("volumes have been updated")

        
        if minute == 59 and second == 12 and hour == 23:
            BinanceController.main_sort_pairs_list(logger=logger, db_logger=db_logger)
            logger.info("list of instruments updated")
        
        sleep(0.8)

    

    

if __name__ == '__main__':
    db = DatabaseConnection()
    db_logger = db.get_db(DATABASE_API_ERROR)
    logger = LoggingController.start_logging()
    
    sleep(2)
    main(db=db, logger=logger, db_logger=db_logger)