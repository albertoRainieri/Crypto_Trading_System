
from time import time, sleep
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from app.Controller.TrackerController import TrackerController
from datetime import datetime
import asyncio
from constants.constants import *

def main(db_trades, db_tracker, db_benchmark, db_trading, db_logger, logger):
    '''
    This function tracks the statistics of the most traded pairs each minute
    '''
    
    logger.info('Tracker Started')

    while True:
        now=datetime.now()
        second = now.second

        if second == 2:
            asyncio.run(TrackerController.start_tracking(db_trades=db_trades, db_tracker=db_tracker, db_benchmark=db_benchmark, db_trading=db_trading, logger=logger, db_logger=db_logger))

        sleep(0.8)

    

if __name__ == '__main__':
    db = DatabaseConnection()
    logger = LoggingController.start_logging()
    db_trades = db.get_db(database=DATABASE_MARKET)
    db_tracker = db.get_db(database=DATABASE_TRACKER)
    db_benchmark = db.get_db(database=DATABASE_BENCHMARK)
    db_trading = db.get_db(database=DATABASE_TRADING)
    db_logger = db.get_db(DATABASE_LOGGING)

    sleep(2)
    main(db_trades=db_trades, db_tracker=db_tracker, db_benchmark=db_benchmark, db_trading=db_trading, db_logger=db_logger, logger=logger)