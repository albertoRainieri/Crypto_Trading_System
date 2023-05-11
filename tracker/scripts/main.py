
from time import time, sleep
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from app.Controller.TrackerController import TrackerController
from datetime import datetime
from constants.constants import *

def main(db, db_logger, logger):
    '''
    This function tracks the statistics of the most traded pairs each minute
    '''
    
    logger.info('Tracker Started')

    while True:
        now=datetime.now()
        second = now.second

        if second == 1:
            TrackerController.getData(db_trades=db, logger=logger, db_logger=db_logger)
        sleep(0.9)

    

if __name__ == '__main__':
    db = DatabaseConnection()
    logger = LoggingController.start_logging()
    db_market = db.get_db('Market_Trades')
    db_logger = db.get_db(DATABASE_LOGGING)
    sleep(2)
    main(db=db_market, db_logger=db_logger, logger=logger)