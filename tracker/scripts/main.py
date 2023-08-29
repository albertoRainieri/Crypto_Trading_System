
from time import time, sleep
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from app.Controller.TrackerController import TrackerController
from datetime import datetime, timedelta
import asyncio
from constants.constants import *
import json

SECOND_START_TRACKING = 2
FIRST_RUN = True

def main(db_trades, db_tracker, db_benchmark, db_logger, logger, FIRST_RUN, SECOND_START_TRACKING, user_configuration):
    '''
    This function tracks the statistics of the most traded pairs each minute
    '''
    
    logger.info('Tracker Started')

    while True:


        if datetime.now().second == SECOND_START_TRACKING:
            #logger.info(datetime.now())
            #asyncio.run(TrackerController.start_tracking(db_trades=db_trades, db_tracker=db_tracker, db_benchmark=db_benchmark, logger=logger, db_logger=db_logger))
            TrackerController.start_tracking(db_trades=db_trades, db_tracker=db_tracker, db_benchmark=db_benchmark, logger=logger, db_logger=db_logger)
            
            #update user_configuration
            # f = open ('/tracker/user_configuration/userconfiguration.json', "r")
            # user_configuration = json.loads(f.read())
            
            # sleep until next run of tracking
            FIRST_RUN = False
            time_delta = (datetime.now() + timedelta(minutes=1)).replace(second=SECOND_START_TRACKING, microsecond=0) - datetime.now()
            sleep_seconds = time_delta.seconds + (time_delta.microseconds / 10**6)
            sleep(sleep_seconds)

            
            

        if FIRST_RUN:
            sleep(0.5)

    

if __name__ == '__main__':
    db = DatabaseConnection()
    logger = LoggingController.start_logging()
    db_trades = db.get_db(database=DATABASE_MARKET)
    db_tracker = db.get_db(database=DATABASE_TRACKER)
    db_benchmark = db.get_db(database=DATABASE_BENCHMARK)
    #db_trading = db.get_db(database=DATABASE_TRADING)
    db_logger = db.get_db(DATABASE_LOGGING)

    SECOND_START_TRACKING = 2
    FIRST_RUN = True

    f = open ('/tracker/user_configuration/userconfiguration.json', "r")
    user_configuration = json.loads(f.read())



    sleep(2)
    main(db_trades=db_trades, db_tracker=db_tracker, db_benchmark=db_benchmark, db_logger=db_logger,
          logger=logger, FIRST_RUN=FIRST_RUN, SECOND_START_TRACKING=SECOND_START_TRACKING,
          user_configuration=user_configuration)