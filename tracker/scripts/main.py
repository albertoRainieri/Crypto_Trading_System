
from time import time, sleep
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from app.Helpers.Helpers import round_, get_best_coins, get_volume_standings, get_coins_list
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from app.Controller.TrackerController import TrackerController
from datetime import datetime, timedelta
import asyncio
from constants.constants import *
import json

SECOND_START_TRACKING = 2
FIRST_RUN = True

def main(db_trades, db_tracker, db_benchmark, db_logger, db_volume_standings, logger, FIRST_RUN, SECOND_START_TRACKING, coins_list):
    '''
    This function tracks the statistics of the most traded pairs each minute
    '''

    volume_standings = get_volume_standings(db_volume_standings)
    best_x_coins = get_best_coins(volume_standings)

    while True:

        now = datetime.now()
        if now.second == SECOND_START_TRACKING:
            t1 = time()
            TrackerController.start_tracking(best_x_coins, coins_list, volume_standings=volume_standings, db_trades=db_trades, db_tracker=db_tracker,
                                              db_benchmark=db_benchmark, logger=logger, db_logger=db_logger)
            if now.hour == 23 and now.minute == 59:
                coins_list = get_coins_list()
            if now.hour == 0 and now.minute == 0:
                t2 = time()
                timespent = round_(t2-t1,3)
                logger.info(f'start_tracking executed in {timespent}s')
                volume_standings = get_volume_standings(db_volume_standings)
                best_x_coins = get_best_coins(volume_standings)
    
            # sleep until next run of tracking
            FIRST_RUN = False
            time_delta = (datetime.now() + timedelta(minutes=1)).replace(second=SECOND_START_TRACKING, microsecond=0) - datetime.now()
            sleep_seconds = time_delta.seconds + (time_delta.microseconds / 10**6)
            sleep(sleep_seconds)
            

        if FIRST_RUN:
            sleep(0.5)

    

if __name__ == '__main__':
    client = DatabaseConnection()
    logger = LoggingController.start_logging()
    db_trades = client.get_db(database=DATABASE_MARKET)
    db_tracker = client.get_db(database=DATABASE_TRACKER)
    db_benchmark = client.get_db(database=DATABASE_BENCHMARK)
    db_volume_standings = client.get_db(DATABASE_VOLUME_STANDINGS)
    db_logger = client.get_db(DATABASE_LOGGING)

    SECOND_START_TRACKING = 3
    FIRST_RUN = True


    sleep(2)
    logger.info('Main Script Started')
    coins_list = get_coins_list()
    main(db_trades=db_trades, db_tracker=db_tracker, db_benchmark=db_benchmark, db_logger=db_logger,
          db_volume_standings=db_volume_standings, logger=logger, FIRST_RUN=FIRST_RUN, SECOND_START_TRACKING=SECOND_START_TRACKING, coins_list=coins_list)