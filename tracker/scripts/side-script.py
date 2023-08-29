
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))

from time import sleep
from app.Controller.LoggingController import LoggingController
from database.DatabaseConnection import DatabaseConnection
from app.Controller.BinanceController import BinanceController
from app.Controller.BenchmarkController import Benchmark
from app.Controller.TradingController import TradingController
from constants.constants import *
from datetime import datetime
import json


def main(db, logger, db_logger, user_configuration):
    logger.info('Side Script Started')
    while True:
        
        now=datetime.now()
        minute = now.minute
        second = now.second
        hour = now.hour

        if minute == 59 and second == 15 and hour == 23:
        #if second == 10:
            TradingController.checkPerformance(logger, user_configuration)
            logger.info("Performance has been updated from 'TradingController.checkPerformance'")
            sleep(0.2)

        if minute == 59 and second == 18 and hour == 23:
            BinanceController.main_sort_pairs_list(logger=logger, db_logger=db_logger)
            logger.info("list of instruments updated from 'BinanceController.main_sort_pairs_list'")

        if minute == 0 and second == 20 and hour == 0:
            Benchmark.computeVolumeAverage(db=db)
            logger.info("volumes have been updated from 'Benchmark.computeVolumeAverage'")
        
        if second == 55:
            logger.info('Updating Balance Account')
            f = open ('/tracker/user_configuration/userconfiguration.json', "r")
            user_configuration = json.loads(f.read())
            TradingController.get_balance_account(logger, user_configuration)
            sleep(0.2)
        
        sleep(0.8)

    

    

if __name__ == '__main__':
    db = DatabaseConnection()
    db_logger = db.get_db(DATABASE_API_ERROR)

    logger = LoggingController.start_logging()

    f = open ('/tracker/user_configuration/userconfiguration.json', "r")
    user_configuration = json.loads(f.read())

    sleep(2)
    TradingController.clean_db_trading(logger, db_logger, user_configuration)

    for user in user_configuration:
        if user_configuration[user]['trading_live']:
            logger.info(f'TRADING_LIVE is enabled for {user}')
        else:
            logger.info(f'TRADING_LIVE is NOT enabled for {user}')


    main(db=db, logger=logger, db_logger=db_logger, user_configuration=user_configuration)