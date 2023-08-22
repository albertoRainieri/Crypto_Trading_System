
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


def main(TRADING_LIVE, db, logger, db_trading, db_logger):
    while True:
        now=datetime.now()
        minute = now.minute
        second = now.second
        hour = now.hour

        if minute == 59 and second == 15 and hour == 23:
        #if second == 10:
            TradingController.checkPerformance(db_trading, logger, TRADING_LIVE)
            logger.info("Performance has been updated from 'TradingController.checkPerformance'")

        if minute == 59 and second == 18 and hour == 23:
            BinanceController.main_sort_pairs_list(logger=logger, db_logger=db_logger)
            logger.info("list of instruments updated from 'BinanceController.main_sort_pairs_list'")

        if minute == 0 and second == 20 and hour == 0:
            Benchmark.computeVolumeAverage(db=db)
            logger.info("volumes have been updated from 'Benchmark.computeVolumeAverage'")
        
        if second == 55:
            TradingController.get_balance_account(logger, db_trading)
        
        sleep(0.8)

    

    

if __name__ == '__main__':
    db = DatabaseConnection()
    db_logger = db.get_db(DATABASE_API_ERROR)
    db_trading = db.get_db(DATABASE_TRADING)
    db_benchmark = db.get_db(DATABASE_BENCHMARK)

    logger = LoggingController.start_logging()
    
    sleep(2)
    TradingController.clean_db_trading(logger, db_trading, db_benchmark, db_logger)
    del db_benchmark

    TRADING_LIVE = bool(int(os.getenv('TRADING_LIVE')))
    if TRADING_LIVE:
        logger.info('Trading Live is enabled')
    else:
        logger.info('Trading live is not enabled')


    main(TRADING_LIVE, db=db, logger=logger, db_trading=db_trading, db_logger=db_logger)