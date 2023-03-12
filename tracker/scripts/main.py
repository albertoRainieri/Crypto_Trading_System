
from time import time, sleep
import sys, os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from database.DatabaseConnection import DatabaseConnection
from app.Controller.LoggingController import LoggingController
from app.Controller.TrackerController import TrackerController

def main(db, logger):
    logger.info('Hello there')
    while True:
        TrackerController.getData(db_trades=db, logger=logger)
        sleep(20)

    

if __name__ == '__main__':
    db = DatabaseConnection()
    logger = LoggingController.start_logging()
    db = db.get_db('Market_Trades')
    sleep(1)
    main(db=db, logger=logger)