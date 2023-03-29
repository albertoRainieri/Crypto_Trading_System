import logging
import json
from logging.handlers import RotatingFileHandler
import os


class LoggingController:

    @staticmethod
    def start_logging():
        if not os.path.exists("logs"):
            os.mkdir("logs")

        debug = logging.FileHandler("logs/debug.log")
        debug.setLevel(logging.DEBUG)

        error = logging.FileHandler("logs/error.log")
        error.setLevel(logging.ERROR)

        console = logging.StreamHandler()

        logging.basicConfig(  # noqa
            level=logging.INFO,
            #format="[%(asctime)s]:%(levelname)s: %(message)s",
            #format="[%(asctime)s]:%(levelname)s %(name)s :%(module)s/%(funcName)s,%(lineno)d: %(message)s",
            format="[%(asctime)s]: %(message)s",

            handlers=[debug, error, console]
        )

        # logger.debug("This is debug  [error+warning]")
        # logger.error("This is error  [error only]")
        # logger.warning("This is warn [error+warning]")

        logger = logging.getLogger()
        #logger.error("This is error  [error only]")

        handler = RotatingFileHandler(filename="logs/debug.log", maxBytes=50000,
                                  backupCount=1)
        handler2 = RotatingFileHandler(filename="logs/error.log", maxBytes=50000,
                                  backupCount=1)
        #logger.addHandler(handler2)
        #logger.addHandler(handler2)
        return logger