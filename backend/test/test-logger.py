import logging
from logging.handlers import RotatingFileHandler


# Create a logger
logger = logging.getLogger('my_logger')
logger.setLevel(logging.DEBUG)

# Create a file handler for info messages
info_handler = logging.FileHandler('info.log')
info_handler.setLevel(logging.INFO)

# Create a file handler for error messages
error_handler = logging.FileHandler('error.log')
error_handler.setLevel(logging.ERROR)

# Create a formatter for the handlers
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add the formatter to the handlers
info_handler.setFormatter(formatter)
error_handler.setFormatter(formatter)

# Add the handlers to the logger
logger.addHandler(info_handler)
logger.addHandler(error_handler)


handler = RotatingFileHandler(filename="info.log", maxBytes=500000,
                          backupCount=5)
handler2 = RotatingFileHandler(filename="error.log", maxBytes=500000,
                          backupCount=5)
logger.addHandler(handler)
logger.addHandler(handler2)

# Log an info message
logger.info('This is an info message')

# Log an error message
logger.error('This is an error message')