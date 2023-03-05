from time import time
from app.Controller.LoggingController import LoggingController


logger = LoggingController.start_logging()
    
def round_(number, decimal):
    return float(format(number, f".{decimal}f"))

def timer_func(func):
    # This function shows the execution time of 
    # the function object passed
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        logger.info(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func