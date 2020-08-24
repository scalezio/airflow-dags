import logging
import sys
from functools import wraps
from time import time
import traceback

logging.getLogger('boto').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)

timing_logger = logging.getLogger()


def timing(f_name=""):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            start = time()
            name = f_name if f_name else f.__name__
            try:
                result = f(*args, **kwargs)
                time_ = int((time() - start) * 1000)
                timing_logger.info(f'Done {name} in {time_} milliseconds')
                return result
            except Exception as e:
                timing_logger.error(e)
                traceback.print_tb(e.__traceback__)
                raise e

        return wrapper

    return decorator


def get_logger(name=''):
    logger_ = logging.getLogger(name)
    logger_.setLevel(logging.INFO)
    logger_.propagate = False
    logger_.handlers = []

    handler_ = logging.StreamHandler(sys.stdout)
    handler_.setLevel(logging.INFO)
    formatter_ = logging.Formatter("[%(levelname)-5.5s] %(asctime)s %(message)s")
    handler_.setFormatter(formatter_)
    logger_.addHandler(handler_)
    return logger_
