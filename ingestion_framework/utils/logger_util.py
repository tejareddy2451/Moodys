import sys
import logging
import functools
import traceback

format_str='%(asctime)s %(levelname)s%(message)s'

logging.basicConfig(format=format_str, level=logging.INFO)
logger = logging.getLogger()

def common_logger_exception(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                print("::: Start Of "+func.__name__)
                res = func(*args, **kwargs)
                print("::: End Of "+func.__name__)
                return res
            except Exception as err:
                """log exception if occurs in function"""
                logger.error("::: Error in "+func.__name__)
                logger.error(f"::: Exception: {str(sys.exc_info()[1])}")
                logger.error(f"::: Exception: {str(traceback.print_exc())}")
                raise Exception(f'Exception {err}')
        return inner