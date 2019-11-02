from logger import get_logger
from common import log_on_failure

logger = get_logger(__name__, 'testing_logs')


@log_on_failure
def divide_by_zero():
    raise BaseException("Please print this, please")


divide_by_zero()