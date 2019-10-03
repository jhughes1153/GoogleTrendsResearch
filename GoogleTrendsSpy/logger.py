import logging
import os
import getpass
import datetime as dt


def get_logger(logger_, application: str):
    logger = logging.getLogger(logger_)
    logger.setLevel(logging.DEBUG)

    create_logging_dir()

    fh = logging.FileHandler(f'/home/{getpass.getuser()}/log/{application}_'
                             f'{dt.datetime.now().strftime("%Y-%m-%d_%H%m")}.log')
    sh = logging.StreamHandler()
    formatter = logging.Formatter('(%(asctime)s) (%(funcName)-8s) [%(levelname)-5s] [%(processName)-8s]: %(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)

    logger.addHandler(sh)
    logger.addHandler(fh)

    logging.info('%%%%%%%%%%%%%%%%%%%%%%%%%')
    logging.info(f'{application}')
    logging.info(f'%%%%%%%%%%%%%%%%%%%%%%%%%')
    logging.info(f'Started at: {dt.datetime.now()}')

    return logger


def create_logging_dir():
    log_dir = f'/home/{getpass.getuser()}/log'
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
