import logging
import os
from datetime import datetime

LOGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')

def setup_logger():
    os.makedirs(LOGS_DIR, exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_file = os.path.join(LOGS_DIR, f'run_{timestamp}.log')

    logger = logging.getLogger('pipeline')
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(message)s'))

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info(f"Log file: {os.path.abspath(log_file)}")
    return logger

def get_logger():
    logger = logging.getLogger('pipeline')
    if not logger.handlers:
        setup_logger()
    return logger
