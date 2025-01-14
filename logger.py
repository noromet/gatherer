import logging
from logging import StreamHandler
from logging.handlers import RotatingFileHandler

def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    rotating_file_handler = RotatingFileHandler(
        "gatherer.log", maxBytes=5*1024*1024, backupCount=5
    )
    rotating_file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    rotating_file_handler.setFormatter(formatter)
    logger.addHandler(rotating_file_handler)

    stream_handler = StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

def set_debug_mode():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    for handler in logger.handlers:
        handler.setLevel(logging.DEBUG)