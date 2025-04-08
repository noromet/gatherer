import logging
from logging import StreamHandler
from logging.handlers import RotatingFileHandler

# Define ANSI escape codes for colors
LOG_COLORS = {
    "DEBUG": "\033[36m",    # Cyan
    "INFO": "\033[32m",     # Green
    "WARNING": "\033[33m",  # Yellow
    "ERROR": "\033[31m",    # Red
    "CRITICAL": "\033[1;31m",  # Bold Red
    "RESET": "\033[0m"      # Reset
}

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        log_color = LOG_COLORS.get(record.levelname, LOG_COLORS["RESET"])
        record.msg = f"{log_color}{record.msg}{LOG_COLORS['RESET']}"
        return super().format(record)

def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # File handler (no color)
    rotating_file_handler = RotatingFileHandler(
        "gatherer.log", maxBytes=5*1024*1024, backupCount=5
    )
    rotating_file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    rotating_file_handler.setFormatter(file_formatter)
    logger.addHandler(rotating_file_handler)

    # Stream handler (with color)
    stream_handler = StreamHandler()
    stream_handler.setLevel(logging.INFO)
    color_formatter = ColoredFormatter("%(asctime)s [%(levelname)s] %(message)s")
    stream_handler.setFormatter(color_formatter)
    logger.addHandler(stream_handler)

def set_debug_mode():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    for handler in logger.handlers:
        handler.setLevel(logging.DEBUG)