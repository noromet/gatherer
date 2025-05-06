"""
logger.py

This module provides logging functionality for the gatherer application.
It includes a custom formatter for colored log messages and functions
to set up logging with both file and stream handlers.
"""

import logging
from logging import StreamHandler
from logging.handlers import RotatingFileHandler

# Define ANSI escape codes for colors
LOG_COLORS = {
    "DEBUG": "\033[36m",  # Cyan
    "INFO": "\033[32m",  # Green
    "WARNING": "\033[33m",  # Yellow
    "ERROR": "\033[31m",  # Red
    "CRITICAL": "\033[1;31m",  # Bold Red
    "RESET": "\033[0m",  # Reset
}


class ColoredFormatter(logging.Formatter):
    """
    A custom logging formatter that adds color to log messages
    based on their severity level.
    """

    def format(self, record):
        """
        Formats a log record with color based on its level.

        Args:
            record (LogRecord): The log record to format.

        Returns:
            str: The formatted log message.
        """
        log_color = LOG_COLORS.get(record.levelname, LOG_COLORS["RESET"])
        record.msg = f"{log_color}{record.msg}{LOG_COLORS['RESET']}"
        return super().format(record)


def setup_logger():
    """
    Sets up the logger with both file and stream handlers.
    The file handler writes logs to a rotating file, while
    the stream handler outputs colored logs to the console.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # File handler (no color)
    rotating_file_handler = RotatingFileHandler(
        "gatherer.log", maxBytes=5 * 1024 * 1024, backupCount=5
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
    """
    Sets the logger to debug mode, enabling debug-level logging
    for all handlers.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    for handler in logger.handlers:
        handler.setLevel(logging.DEBUG)
