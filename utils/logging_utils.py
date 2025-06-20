import logging
from logging.handlers import RotatingFileHandler
import os


def setup_logging(
    log_file_path: str = "tmp/app.log", level: int = logging.INFO
) -> None:
    """Configures root logger and file/console handlers for application.

    Args:
        log_file_path (str): Path to log file.
        level (int): Logging level (ex: logging.INFO, logging.DEBUG).

    Returns:
        None
    """
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(level)
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = RotatingFileHandler(log_file_path, maxBytes=5_000_000, backupCount=3)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
