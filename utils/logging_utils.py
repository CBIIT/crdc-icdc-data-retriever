import logging
import os


def setup_logging(log_file_path="tmp/app.log", level=logging.INFO):
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = [console_handler, file_handler]
