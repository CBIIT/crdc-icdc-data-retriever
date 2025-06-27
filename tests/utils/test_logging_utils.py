import logging
import os
import tempfile

from utils.logging_utils import setup_logging


def test_create_log_file():
    with tempfile.TemporaryDirectory() as temp_dir:
        log_file_path = os.path.join(temp_dir, "test.log")

    setup_logging(log_file_path=log_file_path, level=logging.DEBUG)

    logger = logging.getLogger()
    logger.debug("Test log message.")

    assert os.path.exists(log_file_path)

    with open(log_file_path, "r") as log_file:
        contents = log_file.read()
        assert "Test log message." in contents


def test_no_duplicate_handlers():
    logger = logging.getLogger()
    logger.handlers.clear()

    setup_logging(log_file_path="tmp/test1.log", level=logging.INFO)
    initial_handlers_count = len(logger.handlers)

    setup_logging(log_file_path="tmp/test2.log", level=logging.INFO)
    latter_handlers_count = len(logger.handlers)

    assert initial_handlers_count == latter_handlers_count
