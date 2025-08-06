"""
Main entry point for the Data Retriever Service.

Loads configuration file, orchestrates data fetching and entity mapping,
writes results to OpenSearch and sends success/failure notifications.
"""

import argparse
import logging

from config_loader import ConfigHandler
import core.dispatcher as dispatcher
from core.sns_notifier import SNSNotifier
from core.writer.opensearch_writer import OpenSearchWriter
from utils.logging_utils import setup_logging
from utils.notification_utils import build_notification_message

logger = logging.getLogger(__name__)


def parse_args(args=None):
    """
    Parses command-line arguments for the application.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Run data retriever service.")

    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to config file (default: config.yaml).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set the logging level (default: INFO).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the app without writing to OpenSearch or sending SNS notifications.",
    )
    parser.add_argument(
        "--parallel-fetch",
        action="store_true",
        help="Enable multithreaded fetching of source data.",
    )

    return parser.parse_args()


def main():
    """
    Runs the data retriever pipeline.

    Steps:
        - Loads the configuration file
        - Fetches external data
        - Maps data to project entities
        - Writes data to OpenSearch
        - Sends success/failure SNS notification

    Returns:
        None
    """
    success = False
    mappings = []
    config = {}
    project = "<unknown>"

    args = parse_args()
    setup_logging(level=getattr(logging, args.log_level))

    try:
        config_handler = ConfigHandler.load_config_with_env_vars(args.config)
        config = config_handler.config
        project = config["project"]

        mappings = dispatcher.run_dispatcher(config, args.parallel_fetch)
        if mappings:
            if args.dry_run:
                logger.info(
                    "Dry run mode enabled: skipping OpenSearch write and notifications"
                )
            else:
                writer = OpenSearchWriter(config=config)
                write_results = writer.bulk_write_documents(mappings)

                written = write_results.get("success", 0)
                if written > 0:
                    success = True

    except Exception as e:
        logger.exception(f"Data Retriever Service pipeline failed: {e}")

    finally:
        if config.get("notifications") and not args.dry_run:
            try:
                topic_arn = config["notifications"]["config"]["topic_arn"]
                region = config["notifications"]["config"]["region"]

                notifier = SNSNotifier(topic_arn=topic_arn, region=region)
                message = build_notification_message(
                    success=success, mappings=mappings, project=project
                )
                notifier.notify(subject="Data Retriever Service", message=message)
            except Exception as notify_err:
                logger.error(f"Failed to send SNS notification: {notify_err}")


if __name__ == "__main__":
    main()
