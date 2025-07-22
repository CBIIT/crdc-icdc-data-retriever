import logging
import os

import boto3

logger = logging.getLogger(__name__)


class SNSNotifier:
    """
    Handles configuration and interaction with AWS SNS for sending notifications.
    """

    def __init__(self, topic_arn: str, region: str):
        """
        Initialize SNSNotifier with SNS topic ARN and region.

        Args:
            topic_arn (str): SNS topic ARN.
            region (str): AWS region.

        Raises:
            EnvironmentError: If required AWS credentials are missing.
        """
        self.topic_arn = topic_arn
        self.region = region

        aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        if not aws_access_key_id or not aws_secret_access_key:
            raise EnvironmentError("Missing AWS credentials in environment variables")

        logger.debug(f"Initializing SNS client for topic {self.topic_arn}")

        self.client = boto3.client(
            "sns",
            region_name=self.region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    def notify(self, subject: str, message: str) -> bool:
        """
        Publish message to SNS topic.

        Args:
            subject (str): Message subject.
            message (str): Notification message.

        Returns:
            bool: True if the message was successfully published, otherwise False.
        """
        try:
            response = self.client.publish(
                TopicArn=self.topic_arn,
                Message=message,
                Subject=subject,
            )
            logger.info(f"Notification published to SNS topic {self.topic_arn}")
            logger.debug(f"SNS publish response: {response}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish SNS notification: {e}")
            return False
