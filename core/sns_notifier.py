import logging
import os

import boto3

logger = logging.getLogger(__name__)


class SNSNotifier:
    def __init__(self, topic_arn: str, region: str = "us-east-1"):
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
