import os
import boto3


class SNSNotifier:
    def __init__(self, topic_arn: str, region: str = "us-east-1"):
        self.topic_arn = topic_arn
        self.region = region

        self.client = boto3.client(
            "sns",
            region_name=self.region,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

    def notify(self, subject: str, message: str) -> bool:
        try:
            response = self.client.publish(
                TopicArn=self.topic_arn,
                Message=message,
                Subject=subject,
            )
            # logging
            return True
        except Exception as e:
            # logging
            return False
