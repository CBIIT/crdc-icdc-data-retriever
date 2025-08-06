import os
from unittest.mock import patch, MagicMock

import pytest

from core.sns_notifier import SNSNotifier


@pytest.fixture(autouse=True)
def set_env_vars(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-access-key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")


@patch("core.sns_notifier.boto3.client")
def test_init_success(mock_boto_client):
    mock_client_instance = MagicMock()
    mock_boto_client.return_value = mock_client_instance

    notifier = SNSNotifier(topic_arn="arn:aws:sns:us-east-1:12345:test-topic")
    assert notifier.topic_arn == "arn:aws:sns:us-east-1:12345:test-topic"
    assert notifier.region == "us-east-1"

    mock_boto_client.assert_called_once_with(
        "sns",
        region_name="us-east-1",
        aws_access_key_id="test-access-key",
        aws_secret_access_key="test-secret-key",
    )


@patch("core.sns_notifier.boto3.client")
def test_notify_success(mock_boto_client):
    mock_client_instance = MagicMock()
    mock_client_instance.publish.return_value = {"Message": "test message"}
    mock_boto_client.return_value = mock_client_instance

    notifier = SNSNotifier(topic_arn="arn:aws:sns:us-east-1:12345:test-topic")
    result = notifier.notify(subject="TEST", message="Test message.")

    assert result is True
    mock_client_instance.publish.assert_called_once()


@patch("core.sns_notifier.boto3.client")
def test_notify_failure(mock_boto_client, caplog):
    mock_client_instance = MagicMock()
    mock_client_instance.publish.side_effect = Exception("SNS failure")
    mock_boto_client.return_value = mock_client_instance

    notifier = SNSNotifier(topic_arn="arn:aws:sns:us-east-1:12345:test-topic")
    result = notifier.notify(subject="TEST", message="Test message.")

    assert result is False
    assert "Failed to publish SNS notification" in caplog.text


@patch.dict(os.environ, {}, clear=True)
def test_missing_credentials_raises_error(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)

    with pytest.raises(
        EnvironmentError, match="Missing AWS credentials in environment variables"
    ):
        SNSNotifier(topic_arn="arn:aws:sns:us-east-1:12345:test-topic")
