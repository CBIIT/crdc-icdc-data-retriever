import os
from unittest.mock import patch

import pytest
from opensearchpy.exceptions import OpenSearchException

from core.writer.opensearch_writer import OpenSearchWriter


@pytest.fixture
def mock_config():
    return {
        "project": "TEST",
        "output": {
            "config": {
                "host": "https://mock-host",
                "index": "test_index",
                "use_ssl": True,
                "verify_certs": True,
            }
        },
    }


@pytest.fixture(autouse=True)
def set_env_vars(monkeypatch):
    monkeypatch.setenv("OPENSEARCH_USERNAME", "user")
    monkeypatch.setenv("OPENSEARCH_PASSWORD", "password")


@patch("core.writer.opensearch_writer.OpenSearch")
def test_init_success(mock_opensearch, mock_config):
    mock_opensearch.return_value.ping.return_value = True
    writer = OpenSearchWriter(mock_config)
    assert writer.index == "test_index"


@patch.dict(os.environ, {}, clear=True)
def test_init_missing_credentials_raises_error(mock_config):
    with pytest.raises(EnvironmentError):
        OpenSearchWriter(mock_config)


@patch("core.writer.opensearch_writer.OpenSearch")
def test_init_ping_fail_raises_error(mock_opensearch, mock_config):
    mock_opensearch.return_value.ping.return_value = False
    with pytest.raises(ConnectionError):
        OpenSearchWriter(mock_config)


@patch("core.writer.opensearch_writer.bulk")
@patch("core.writer.opensearch_writer.OpenSearch")
def test_successful_bulk_write(mock_opensearch, mock_bulk, mock_config):
    mock_opensearch.return_value.ping.return_value = True
    mock_bulk.return_value = (2, [])

    writer = OpenSearchWriter(mock_config)
    documents = [
        {"entity_id": "TEST1", "CRDCLinks": [{"repository": "test_repo_1"}]},
        {"entity_id": "TEST2", "CRDCLinks": [{"repository": "test_repo_2"}]},
    ]

    result = writer.bulk_write_documents(documents)
    assert result == {"success": 2, "attempted": 2}


@patch("core.writer.opensearch_writer.bulk")
@patch("core.writer.opensearch_writer.OpenSearch")
def test_bulk_write_skip_unserializable(
    mock_opensearch, mock_bulk, mock_config, caplog
):
    mock_opensearch.return_value.ping.return_value = True
    mock_bulk.return_value = (2, [])

    writer = OpenSearchWriter(mock_config)
    unserializable = {
        "entity_id": "TEST1",
        "CRDCLinks": [{"repository": "test_repo_1"}],
        "bad_data": set([1, 2, 3]),
    }
    serializable = {"entity_id": "TEST2", "CRDCLinks": [{"repository": "test_repo_2"}]}
    result = writer.bulk_write_documents([serializable, unserializable])

    assert result["attempted"] == 1
    assert "Skipping unserializable document" in caplog.text


@patch("core.writer.opensearch_writer.bulk", side_effect=OpenSearchException)
@patch("core.writer.opensearch_writer.OpenSearch")
def test_bulk_write_error(mock_opensearch, mock_bulk, mock_config):
    mock_opensearch.return_value.ping.return_value = True
    writer = OpenSearchWriter(mock_config)
    documents = [
        {"entity_id": "TEST1", "CRDCLinks": [{"repository": "test_repo_1"}]},
        {"entity_id": "TEST2", "CRDCLinks": [{"repository": "test_repo_2"}]},
    ]

    with pytest.raises(RuntimeError, match="Failed to perform bulk write"):
        writer.bulk_write_documents(documents)
