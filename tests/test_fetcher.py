from unittest.mock import patch, Mock

import pytest

from core.fetcher import (
    fetch_from_source,
    fetch_direct,
    fetch_graphql,
    do_discovery_then_fetch,
    extract_response_data,
)


@pytest.fixture
def rest_source():
    return {
        "name": "rest_source",
        "type": "rest",
        "api_base_url": "http://mock-api",
        "endpoint": "/data",
        "response_data_key": "test_data",
    }


@pytest.fixture
def graphql_source():
    return {
        "name": "graphql_source",
        "type": "graphql",
        "api_base_url": "http://mock-api",
        "endpoint": "/graphql",
        "query": "{ data }",
        "response_data_key": "data.items",
    }


@pytest.fixture
def discovery_source():
    return {
        "name": "discoverer",
        "type": "rest",
        "api_base_url": "http://mock-api",
        "discovery": {
            "endpoint": "/discover",
            "match_key": "id",
            "filter_prefix": "abc",
        },
        "fetch": {"endpoint_template": "/data/{id}", "key_param": "id"},
        "response_data_key": "results",
    }


@pytest.fixture
def rest_source_discovery():
    return {
        "name": "rest_discovery_source",
        "type": "rest",
        "api_base_url": "http://mock-api",
        "discovery": {
            "endpoint": "/discovery",
            "match_key": "id",
            "filter_prefix": "abc",
        },
        "fetch": {"endpoint_template": "/details/{id}", "key_param": "id"},
        "response_data_key": "data",
    }


@patch("core.fetcher.fetch_direct")
def test_fetch_from_source_rest_direct(mock_fetch_direct, rest_source):
    mock_fetch_direct.return_value = [{"id": 1}]
    result = fetch_from_source(rest_source)
    assert result == [{"id": 1}]
    mock_fetch_direct.assert_called_once()


@patch("core.fetcher.do_discovery_then_fetch")
def test_fetch_from_source_rest_discovery(mock_discovery, discovery_source):
    mock_discovery.return_value = [["found1"], ["found2"]]
    result = fetch_from_source(discovery_source)
    assert result == [["found1"], ["found2"]]
    mock_discovery.assert_called_once()


@patch("core.fetcher.fetch_graphql")
def test_fetch_from_source_graphql(mock_fetch_graphql, graphql_source):
    mock_fetch_graphql.return_value = [{"name": "test"}]
    result = fetch_from_source(graphql_source)
    assert result == [{"name": "test"}]


def test_fetch_from_source_unknown_type():
    unknown_source = {"name": "unknown", "type": "ftp"}
    result = fetch_from_source(unknown_source)
    assert result is None


@patch("core.fetcher.requests.get")
def test_fetch_direct_success(mock_get, rest_source):
    mock_get.return_value.ok = True
    mock_get.return_value.json.return_value = {"test_data": [{"id": "A"}]}
    data = fetch_direct(rest_source)
    assert data == [{"id": "A"}]


@patch("core.fetcher.requests.get")
def test_fetch_direct_with_filtering(mock_get):
    test_source = {
        "name": "filtered",
        "type": "rest",
        "api_base_url": "http://mock-api",
        "endpoint": "/data",
        "match_key": "id",
        "filter_prefix": "match",
        "response_data_key": "test_data",
    }
    mock_get.return_value.ok = True
    mock_get.return_value.json.return_value = {
        "test_data": [{"id": "match_1"}, {"id": "---"}]
    }
    data = fetch_direct(test_source)
    assert data == [{"id": "match_1"}]


@patch("core.fetcher.requests.post")
def test_fetch_graphql_success(mock_post, graphql_source):
    mock_post.return_value.ok = True
    mock_post.return_value.json.return_value = {"data": {"items": [1, 2, 3]}}
    result = fetch_graphql(graphql_source)
    assert result == [1, 2, 3]


@patch("core.fetcher.requests.get")
def test_do_discovery_then_fetch_success(mock_get, rest_source_discovery):
    discovery_response = Mock()
    discovery_response.ok = True
    discovery_response.json.return_value = {
        "data": [{"id": "abc123"}, {"id": "xyz456"}]
    }

    fetch_response = Mock()
    fetch_response.ok = True
    fetch_response.json.side_effect = [{"data": {"val": 1}}, {"data": {"val": 2}}]

    mock_get.side_effect = [discovery_response, fetch_response]
    result = do_discovery_then_fetch(rest_source_discovery)
    assert result == [{"val": 1}]


def test_extract_response_data_nested_key():
    test_source = {"response_data_key": "data.items"}
    response_json = {"data": {"items": [1, 2, 3]}}
    result = extract_response_data(test_source, response_json)
    assert result == [1, 2, 3]


def test_extract_response_data_no_key():
    test_source = {}
    response_json = {"hello": "world"}
    result = extract_response_data(test_source, response_json)
    assert result == {"hello": "world"}
