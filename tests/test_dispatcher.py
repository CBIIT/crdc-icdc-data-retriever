import copy
from unittest.mock import patch

import pytest

SOURCE_CONFIG = [
    {"name": "source_A"},
    {
        "name": "source_B",
        "dataset_base_url": "https://mock-data-url/{id}",
        "dataset_base_url_param": "id",
    },
]

ENTITIES = [{"id": 1}, {"id": 2}]

FETCHED_DATA = {
    "source_A": ENTITIES,
    "source_B": [{"entity_id": 1, "entity_id": 2}],
}


@pytest.fixture
def config():
    return {"entity_source": "source_A", "sources": SOURCE_CONFIG}


@patch("core.dispatcher.fetch_from_source")
def test_fetch_all(mock_fetch):
    mock_fetch.side_effect = lambda s: f"data_for_{s['name']}"
    from core.dispatcher import fetch_all

    test_sources = [{"name": "test_source_1"}, {"name": "test_source_2"}]
    results = fetch_all(test_sources)
    assert results == {
        "test_source_1": "data_for_test_source_1",
        "test_source_2": "data_for_test_source_2",
    }


@patch("core.dispatcher.fetch_from_source")
def test_fetch_all_parallel(mock_fetch):
    mock_fetch.side_effect = lambda s: f"parallel_data_for_{s['name']}"
    from core.dispatcher import fetch_all_parallel

    test_sources = [{"name": "test_source_1"}, {"name": "test_source_2"}]
    results = fetch_all_parallel(test_sources, max_workers=2)
    assert results == {
        "test_source_1": "parallel_data_for_test_source_1",
        "test_source_2": "parallel_data_for_test_source_2",
    }


@patch("core.dispatcher.collect_mappings")
@patch("core.dispatcher.get_post_processor")
def test_match_all(mock_get_pp, mock_collect):
    mock_get_pp.return_value = None
    mock_collect.return_value = [
        {
            "entity_id": "test_entity",
            "CRDCLinks": [{"repository": "test_repo", "metadata": {}}],
        }
    ]
    from core.dispatcher import match_all

    result = match_all(ENTITIES, SOURCE_CONFIG, FETCHED_DATA, "source_A")
    assert isinstance(result, list)
    assert result[0]["entity_id"] == "test_entity"


@patch("core.dispatcher.fetch_all")
@patch("core.dispatcher.match_all")
def test_run_dispatcher_sequential(mock_match_all, mock_fetch_all, config):
    mock_fetch_all.return_value = FETCHED_DATA
    mock_match_all.return_value = ["test_mapping_1", "test_mapping_2"]
    from core.dispatcher import run_dispatcher

    result = run_dispatcher(config, parallel=False)
    assert result == ["test_mapping_1", "test_mapping_2"]


@patch("core.dispatcher.fetch_all_parallel")
@patch("core.dispatcher.match_all")
def test_run_dispatcher_parallel(mock_match_all, mock_fetch_parallel, config):
    mock_fetch_parallel.return_value = FETCHED_DATA
    mock_match_all.return_value = ["test_mapping_1", "test_mapping_2"]
    from core.dispatcher import run_dispatcher

    result = run_dispatcher(config, parallel=True)
    assert result == ["test_mapping_1", "test_mapping_2"]


@patch("core.dispatcher.fetch_all")
def test_run_dispatcher_missing_entities(mock_fetch_all, config):
    mock_data = copy.deepcopy(FETCHED_DATA)
    mock_data["source_A"] = None
    mock_fetch_all.return_value = mock_data
    from core.dispatcher import run_dispatcher

    result = run_dispatcher(config, parallel=False)
    assert result == []
