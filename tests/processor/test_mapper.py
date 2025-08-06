from unittest.mock import patch

import pytest

from core.processor.mapper import map_matches_to_entity, collect_mappings


@pytest.fixture
def test_entity():
    return {"clinical_study_designation": "GLIOMA01"}


@pytest.fixture
def source_config():
    return {"entity_id_key": "clinical_study_designation"}


@pytest.fixture
def dataset_params():
    return {
        "dataset_base_url": "https://data.example.org/{collection_id}",
        "dataset_base_url_param": "collection_id",
        "repository_name": "Test Repo",
        "match_key": "collection_id",
    }


@patch("utils.match_utils.is_fuzzy_match", return_value=True)
@patch("utils.mapping_utils.extract_first_valid_match", return_value="GLIOMA01")
@patch(
    "utils.mapping_utils.normalize_metadata_groups",
    return_value=[[{"clinical_study_designation": "GLIOMA01"}]],
)
def test_map_matches_to_entity(
    _mock_norm,
    _mock_extract,
    _mock_match,
    test_entity,
    source_config,
    dataset_params,
):
    result = map_matches_to_entity(
        entity=test_entity,
        source_config=source_config,
        matched_source_data=[[{"collection_id": "icdc_glioma"}]],
        dataset_base_url=dataset_params["dataset_base_url"],
        dataset_base_url_param=dataset_params["dataset_base_url_param"],
        repository_name=dataset_params["repository_name"],
        match_key=dataset_params["match_key"],
    )

    assert isinstance(result, list)
    assert result[0]["repository"] == "Test Repo"
    assert result[0]["url"] == "https://data.example.org/icdc_glioma"


@patch(
    "core.processor.mapper.map_matches_to_entity",
    return_value=[
        {"repository": "Test Repo", "url": "https://test.gov", "metadata": {}}
    ],
)
def test_collect_mappings(_mock_mapper, test_entity, source_config, dataset_params):
    result = collect_mappings(
        entities=[test_entity],
        source_config=source_config,
        matched_source_data=[],
        dataset_base_url=dataset_params["dataset_base_url"],
        dataset_base_url_param=dataset_params["dataset_base_url_param"],
        repository_name=dataset_params["repository_name"],
        match_key=dataset_params["match_key"],
        post_processor=None,
    )

    assert len(result) == 1
    assert result[0]["entity_id"] == "GLIOMA01"
    assert "CRDCLinks" in result[0]
