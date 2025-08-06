import copy
import os
from unittest.mock import patch, mock_open

import pytest

from config_loader import ConfigHandler


@pytest.fixture
def valid_config():
    return {
        "project": "TEST_PROJECT",
        "entity_source": "source_A",
        "sources": [
            {
                "name": "source1",
                "type": "graphql",
                "api_base_url": "https://mock-api.gov",
                "entity_id_key": "id",
                "endpoint": "/graphql",
                "query": "{ testQuery }",
            }
        ],
        "output": {
            "destination": "opensearch",
            "config": {"host": "https://mock-host:9200", "index": "test-index"},
        },
    }


@pytest.fixture
def yaml_with_env_var():
    return """
    project: ${PROJECT_NAME:-TestProject}
    entity_source: source_A
    output:
      destination: opensearch
      config:
        host: http://mock-host.com
        index: test-index
    sources:
      - name: source1
        type: graphql
        api_base_url: http://mock-api.com
        entity_id_key: mock_id_key
        endpoint: /graphql
        query: "{ testQuery }"
    """


def test_init_valid_config(valid_config):
    handler = ConfigHandler(valid_config)
    assert handler.config["project"] == "TEST_PROJECT"


def test_init_raises_type_error_on_non_dict():
    with pytest.raises(TypeError):
        ConfigHandler("invalid input")


def test_validate_missing_project_key(valid_config):
    invalid_config = copy.deepcopy(valid_config)
    del invalid_config["project"]
    with pytest.raises(ValueError, match="Missing 'project' key in config"):
        ConfigHandler(invalid_config).validate()


def test_validate_missing_output_config_key(valid_config):
    invalid_config = copy.deepcopy(valid_config)
    del invalid_config["output"]["config"]["host"]
    with pytest.raises(ValueError, match="Missing required 'output' config key: host"):
        ConfigHandler(invalid_config).validate()


def test_validate_invalid_output_destination(valid_config):
    invalid_config = copy.deepcopy(valid_config)
    invalid_config["output"]["destination"] = "neo4j"
    with pytest.raises(
        ValueError,
        match="Currently, only 'opensearch' is supported as an output destination",
    ):
        ConfigHandler(invalid_config).validate()


def test_validate_missing_source_fields(valid_config):
    invalid_config = copy.deepcopy(valid_config)
    invalid_config["sources"][0] = {"name": "invalid_source"}
    with pytest.raises(
        ValueError,
        match="Each data source must define a 'name', 'type', 'api_base_url' and 'entity_id_key'",
    ):
        ConfigHandler(invalid_config).validate()


@patch.dict(os.environ, {}, clear=True)
def test_env_var_fallback(yaml_with_env_var):
    config_yaml = mock_open(read_data=yaml_with_env_var)
    with patch("builtins.open", config_yaml):
        handler = ConfigHandler.load_config_with_env_vars("dummy-config.yaml")
    assert handler.config["project"] == "TestProject"


@patch.dict(os.environ, {"PROJECT_NAME": "FromEnvVar"})
def test_env_var_override(yaml_with_env_var):
    config_yaml = mock_open(read_data=yaml_with_env_var)
    with patch("builtins.open", config_yaml):
        handler = ConfigHandler.load_config_with_env_vars("dummy-config.yaml")
    assert handler.config["project"] == "FromEnvVar"
