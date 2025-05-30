import os
import re
import yaml
from pathlib import Path

ENV_VAR_PATTERN = re.compile(r"\${([^}^{:\-]+)(:-([^}]+))?}")


class ConfigHandler:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config = None
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

    def basic_load(self):
        with open(self.config_path, "r") as file:
            self.config = yaml.safe_load(file)
        self.validate()
        return self.config

    def validate(self):
        if "project" not in self.config:
            raise ValueError("Missing 'project' key in config")

        if "entity_source" not in self.config:
            raise ValueError("Missing 'entity_source' key in config")

        ConfigHandler._require_dict_block(self.config, "output", "config")
        ConfigHandler._validate_output_config(self.config["output"])

        notifications = self.config.get("notifications")
        if notifications:
            ConfigHandler._validate_notifications_config(notifications)

        sources = self.config.get("sources")
        if not sources or not isinstance(sources, list):
            raise ValueError("Missing or invalid 'sources' section in config")

        for source in sources:
            ConfigHandler._validate_source_config(source)

    @staticmethod
    def _validate_output_config(output: dict):
        if "destination" not in output:
            raise ValueError("Missing 'destination' key in 'output'")

        destination = output["destination"].lower()
        if destination != "opensearch":
            raise ValueError(
                "Currently, only 'opensearch' is supported as an output destination"
            )

        ConfigHandler._require_dict_block(output, "config", "output")

        output_config_required_keys = ["host", "index"]
        for key in output_config_required_keys:
            if key not in output["config"]:
                raise ValueError(f"Missing required 'output' config key: {key}")

    @staticmethod
    def _validate_notifications_config(notifications: dict):
        if not isinstance(notifications, dict):
            raise ValueError("Invalid 'notifications' block structure")
        if "destination" not in notifications:
            raise ValueError("Missing 'destination' key in 'notifications'")

        destination = notifications["destination"].lower()
        if destination != "sns":
            raise ValueError(
                "Currently, only 'sns' is supported as a notification destination"
            )

        ConfigHandler._require_dict_block(notifications, "config", "notifications")

        notifications_config_required_keys = ["topic_arn"]
        for key in notifications_config_required_keys:
            if key not in notifications["config"]:
                raise ValueError(f"Missing required 'notifications' config key: {key}")

    @staticmethod
    def _validate_source_config(source: dict):
        if not all(
            key in source for key in ("name", "type", "api_base_url", "entity_id_key")
        ):
            raise ValueError(
                "Each data source must define a 'name', 'type', 'api_base_url' and 'entity_id_key'"
            )
        if "discovery" not in source and "endpoint" not in source:
            raise ValueError("Source must define either 'endpoint' or 'discovery'")

        source_type = source["type"].lower()
        if source_type == "graphql":
            if "query" not in source:
                raise ValueError("'graphql' sources must have a valid 'query'")

        if "discovery" in source:
            discovery = source["discovery"]
            fetch = source.get("fetch", {})
            if not all(
                key in discovery for key in ("endpoint", "match_key", "filter_prefix")
            ):
                raise ValueError(
                    "'discovery' property requires defined 'endpoint', 'match_key' and 'filter_prefix'"
                )
            if not fetch:
                raise ValueError(
                    "source using 'discovery' must define a 'fetch' section with 'endpoint_template' and 'key_param'"
                )
            if not all(key in fetch for key in ("endpoint_template", "key_param")):
                raise ValueError(
                    "'fetch' property requires defined 'endpoint_template' and 'key_param'"
                )

    @staticmethod
    def _require_dict_block(parent: dict, key: str, context: str):
        if key not in parent or not isinstance(parent[key], dict):
            raise ValueError(f"Missing or invalid '{key}' block in '{context}'")

    @staticmethod
    def _env_var_constructor(loader, node):
        value = loader.construct_scalar(node)
        match = ENV_VAR_PATTERN.fullmatch(value)
        if match:
            env_var = match.group(1)
            fallback = match.group(3)
            return os.getenv(env_var, fallback or "")
        return value

    @classmethod
    def load_config_with_env_vars(cls, config_path: str) -> "ConfigHandler":
        # custom YAML loader class
        class EnvVarLoader(yaml.SafeLoader):
            pass

        EnvVarLoader.add_implicit_resolver("!envvar", ENV_VAR_PATTERN, None)
        EnvVarLoader.add_constructor("!envvar", cls._env_var_constructor)

        with open(config_path, "r") as file:
            config_data = yaml.load(file, Loader=EnvVarLoader)

        config_handler = cls(config_data)
        config_handler.validate()

        return config_handler
