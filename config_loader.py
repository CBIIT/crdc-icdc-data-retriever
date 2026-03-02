import logging
import os
import re

import yaml

logger = logging.getLogger(__name__)


ENV_VAR_PATTERN = re.compile(r"\$\{([^}:\s]+)(?::-(.*?)?)?\}")


class ConfigHandler:
    """
    A class for loading, validating and managing YAML-based application configuration.
    """

    def __init__(self, config_data: dict):
        """
        Initialize ConfigHandler with preloaded configuration data.

        Args:
            config_data (dict): Loaded config dict.

        Raises:
            TypeError: If config_data is not a dict.
        """
        if not isinstance(config_data, dict):
            raise TypeError("Config data must be a dictionary!")
        self.config = config_data
        logger.debug("ConfigHandler instance initialized with supplied config data.")

    def basic_load(self) -> dict:
        """
        Load and validate config from set file path.

        Returns:
            dict: Loaded config dict.
        """
        with open(self.config_path, "r") as file:
            self.config = yaml.safe_load(file)
        self.validate()
        return self.config

    def validate(self) -> None:
        """
        Validate the structure and required fields of the config data.

        Raises:
            ValueError: If required fields or formats are missing/invalid.
        """
        logger.info("Validating configuration file...")

        if "project" not in self.config:
            raise ValueError("Missing 'project' key in config")

        source_types = [
            s.get("type", "").lower() for s in self.config.get("sources", [])
        ]
        if "entity_source" not in self.config and not all(
            st == "rest_raw" for st in source_types
        ):
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

        logger.info("Configuration successfully validated!")

    @staticmethod
    def _validate_output_config(output: dict) -> None:
        """
        Validate 'output' section of application config.

        Args:
            output (dict): Output config block.

        Raises:
            ValueError: If required fields are missing or invalid.
        """
        logger.debug("Validating output configuration block")

        if "destination" not in output:
            raise ValueError("Missing 'destination' key in 'output'")

        destination = output["destination"].lower()
        if destination != "opensearch":
            raise ValueError(
                "Currently, only 'opensearch' is supported as an output destination"
            )

        ConfigHandler._require_dict_block(output, "config", "output")
        config_block = output["config"]

        if not any(key in config_block for key in ("host", "hosts")):
            raise ValueError(
                f"Missing required 'output' config key: must specify 'host' or 'hosts'"
            )

        if "index" not in config_block:
            raise ValueError(f"Missing required 'output' config key: 'index'")

        if "hosts" in config_block and "host" in config_block:
            raise ValueError(
                f"Invalid configuration: both 'host' and 'hosts' specified in 'output.config'. Please specify only one."
            )

        if "host" in config_block:
            host = config_block.get("host")
            if not isinstance(host, str) or not host.strip():
                raise ValueError(
                    "Invalid 'host' value in 'output.config': expected a non-empty string"
                )
        if "hosts" in config_block:
            hosts = config_block.get("hosts")
            if not isinstance(hosts, list) or not hosts:
                raise ValueError(
                    "Invalid 'hosts' value in 'output.config': expected a non-empty list of host strings"
                )
            for host in hosts:
                if not isinstance(host, str) or not host.strip():
                    raise ValueError(
                        "Invalid host entry in 'hosts' list: expected non-empty strings"
                    )

    @staticmethod
    def _validate_notifications_config(notifications: dict) -> None:
        """
        Validate 'notifications' section of application config.

        Args:
            notifications (dict): Notifications config block.

        Raises:
            ValueError: If required fields are missing or invalid.
        """
        logger.debug("Validating notifications configuration block")

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

        notifications_config_required_keys = ["topic_arn", "region"]
        for key in notifications_config_required_keys:
            if key not in notifications["config"]:
                raise ValueError(f"Missing required 'notifications' config key: {key}")

    @staticmethod
    def _validate_source_config(source: dict) -> None:
        """
        Validate a source entry in the config.

        Args:
            source (dict): Source config block.

        Raises:
            ValueError: If required fields are missing or invalid.
        """
        logger.debug("Validating sources configuration block")

        source_type = source.get("type", "").lower()
        required_base_keys = ["name", "type", "api_base_url"]

        if source_type != "rest_raw":
            required_base_keys.append("entity_id_key")

        for key in required_base_keys:
            if key not in source:
                raise ValueError(f"Missing required source key: {key}")

        if "discovery" not in source and "endpoint" not in source:
            raise ValueError("Source must define either 'endpoint' or 'discovery'")

        if source_type == "graphql":
            if "query" not in source:
                raise ValueError("'graphql' sources must have a valid 'query'")

        if "discovery" in source and source_type != "rest_raw":
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
    def _require_dict_block(parent: dict, key: str, context: str) -> None:
        """
        Checks that a config block exists and is a dict.

        Args:
            parent (dict): Parent config block.
            key (str): Key/name of the config block to check.
            context (str): Context name for error message.

        Raises:
            ValueError: If config block is missing or not a dict.
        """
        if key not in parent or not isinstance(parent[key], dict):
            raise ValueError(f"Missing or invalid '{key}' block in '{context}'")

    @staticmethod
    def _env_var_constructor(loader: yaml.SafeLoader, node: yaml.Node) -> str:
        """
        Handle environment variable substitution for YAML loader.

        Args:
            loader (yaml.SafeLoader): YAML loader instance.
            node (yaml.Node): Node to resolve.

        Returns:
            str: Resolved value from environment variable or fallback value.
        """
        value = loader.construct_scalar(node)
        match = ENV_VAR_PATTERN.fullmatch(value)
        if not match:
            return value

        env_var = match.group(1)
        fallback = match.group(2)

        env_value = os.getenv(env_var)
        if env_value is not None:
            logger.debug(f"Substituting environment variable: {env_var}")
            return env_value

        if fallback is not None:
            logger.warning(
                f"Environment variable '{env_var}' not found; using fallback '{fallback}'"
            )
            return fallback

        logger.warning(
            f"Environment variable '{env_var}' not found and no fallback provided; returning empty string"
        )
        return ""

    @classmethod
    def load_config_with_env_vars(cls, config_path: str) -> "ConfigHandler":
        """
        Load a config YAML with environment variable substitution enabled.

        Args:
            config_path (str): Path to config YAML file.

        Returns:
            ConfigHandler: Validated ConfigHandler instance.
        """
        logger.info(f"Loading config file: {config_path}")

        # custom YAML loader
        class EnvVarLoader(yaml.SafeLoader):
            pass

        EnvVarLoader.add_implicit_resolver("!envvar", ENV_VAR_PATTERN, None)
        EnvVarLoader.add_constructor("!envvar", cls._env_var_constructor)

        with open(config_path, "r") as file:
            config_data = yaml.load(file, Loader=EnvVarLoader)

        logger.info("Config file successfully loaded!")

        config_handler = cls(config_data)
        config_handler.validate()

        return config_handler
