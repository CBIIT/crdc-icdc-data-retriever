import yaml
from pathlib import Path


class ConfigHandler:
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.config = None
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

    def load(self):
        with open(self.config_path, "r") as file:
            self.config = yaml.safe_load(file)
        self.validate()
        return self.config

    def validate(self):
        # minimal validation logic
        if "project" not in self.config:
            raise ValueError("Missing 'project' key in config")

        sources = self.config.get("sources")
        if not sources or not isinstance(sources, list):
            raise ValueError("Missing or invalid 'sources' section in config")

        for source in sources:
            if not all(key in source for key in ("name", "type", "api_base_url")):
                raise ValueError(
                    "Each data source must define a 'name', 'type' and 'api_base_url'"
                )

            if "discovery" not in source and "endpoint" not in source:
                raise ValueError("Source must define either 'endpoint' or 'discovery'")

            if source["type"] == "graphql":
                if "query" not in source:
                    raise ValueError("'graphql' sources must have a valid 'query'")

            if "discovery" in source:
                discovery = source["discovery"]
                fetch = source.get("fetch", {})
                if not all(
                    key in discovery
                    for key in ("endpoint", "match_key", "filter_prefix")
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
