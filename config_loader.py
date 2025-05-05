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
        if "sources" not in self.config or not isinstance(self.config["sources"], list):
            raise ValueError("Missing or invalid 'sources' section in config")
        for source in self.config["sources"]:
            if (
                "name" not in source or 
                "type" not in source or 
                "api_base_url" not in source or
                ("endpoint" not in source and "discovery" not in source)
            ):
                raise ValueError("Each data source must define a 'name', 'type', 'api_base_url' and 'endpoint'")
            if (
                source["type"] == "graphql" and
                (("query" not in source or "method" not in source) or
                source["method"].lower() != "post") 
            ):
                raise ValueError("GraphQL type sources require a 'query' and POST 'method'")
            if "discovery" in source and "fetch" not in source:
                raise ValueError("'fetch' property undefined for source requiring 'discovery'")
            if (
                "discovery" in source and
                "endpoint" not in source["discovery"] or
                "match_key" not in source["discovery"] or
                "filter_prefix" not in source["discovery"]
            ):
                raise ValueError("'discovery' property requires 'endpoint', 'match_key' and 'filter_prefix'")
            if (
                "discovery" in source and
                "endpoint_template" not in source["fetch"] or
                "key_param" not in source["fetch"]
            ):
                raise ValueError("'fetch' property requires 'endpoint_template' and 'key_param'")
            
        return