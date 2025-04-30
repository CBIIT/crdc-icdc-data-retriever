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
