import yaml
from pathlib import Path
from typing import Any, Dict
from src.exceptions import ConfigurationError

class PipelineConfig:
    """
    Handles robust loading and validation of the ETL configuration.
    """
    def __init__(self, config_path: str):
        self.config_path = Path(config_path)
        self.raw_config: Dict[str, Any] = self._load_config()
        self._validate_structure()

    def _load_config(self) -> Dict[str, Any]:
        """Loads and parses the YAML configuration file."""
        if not self.config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                return data if data is not None else {}
        except yaml.YAMLError as exc:
            raise ConfigurationError(f"YAML parsing error in config file: {exc}")
        except Exception as e:
            raise ConfigurationError(f"Unexpected error loading config file: {e}")

    def _validate_structure(self):
        """Perform basic structure checks on the loaded configuration."""
        if not isinstance(self.raw_config, dict):
            raise ConfigurationError("Configuration file must contain a top-level dictionary mapping.")
            
        required_sections = ["validation", "mapping", "transformations"]
        missing = [sec for sec in required_sections if sec not in self.raw_config]
        if missing:
            raise ConfigurationError(f"Configuration is missing required sections: {missing}")

    def get_validation_rules(self) -> Dict[str, Any]:
        return self.raw_config.get("validation", {})
        
    def get_mapping_rules(self) -> Dict[str, str]:
        return self.raw_config.get("mapping", {})
        
    def get_transformation_rules(self) -> Dict[str, Any]:
        return self.raw_config.get("transformations", {})
