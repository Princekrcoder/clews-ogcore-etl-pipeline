import json
from typing import Dict, Any, List
from pathlib import Path
from src.logger import get_logger
from src.schema import validate_output_data
from src.exceptions import DataLoadError

logger = get_logger()

class Loader:
    def __init__(self, output_path: str):
        self.output_path = output_path

    def load(self, data: List[Dict[str, Any]]):
        logger.info("Starting load process.")
        
        # This will raise SchemaValidationError if format is bad
        validate_output_data(data)
            
        out_path = Path(self.output_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Writing structured output to {self.output_path}")
        
        try:
            with open(self.output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4)
            logger.info(f"Successfully loaded {len(data)} records to {self.output_path}")
        except Exception as e:
            raise DataLoadError(f"Failed to write output to {self.output_path}: {e}")
