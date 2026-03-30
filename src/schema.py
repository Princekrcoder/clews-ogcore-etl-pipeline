from jsonschema import Draft7Validator
from src.logger import get_logger
from src.exceptions import SchemaValidationError

logger = get_logger()

OUTPUT_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "id": {"type": ["string", "integer"]},
            "year": {"type": "integer"},
            "energy_gw": {"type": "number"},
            "region": {"type": "string"},
            "sector": {"type": "string"}
        },
        "required": ["year", "energy_gw", "region", "sector"],
        "additionalProperties": True 
    }
}

def validate_output_data(data: list) -> None:
    """
    Validates output data against OUTPUT_SCHEMA. Raises SchemaValidationError if invalid.
    """
    logger.info("Validating output JSON data against structural schema.")
    
    validator = Draft7Validator(OUTPUT_SCHEMA)
    errors = sorted(validator.iter_errors(data), key=lambda e: e.path)
    
    if errors:
        error_messages = []
        for error in errors:
            path = ".".join([str(p) for p in error.path])
            msg = f"[{path}]: {error.message}"
            logger.error(f"Schema validation error at path {msg}")
            error_messages.append(msg)
            
        raise SchemaValidationError(f"Output validation failed with errors: {error_messages}")
        
    logger.info("Output schema validation passed successfully.")
