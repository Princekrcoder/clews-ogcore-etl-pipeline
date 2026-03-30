class ETLError(Exception):
    """Base exception for all ETL pipeline errors."""
    pass

class ConfigurationError(ETLError):
    """Raised when there is an issue with the configuration file or loading process."""
    pass

class DataExtractionError(ETLError):
    """Raised when data cannot be extracted from the source file or directory."""
    pass

class DataValidationError(ETLError):
    """Raised when input data fails early validation rules."""
    pass

class MapTransformError(ETLError):
    """Raised when an error occurs during column mapping or transformation."""
    pass

class SchemaValidationError(ETLError):
    """Raised when the final output data does not match the target schema."""
    pass

class DataLoadError(ETLError):
    """Raised when structured data cannot be written to the output destination."""
    pass
