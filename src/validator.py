import pandas as pd
from typing import Dict, Any
from src.logger import get_logger
from src.exceptions import DataValidationError

logger = get_logger()

class Validator:
    def __init__(self, validation_rules: Dict[str, Any]):
        self.config = validation_rules
        self.required_columns = self.config.get("required_columns", [])
        self.year_range = self.config.get("year_range", None)
        self.year_column = self.config.get("year_column", "year")

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting data validation.")
        initial_count = len(df)
        
        missing_cols = [col for col in self.required_columns if col not in df.columns]
        if missing_cols:
            raise DataValidationError(f"Missing required columns in input data: {missing_cols}")
            
        df = df.dropna(subset=self.required_columns)
        new_count = len(df)
        if new_count < initial_count:
            logger.warning(f"Dropped {initial_count - new_count} rows due to missing values in required columns.")
            
        if self.year_range and self.year_column in df.columns:
            # Ensure the year column is numeric (bad strings become NaN)
            df[self.year_column] = pd.to_numeric(df[self.year_column], errors='coerce')
            df = df.dropna(subset=[self.year_column])
            
            min_year = self.year_range.get("min")
            max_year = self.year_range.get("max")
            
            if min_year is not None and max_year is not None:
                valid_years = df[self.year_column].between(min_year, max_year)
                df = df[valid_years]
                filtered_count = len(df)
                if filtered_count < new_count:
                    logger.info(f"Filtered {new_count - filtered_count} rows outside the year range {min_year}-{max_year}.")
                    
        logger.info(f"Data validation complete. Valid rows remaining: {len(df)}")
        return df
