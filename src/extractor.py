import os
import pandas as pd
from pathlib import Path
from src.logger import get_logger
from src.exceptions import DataExtractionError

logger = get_logger()

class Extractor:
    def __init__(self, input_path: str):
        self.input_path = input_path

    def extract(self) -> pd.DataFrame:
        logger.info(f"Starting extraction from: {self.input_path}")
        
        path = Path(self.input_path)
        
        if not path.exists():
            raise DataExtractionError(f"Input path does not exist: {self.input_path}")

        if path.is_file():
            if path.suffix.lower() == '.csv':
                try:
                    df = pd.read_csv(self.input_path)
                    logger.info(f"Extracted {len(df)} records from {path.name}")
                    return df
                except Exception as e:
                    raise DataExtractionError(f"Failed parsing CSV {path.name}: {e}")
            else:
                raise DataExtractionError("Input file must be a .csv file.")
                
        elif path.is_dir():
            csv_files = list(path.glob('*.csv'))
            if not csv_files:
                raise DataExtractionError("No CSV files found in directory.")
            
            dfs = []
            for filepath in csv_files:
                try:
                    df_part = pd.read_csv(filepath)
                    logger.info(f"Extracted {len(df_part)} records from {filepath.name}")
                    dfs.append(df_part)
                except Exception as e:
                    logger.warning(f"Failed to read {filepath.name}: {e}")
                    
            if not dfs:
                raise DataExtractionError("Could not extract any data from the provided directory.")
                
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info(f"Total extracted records from directory: {len(combined_df)}")
            return combined_df
        
        else:
            raise DataExtractionError(f"Unexpected path type: {self.input_path}")
