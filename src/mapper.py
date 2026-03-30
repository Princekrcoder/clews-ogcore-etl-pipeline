import pandas as pd
from typing import Dict
from src.logger import get_logger
from src.exceptions import MapTransformError

logger = get_logger()

class Mapper:
    def __init__(self, mapping_rules: Dict[str, str]):
        self.column_mapping = mapping_rules

    def map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.column_mapping:
            logger.warning("No column mapping defined in config. Proceeding without mapping.")
            return df
            
        logger.info(f"Applying column mappings: {self.column_mapping}")
        
        try:
            rename_dict = {
                src: dest for src, dest in self.column_mapping.items() if src in df.columns
            }
            
            df_mapped = df.rename(columns=rename_dict)
            logger.info("Column mapping complete.")
            return df_mapped
        except Exception as e:
            raise MapTransformError(f"Failed to map columns: {e}")
