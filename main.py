import argparse
import sys

from src.logger import setup_logger
from src.config import PipelineConfig
from src.extractor import Extractor
from src.validator import Validator
from src.mapper import Mapper
from src.transformer import Transformer
from src.loader import Loader
from src.exceptions import ETLError

def main():
    parser = argparse.ArgumentParser(
        description="CLEWS-OGCore Production ETL Pipeline",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input", required=True, help="Path to input CSV file or directory")
    parser.add_argument("--output", required=True, help="Path to output JSON file")
    parser.add_argument("--config", required=True, help="Path to YAML configuration file")
    parser.add_argument("--log-level", default="INFO", 
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"], 
                        help="Logging verbosity")
    
    args = parser.parse_args()
    
    logger = setup_logger(args.log_level)
    
    logger.info("="*50)
    logger.info("Initializing CLEWS-OGCore Production ETL Pipeline")
    logger.info("="*50)
    
    try:
        logger.info("Loading configuration...")
        config = PipelineConfig(args.config)
        
        extractor = Extractor(args.input)
        df_raw = extractor.extract()
        
        validator = Validator(config.get_validation_rules())
        df_valid = validator.validate(df_raw)
        
        mapper = Mapper(config.get_mapping_rules())
        df_mapped = mapper.map_columns(df_valid)
        
        transformer = Transformer(config.get_transformation_rules())
        df_transformed = transformer.transform(df_mapped)
        
        records = df_transformed.to_dict(orient="records")
        
        loader = Loader(args.output)
        loader.load(records)
        
        logger.info("="*50)
        logger.info("ETL Pipeline Completed Successfully!")
        logger.info("="*50)
        
    except ETLError as etl_err:
        logger.error(f"Pipeline Execution Aborted: {etl_err}")
        if args.log_level == "DEBUG":
            logger.exception("Full traceback for debugging:")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Critical System Failure: {e}")
        logger.exception("Full traceback for debugging:")
        sys.exit(1)

if __name__ == "__main__":
    main()
