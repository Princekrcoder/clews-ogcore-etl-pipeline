import logging
import sys
import os

def setup_logger(log_level_str="INFO"):
    """
    Sets up a logger that logs to both the console and a file named pipeline.log.
    
    Args:
        log_level_str (str): The string representation of the log level (e.g. 'INFO', 'DEBUG').
    """
    level = getattr(logging, log_level_str.upper(), logging.INFO)
    
    logger = logging.getLogger("etl_pipeline")
    logger.setLevel(level)
    logger.propagate = False
    
    # Avoid duplicate handlers if setup multiple times
    if not logger.handlers:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # File Handler
        try:
            fh = logging.FileHandler("pipeline.log", mode='w')
            fh.setLevel(level)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
        except Exception as e:
            print(f"Failed to set up file handler: {e}")

        # Console Handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger

logger = setup_logger()

def get_logger():
    """Returns the globally configured logger."""
    return logging.getLogger("etl_pipeline")
