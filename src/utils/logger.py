import sys
from pathlib import Path
from loguru import logger


def setup_logger(log_dir: str = "logs", log_level: str = "INFO") -> None:
    """
    Configure loguru logger for the pipeline.
    Writes logs to both console and a log file.
    
    Args:
        log_dir: Directory where log files will be saved
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    # create logs directory if it doesn't exist
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # remove default loguru handler
    logger.remove()
    
    # add console handler - shows logs in the terminal
    logger.add(
        sys.stdout,
        level=log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <cyan>{module}</cyan> | {message}"
    )
    
    # add file handler - saves logs to a file per day
    logger.add(
        f"{log_dir}/pipeline_{{time:YYYY-MM-DD}}.log",
        level=log_level,
        rotation="1 day",    # new log file every day
        retention="7 days",  # keep logs for 7 days, then delete
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module} | {message}"
    )