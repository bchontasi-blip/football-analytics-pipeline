import yaml  # reads the config.yaml file
from pathlib import Path  # handles file paths in a cross-platform way (Windows/Mac/Linux)
from loguru import logger  # structured logging


def load_config(config_path: str = "configs/config.yaml") -> dict:
    """
    Load the pipeline configuration from a YAML file.
    
    Args:
        config_path: Path to the config file. Defaults to configs/config.yaml
        
    Returns:
        Dictionary containing the full pipeline configuration
    """
    # convert the string path to a Path object so we can check if it exists
    config_file = Path(config_path)
    
    # if the config file doesn't exist, log the error and stop the pipeline
    if not config_file.exists():
        logger.error(f"Config file not found: {config_path}")
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    # open the file and parse the YAML into a Python dictionary
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)  # safe_load prevents execution of arbitrary code in YAML
    
    logger.info(f"Config loaded successfully from {config_path}")
    return config  # returns a dict, e.g. config["paths"]["bronze"] gives "data/bronze"