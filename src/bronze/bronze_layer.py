import pandas as pd
from pathlib import Path
from loguru import logger


def read_bronze_table(table_name: str, bronze_path: str) -> pd.DataFrame:
    """
    Read a parquet file from the Bronze layer.
    
    Args:
        table_name: Name of the table (e.g. 'players')
        bronze_path: Path to the bronze folder
        
    Returns:
        DataFrame with the raw bronze data
    """
    # build path to parquet file e.g. data/bronze/players.parquet
    parquet_file = Path(bronze_path) / f"{table_name}.parquet"
    
    # stop early if bronze file doesn't exist - ingestion must run first
    if not parquet_file.exists():
        logger.error(f"Bronze file not found: {parquet_file}")
        raise FileNotFoundError(f"Bronze file not found: {parquet_file}")
    
    logger.info(f"Reading {table_name} from Bronze layer")
    df = pd.read_parquet(parquet_file)  # read parquet back into a dataframe
    
    logger.info(f"Loaded {len(df)} rows from {parquet_file}")
    return df  # Silver layer will use this dataframe


def validate_bronze_schema(df: pd.DataFrame, table_name: str) -> bool:
    """
    Validate that expected columns exist in the Bronze table.
    Catches schema drift early before Silver transformation.
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table for logging
        
    Returns:
        True if schema is valid, False otherwise
    """
    # define minimum required columns per table
    # if source data changes and drops a column, we catch it here (schema drift)
    expected_columns = {
        "players": ["player_id", "name", "position", "current_club_id"],
        "games": ["game_id", "competition_id", "season", "date", "home_club_id", "away_club_id", "home_club_goals", "away_club_goals"],
        "appearances": ["appearance_id", "game_id", "player_id", "minutes_played", "goals", "assists"],
        "clubs": ["club_id", "name"],
        "competitions": ["competition_id", "name"],
        "player_valuations": ["player_id", "date", "market_value_in_eur"]
    }
    
    if table_name not in expected_columns:
        logger.warning(f"No schema defined for table: {table_name}")
        return True  # unknown tables pass through - don't block the pipeline
    
    required = set(expected_columns[table_name])  # columns we expect
    actual = set(df.columns)                       # columns we actually have
    missing = required - actual                    # columns that are missing
    
    if missing:
        # schema drift detected - source changed its structure
        logger.error(f"Schema drift detected in {table_name}. Missing columns: {missing}")
        return False
    
    logger.info(f"Schema validation passed for {table_name}")
    return True  # all required columns present, safe to proceed to Silver