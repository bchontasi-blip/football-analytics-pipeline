import pandas as pd
from pathlib import Path
from loguru import logger
from src.utils.config_loader import load_config  # our custom config loader


def ingest_table(table_name: str, source_path: str, output_path: str) -> pd.DataFrame:
    """
    Read a single CSV file and save it as parquet (Bronze layer).
    Minimal transformation - just enforce schema and save.
    
    Args:
        table_name: Name of the table (e.g. 'players')
        source_path: Path to the CSV source folder
        output_path: Path to save the parquet file
        
    Returns:
        DataFrame with the raw data
    """
    # build the full path to the CSV file e.g. data/day1/players.csv
    csv_file = Path(source_path) / f"{table_name}.csv"
    
    # stop early if the file doesn't exist - better than a cryptic pandas error
    if not csv_file.exists():
        logger.error(f"Source file not found: {csv_file}")
        raise FileNotFoundError(f"Source file not found: {csv_file}")
    
    # read the CSV - low_memory=False prevents mixed type warnings on large files
    logger.info(f"Ingesting {table_name} from {csv_file}")
    df = pd.read_csv(csv_file, low_memory=False)
    
    # add metadata columns so we know when and where each record came from
    # useful for debugging and auditing in production
    df["_ingested_at"] = pd.Timestamp.now()  # timestamp of ingestion
    df["_source_file"] = str(csv_file)        # full path to source file
    
    # create bronze folder if it doesn't exist yet
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # save as parquet - columnar format, faster queries, smaller size than CSV
    parquet_file = output_dir / f"{table_name}.parquet"
    df.to_parquet(parquet_file, index=False)  # index=False - don't save the row numbers
    
    logger.info(f"Saved {len(df)} rows to {parquet_file}")
    return df  # return df so the next layer (Silver) can use it directly


def run_ingestion(config: dict, source_key: str = "data_day1") -> dict:
    """
    Run ingestion for all tables defined in config.
    
    Args:
        config: Pipeline configuration dictionary
        source_key: Which data source to use ('data_day1' or 'data_day2')
        
    Returns:
        Dictionary of {table_name: dataframe}
    """
    # read paths and table list from config - no hardcoded values
    source_path = config["paths"][source_key]   # e.g. "data/day1"
    bronze_path = config["paths"]["bronze"]      # e.g. "data/bronze"
    tables = config["ingestion"]["source_tables"] # e.g. ["players", "games", ...]
    
    dataframes = {}  # will store {table_name: dataframe} for all tables
    
    # loop through each table and ingest it
    for table in tables:
        df = ingest_table(table, source_path, bronze_path)
        dataframes[table] = df  # store result so Silver layer can access it
    
    logger.info(f"Ingestion complete. {len(dataframes)} tables ingested.")
    return dataframes  # e.g. {"players": df_players, "games": df_games, ...}