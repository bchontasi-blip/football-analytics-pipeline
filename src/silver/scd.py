import pandas as pd
from pathlib import Path
from loguru import logger
from datetime import date


def apply_scd_type2(df_existing: pd.DataFrame, df_new: pd.DataFrame, 
                     key_column: str, tracked_columns: list) -> pd.DataFrame:
    """
    Apply SCD Type 2 logic to a dimension table.
    - New records get inserted with is_current=True
    - Changed records get old record closed (end_date set, is_current=False)
    - Unchanged records stay as they are
    
    Args:
        df_existing: Current state of the dimension table (from day1)
        df_new: New incoming data (from day2)
        key_column: Primary key column e.g. 'player_id'
        tracked_columns: Columns to track for changes e.g. ['current_club_id', 'position']
        
    Returns:
        Updated dimension table with full SCD Type 2 history
    """
    today = pd.Timestamp(date.today())
    
    # if no existing data, this is the first load
    if df_existing is None or len(df_existing) == 0:
        df_new["effective_date"] = today
        df_new["end_date"] = pd.NaT      # null = still current
        df_new["is_current"] = True
        logger.info(f"First load: {len(df_new)} records inserted with is_current=True")
        return df_new
    
    # ensure SCD columns exist in existing data
    if "effective_date" not in df_existing.columns:
        df_existing["effective_date"] = today
        df_existing["end_date"] = pd.NaT
        df_existing["is_current"] = True
    
    # get current records only for comparison
    current_records = df_existing[df_existing["is_current"] == True].copy()
    
    # merge new data with current records to find changes
    merged = df_new.merge(
        current_records[[key_column] + tracked_columns],
        on=key_column,
        how="left",
        suffixes=("_new", "_existing")
    )
    
    # identify changed records - any tracked column has a different value
    changed_mask = pd.Series([False] * len(merged))
    for col in tracked_columns:
        new_col = f"{col}_new" if f"{col}_new" in merged.columns else col
        existing_col = f"{col}_existing" if f"{col}_existing" in merged.columns else col
        if new_col in merged.columns and existing_col in merged.columns:
            changed_mask = changed_mask | (merged[new_col] != merged[existing_col])
    
    # identify new records - key doesn't exist in current records
    existing_keys = set(current_records[key_column].astype(str))
    new_keys = set(df_new[key_column].astype(str))
    truly_new_keys = new_keys - existing_keys
    
    # close old records for changed keys
    changed_keys = set(merged[changed_mask][key_column].astype(str))
    df_existing.loc[
        (df_existing[key_column].astype(str).isin(changed_keys)) & 
        (df_existing["is_current"] == True),
        ["end_date", "is_current"]
    ] = [today, False]
    
    # prepare new/changed records to insert
    records_to_insert = df_new[
        df_new[key_column].astype(str).isin(changed_keys | truly_new_keys)
    ].copy()
    records_to_insert["effective_date"] = today
    records_to_insert["end_date"] = pd.NaT
    records_to_insert["is_current"] = True
    
    # combine existing (with closed records) and new inserts
    result = pd.concat([df_existing, records_to_insert], ignore_index=True)
    
    logger.info(f"SCD Type 2 complete: {len(truly_new_keys)} new, {len(changed_keys)} changed records")
    return result


def run_scd(config: dict, df_existing: pd.DataFrame, 
            df_new: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Run SCD Type 2 for a specific table.
    
    Args:
        config: Pipeline configuration dictionary
        df_existing: Existing dimension table
        df_new: New incoming data
        table_name: Name of the table e.g. 'players'
        
    Returns:
        Updated dimension table with SCD Type 2 history
    """
    scd_config = config["scd"]
    
    # check if this table needs SCD Type 2
    if table_name not in scd_config["tables"]:
        logger.info(f"{table_name} does not require SCD Type 2")
        return df_new
    
    # define which columns to track per table
    tracked_columns = {
        "players": ["current_club_id", "position", "market_value_in_eur"],
        "player_valuations": ["market_value_in_eur", "current_club_id"]
    }
    
    if table_name not in tracked_columns:
        logger.warning(f"No tracked columns defined for {table_name}")
        return df_new
    
    result = apply_scd_type2(
        df_existing=df_existing,
        df_new=df_new,
        key_column="player_id",
        tracked_columns=tracked_columns[table_name]
    )
    
    logger.info(f"SCD Type 2 applied to {table_name}. Total records: {len(result)}")
    return result