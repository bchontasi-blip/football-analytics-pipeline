import pandas as pd
from pathlib import Path
from loguru import logger
from src.utils.config_loader import load_config
from src.bronze.bronze_layer import read_bronze_table


def clean_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Apply basic cleaning to any dataframe:
    - Remove duplicate rows
    - Strip whitespace from string columns
    - Drop rows where all values are null
    
    Args:
        df: Raw dataframe from Bronze layer
        table_name: Name of the table for logging
        
    Returns:
        Cleaned dataframe
    """
    initial_rows = len(df)
    
    # remove fully duplicate rows
    df = df.drop_duplicates()
    
    # strip whitespace from all string columns
    string_cols = df.select_dtypes(include="object").columns
    df[string_cols] = df[string_cols].apply(lambda x: x.str.strip())
    
    # drop rows where ALL values are null
    df = df.dropna(how="all")
    
    removed = initial_rows - len(df)
    logger.info(f"{table_name}: removed {removed} rows during cleaning. {len(df)} rows remaining.")
    
    return df

def build_dim_players(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Build the players dimension table from Bronze data.
    Applies position normalisation and selects relevant columns.
    
    Args:
        df: Raw players dataframe from Bronze
        config: Pipeline configuration dictionary
        
    Returns:
        Clean dim_players dataframe ready for Silver layer
    """
    # clean basic data first
    df = clean_dataframe(df, "players")
    
    # normalise position labels using mapping from config
    position_mapping = config["transformations"]["position_mapping"]
    df["position"] = df["position"].map(position_mapping).fillna("Unknown")
    
    # select only columns we need for the dimension table
    dim_players = df[[
        "player_id",
        "name",
        "position",
        "country_of_birth",
        "date_of_birth",
        "current_club_id",
        "market_value_in_eur",
        "height_in_cm",
        "foot",
        "_ingested_at"
    ]].copy()
    
    # cast types
    dim_players["player_id"] = dim_players["player_id"].astype(str)
    dim_players["current_club_id"] = dim_players["current_club_id"].astype(str)
    dim_players["date_of_birth"] = pd.to_datetime(dim_players["date_of_birth"], errors="coerce")
    
    logger.info(f"Built dim_players with {len(dim_players)} rows")
    return dim_players

def build_dim_clubs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the clubs dimension table from Bronze data.
    
    Args:
        df: Raw clubs dataframe from Bronze
        
    Returns:
        Clean dim_clubs dataframe
    """
    df = clean_dataframe(df, "clubs")
    
    dim_clubs = df[[
        "club_id",
        "name",
        "domestic_competition_id",
        "squad_size",
        "stadium_name",
        "coach_name",
        "_ingested_at"
    ]].copy()
    
    # cast types
    dim_clubs["club_id"] = dim_clubs["club_id"].astype(str)
    dim_clubs["domestic_competition_id"] = dim_clubs["domestic_competition_id"].astype(str)
    
    logger.info(f"Built dim_clubs with {len(dim_clubs)} rows")
    return dim_clubs


def build_dim_competitions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build the competitions dimension table from Bronze data.
    
    Args:
        df: Raw competitions dataframe from Bronze
        
    Returns:
        Clean dim_competitions dataframe
    """
    df = clean_dataframe(df, "competitions")
    
    dim_competitions = df[[
        "competition_id",
        "name",
        "country_name",
        "type",
        "confederation",
        "_ingested_at"
    ]].copy()
    
    dim_competitions["competition_id"] = dim_competitions["competition_id"].astype(str)
    
    logger.info(f"Built dim_competitions with {len(dim_competitions)} rows")
    return dim_competitions

def build_dim_date(df_games: pd.DataFrame) -> pd.DataFrame:
    """
    Build the date dimension table from games dates.
    Generated from match dates - not from a source CSV.
    
    Args:
        df_games: Raw games dataframe from Bronze
        
    Returns:
        dim_date dataframe with date attributes
    """
    # extract all unique dates from games
    dates = pd.to_datetime(df_games["date"], errors="coerce").dropna().unique()
    
    dim_date = pd.DataFrame({"date": dates})
    
    # derive date attributes - all calculated from the date itself
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["day_of_week"] = dim_date["date"].dt.day_name()   # e.g. "Monday"
    dim_date["month_name"] = dim_date["date"].dt.month_name()  # e.g. "January"
    dim_date["quarter"] = dim_date["date"].dt.quarter          # 1, 2, 3, or 4
    dim_date["is_weekend"] = dim_date["date"].dt.dayofweek >= 5  # True if Saturday or Sunday
    dim_date["season"] = dim_date["year"].apply(
        lambda y: f"{y}/{str(y+1)[-2:]}"  # e.g. 2023 → "2023/24"
    )
    
    dim_date = dim_date.sort_values("date").reset_index(drop=True)
    
    logger.info(f"Built dim_date with {len(dim_date)} rows")
    return dim_date
