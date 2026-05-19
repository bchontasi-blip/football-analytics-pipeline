import pandas as pd
from pathlib import Path
from loguru import logger


def build_player_performance(fact_appearances: pd.DataFrame) -> pd.DataFrame:
    """
    Build player performance aggregation table.
    Aggregates goals, assists and minutes played per player per season.
    
    Args:
        fact_appearances: Clean appearances fact table from Silver
        
    Returns:
        Player performance Gold table
    """
    # extract season year from date
    fact_appearances["season_year"] = pd.to_datetime(
        fact_appearances["date"], errors="coerce"
    ).dt.year
    
    # aggregate per player per season
    player_performance = fact_appearances.groupby(
        ["player_id", "season_year"]
    ).agg(
        total_goals=("goals", "sum"),
        total_assists=("assists", "sum"),
        total_minutes=("minutes_played", "sum"),
        matches_played=("appearance_id", "count")
    ).reset_index()
    
    logger.info(f"Built player_performance with {len(player_performance)} rows")
    return player_performance

def build_club_performance(fact_games: pd.DataFrame) -> pd.DataFrame:
    """
    Build club performance aggregation table.
    Aggregates wins, losses, draws and goals per club per season.
    
    Args:
        fact_games: Clean games fact table from Silver
        
    Returns:
        Club performance Gold table
    """
    # calculate home club stats
    home_stats = fact_games.groupby(
        ["home_club_id", "season_year"]
    ).agg(
        matches_played=("game_id", "count"),
        goals_scored=("home_club_goals", "sum"),
        wins=("match_outcome", lambda x: (x == "home_win").sum()),
        losses=("match_outcome", lambda x: (x == "away_win").sum()),
        draws=("match_outcome", lambda x: (x == "draw").sum())
    ).reset_index().rename(columns={"home_club_id": "club_id"})

    # calculate away club stats
    away_stats = fact_games.groupby(
        ["away_club_id", "season_year"]
    ).agg(
        matches_played=("game_id", "count"),
        goals_scored=("away_club_goals", "sum"),
        wins=("match_outcome", lambda x: (x == "away_win").sum()),
        losses=("match_outcome", lambda x: (x == "home_win").sum()),
        draws=("match_outcome", lambda x: (x == "draw").sum())
    ).reset_index().rename(columns={"away_club_id": "club_id"})

    # combine home and away stats
    club_performance = pd.concat([home_stats, away_stats]).groupby(
        ["club_id", "season_year"]
    ).agg(
        matches_played=("matches_played", "sum"),
        goals_scored=("goals_scored", "sum"),
        wins=("wins", "sum"),
        losses=("losses", "sum"),
        draws=("draws", "sum")
    ).reset_index()

    logger.info(f"Built club_performance with {len(club_performance)} rows")
    return club_performance

def build_player_valuation_trend(df_valuations: pd.DataFrame) -> pd.DataFrame:
    """
    Build player valuation trend table.
    Calculates rolling average market value per player over time.
    
    Args:
        df_valuations: Clean player_valuations dataframe from Silver
        
    Returns:
        Player valuation trend Gold table
    """
    df = df_valuations.copy()
    
    # ensure date is datetime and sort chronologically per player
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values(["player_id", "date"])
    
    # calculate rolling average market value per player (last 3 valuations)
    df["rolling_average"] = df.groupby("player_id")["market_value_in_eur"].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean()
    )
    
    # select final columns
    valuation_trend = df[[
        "player_id",
        "date",
        "market_value_in_eur",
        "rolling_average"
    ]].copy()
    
    logger.info(f"Built player_valuation_trend with {len(valuation_trend)} rows")
    return valuation_trend


def run_gold(config: dict, silver_tables: dict) -> dict:
    """
    Run all Gold aggregations.
    Builds all business-ready tables from Silver dataframes.
    
    Args:
        config: Pipeline configuration dictionary
        silver_tables: Dictionary of {table_name: dataframe} from Silver
        
    Returns:
        Dictionary of {table_name: dataframe} for all Gold tables
    """
    gold_path = config["paths"]["gold"]
    Path(gold_path).mkdir(parents=True, exist_ok=True)
    
    gold_tables = {}
    
    # build gold tables from silver
    gold_tables["player_performance"] = build_player_performance(
        silver_tables["fact_appearances"]
    )
    gold_tables["club_performance"] = build_club_performance(
        silver_tables["fact_games"]
    )
    gold_tables["player_valuation_trend"] = build_player_valuation_trend(
        silver_tables["player_valuations"]
    )
    
    # save all gold tables as parquet
    for table_name, df in gold_tables.items():
        output_file = Path(gold_path) / f"{table_name}.parquet"
        df.to_parquet(output_file, index=False)
        logger.info(f"Saved {table_name} to {output_file}")
    
    logger.info(f"Gold layer complete. {len(gold_tables)} tables saved.")
    return gold_tables