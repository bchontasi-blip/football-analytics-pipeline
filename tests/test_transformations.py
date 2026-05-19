import pytest
import pandas as pd
from src.silver.silver_layer import clean_dataframe, build_dim_players, build_fact_games

# ---- HOW PYTEST WORKS ----
# pytest finds all functions starting with "test_" and runs them
# each test creates fake data, runs a function, and checks the result with "assert"
# if assert is True → test passes ✅
# if assert is False → test fails ❌


def test_clean_dataframe_removes_duplicates():
    """Test that clean_dataframe removes duplicate rows"""
    # ARRANGE - create fake input data with a duplicate row
    df = pd.DataFrame({
        "player_id": [1, 1, 2],
        "name": ["Messi", "Messi", "Ronaldo"]  # row 0 and 1 are duplicates
    })
    
    # ACT - run the function we want to test
    result = clean_dataframe(df, "test_table")
    
    # ASSERT - check the result is what we expect
    assert len(result) == 2  # should have 2 rows after dedup, not 3


def test_clean_dataframe_strips_whitespace():
    """Test that clean_dataframe strips whitespace from string columns"""
    # ARRANGE - create data with extra whitespace
    df = pd.DataFrame({
        "player_id": [1, 2],
        "name": ["  Messi  ", "  Ronaldo  "],      # whitespace around names
        "position": ["  Forward  ", "  Forward  "]  # whitespace around positions
    })
    
    # ACT
    result = clean_dataframe(df, "test_table")
    
    # ASSERT - whitespace should be removed
    assert result["name"].iloc[0] == "Messi"      # not "  Messi  "
    assert result["name"].iloc[1] == "Ronaldo"
    assert result["position"].iloc[0] == "Forward" # not "  Forward  "


def test_clean_dataframe_removes_empty_rows():
    """Test that clean_dataframe removes rows where all values are null"""
    # ARRANGE - middle row is completely empty
    df = pd.DataFrame({
        "player_id": [1, None, 2],
        "name": ["Messi", None, "Ronaldo"]  # row 1 has all nulls
    })
    
    # ACT
    result = clean_dataframe(df, "test_table")
    
    # ASSERT - empty row should be removed
    assert len(result) == 2  # should have 2 rows, not 3


def test_build_fact_games_match_outcome():
    """Test that match outcome is correctly derived from scores"""
    # ARRANGE - 3 games: home win, away win, draw
    df = pd.DataFrame({
        "game_id": [1, 2, 3],
        "competition_id": ["GB1", "GB1", "GB1"],
        "season": [2025, 2025, 2025],
        "date": ["2026-01-01", "2026-01-02", "2026-01-03"],
        "home_club_id": [631, 631, 631],
        "away_club_id": [11, 11, 11],
        "home_club_goals": [2, 0, 1],  # 2-1 win, 0-2 loss, 1-1 draw
        "away_club_goals": [1, 2, 1],
        "_ingested_at": [pd.Timestamp.now()] * 3,
        "_source_file": ["test"] * 3
    })
    
    # ACT
    result = build_fact_games(df)
    
    # ASSERT - check each match outcome is correctly derived
    assert result["match_outcome"].iloc[0] == "home_win"   # 2-1 → home wins
    assert result["match_outcome"].iloc[1] == "away_win"   # 0-2 → away wins
    assert result["match_outcome"].iloc[2] == "draw"       # 1-1 → draw


def test_build_fact_games_season_year():
    """Test that season year is correctly extracted from date"""
    # ARRANGE - one game with a known date
    df = pd.DataFrame({
        "game_id": [1],
        "competition_id": ["GB1"],
        "season": [2025],
        "date": ["2026-05-18"],  # year is 2026
        "home_club_id": [631],
        "away_club_id": [11],
        "home_club_goals": [1],
        "away_club_goals": [0],
        "_ingested_at": [pd.Timestamp.now()],
        "_source_file": ["test"]
    })
    
    # ACT
    result = build_fact_games(df)
    
      
    # ASSERT - season year should be extracted from date
    assert result["season_year"].iloc[0] == 2026  # extracted from "2026-05-18"