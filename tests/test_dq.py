import pytest
import pandas as pd
from src.dq.data_quality import check_nulls, check_valid_ranges, check_referential_integrity, check_duplicates


def test_check_nulls_passes_when_no_nulls():
    """Test that null check passes when no nulls exist"""
    # ARRANGE - clean data with no nulls
    df = pd.DataFrame({
        "player_id": [1, 2, 3],
        "game_id": [10, 20, 30]
    })
    
    # ACT
    result = check_nulls(df, "appearances", ["player_id", "game_id"])
    
    # ASSERT - both columns should pass
    assert result["player_id"]["passed"] == True
    assert result["game_id"]["passed"] == True
    assert result["player_id"]["null_count"] == 0


def test_check_nulls_fails_when_nulls_exist():
    """Test that null check fails when nulls are found"""
    # ARRANGE - player_id has a null
    df = pd.DataFrame({
        "player_id": [1, None, 3],  # null in row 1
        "game_id": [10, 20, 30]
    })
    
    # ACT
    result = check_nulls(df, "appearances", ["player_id", "game_id"])
    
    # ASSERT - player_id should fail, game_id should pass
    assert result["player_id"]["passed"] == False
    assert result["player_id"]["null_count"] == 1
    assert result["game_id"]["passed"] == True


def test_check_valid_ranges_passes_when_valid():
    """Test that range check passes when all values are within range"""
    # ARRANGE - all values are valid (>= 0)
    df = pd.DataFrame({
        "goals": [0, 1, 2, 3],
        "minutes_played": [45, 90, 60, 90]
    })
    
    range_config = {
        "goals": {"min": 0},
        "minutes_played": {"min": 0}
    }
    
    # ACT
    result = check_valid_ranges(df, "appearances", range_config)
    
    # ASSERT
    assert result["goals"]["passed"] == True
    assert result["minutes_played"]["passed"] == True


def test_check_valid_ranges_fails_when_invalid():
    """Test that range check fails when values are out of range"""
    # ARRANGE - negative goals and minutes
    df = pd.DataFrame({
        "goals": [1, -1, 2],        # -1 is invalid
        "minutes_played": [90, -5, 45]  # -5 is invalid
    })
    
    range_config = {
        "goals": {"min": 0},
        "minutes_played": {"min": 0}
    }
    
    # ACT
    result = check_valid_ranges(df, "appearances", range_config)
    
    # ASSERT
    assert result["goals"]["passed"] == False
    assert result["goals"]["failed_count"] == 1
    assert result["minutes_played"]["passed"] == False
    assert result["minutes_played"]["failed_count"] == 1


def test_check_referential_integrity_passes():
    """Test that referential integrity passes when all keys exist in parent"""
    # ARRANGE - all player_ids in appearances exist in players
    df_appearances = pd.DataFrame({"player_id": [1, 2, 3]})
    df_players = pd.DataFrame({"player_id": [1, 2, 3, 4, 5]})  # parent has more keys
    
    # ACT
    result = check_referential_integrity(
        df_appearances, df_players, "player_id", "player_id", "appearances"
    )
    
    # ASSERT
    assert result["passed"] == True
    assert result["orphaned_count"] == 0


def test_check_referential_integrity_fails():
    """Test that referential integrity fails when orphaned keys exist"""
    # ARRANGE - player_id 999 doesn't exist in players
    df_appearances = pd.DataFrame({"player_id": [1, 2, 999]})  # 999 is orphaned
    df_players = pd.DataFrame({"player_id": [1, 2, 3]})
    
    # ACT
    result = check_referential_integrity(
        df_appearances, df_players, "player_id", "player_id", "appearances"
    )
    
    # ASSERT
    assert result["passed"] == False
    assert result["orphaned_count"] == 1


def test_check_duplicates_passes():
    """Test that duplicate check passes when no duplicates exist"""
    # ARRANGE - unique appearance_ids
    df = pd.DataFrame({
        "appearance_id": ["app_1", "app_2", "app_3"],
        "player_id": [1, 2, 3]
    })
    
    # ACT
    result = check_duplicates(df, "appearances", ["appearance_id"])
    
    # ASSERT
    assert result["passed"] == True
    assert result["duplicate_count"] == 0


def test_check_duplicates_fails():
    """Test that duplicate check fails when duplicates exist"""
    # ARRANGE - app_1 appears twice
    df = pd.DataFrame({
        "appearance_id": ["app_1", "app_1", "app_3"],  # app_1 is duplicated
        "player_id": [1, 1, 3]
    })
    
    # ACT
    result = check_duplicates(df, "appearances", ["appearance_id"])
    
    # ASSERT
    assert result["passed"] == False
    assert result["duplicate_count"] == 1