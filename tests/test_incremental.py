import pytest
import pandas as pd
from src.silver.scd import apply_scd_type2


# ---- INCREMENTAL LOAD TESTS ----
# These tests verify that the pipeline only processes new/changed records
# and correctly handles SCD Type 2 history tracking


def test_scd_first_load():
    """
    Test SCD Type 2 first load behaviour.
    When there is no existing data, all records should be inserted as current.
    """
    # ARRANGE - no existing data (first load), new data arriving
    df_existing = None  # simulates empty table on first run
    
    df_new = pd.DataFrame({
        "player_id": [1, 2, 3],
        "name": ["Messi", "Ronaldo", "Mbappe"],
        "current_club_id": [131, 418, 631],  # Barcelona, Real Madrid, Chelsea
        "position": ["Forward", "Forward", "Forward"]
    })
    
    # ACT - run SCD Type 2 for first time
    result = apply_scd_type2(
        df_existing=df_existing,
        df_new=df_new,
        key_column="player_id",
        tracked_columns=["current_club_id", "position"]
    )
    
    # ASSERT - all records should be current with no end date
    assert len(result) == 3                          # all 3 players inserted
    assert result["is_current"].all() == True        # all are current
    assert result["end_date"].isna().all() == True   # no end dates yet
    assert "effective_date" in result.columns        # effective date added


def test_scd_detects_changed_records():
    """
    Test SCD Type 2 detects when a tracked column changes.
    Changed records should get a new row and old row should be closed.
    """
    # ARRANGE - existing data from day1
    today = pd.Timestamp.now()
    
    df_existing = pd.DataFrame({
        "player_id": [1, 2],
        "name": ["Messi", "Ronaldo"],
        "current_club_id": [131, 418],   # Messi at Barcelona, Ronaldo at Real Madrid
        "position": ["Forward", "Forward"],
        "effective_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
        "end_date": [pd.NaT, pd.NaT],    # both currently active
        "is_current": [True, True]
    })
    
    # day2 - Messi transfers to PSG (current_club_id changes)
    df_new = pd.DataFrame({
        "player_id": [1, 2],
        "name": ["Messi", "Ronaldo"],
        "current_club_id": [583, 418],   # Messi now at PSG (583), Ronaldo unchanged
        "position": ["Forward", "Forward"]
    })
    
    # ACT
    result = apply_scd_type2(
        df_existing=df_existing,
        df_new=df_new,
        key_column="player_id",
        tracked_columns=["current_club_id", "position"]
    )
    
    # ASSERT
    # should have 3 rows: Messi old, Messi new, Ronaldo unchanged
    assert len(result) == 3
    
    # old Messi record should be closed
    messi_old = result[
        (result["player_id"] == 1) & (result["is_current"] == False)
    ]
    assert len(messi_old) == 1                        # one closed record
    assert messi_old["end_date"].notna().all()        # has an end date
    assert messi_old["current_club_id"].iloc[0] == 131  # still shows Barcelona
    
    # new Messi record should be current with PSG
    messi_new = result[
        (result["player_id"] == 1) & (result["is_current"] == True)
    ]
    assert len(messi_new) == 1                          # one current record
    assert messi_new["current_club_id"].iloc[0] == 583  # now at PSG
    assert messi_new["end_date"].isna().all()            # no end date
    
    # Ronaldo should be unchanged - still one current record
    ronaldo = result[result["player_id"] == 2]
    assert len(ronaldo) == 1              # no new record created
    assert ronaldo["is_current"].iloc[0] == True  # still current


def test_scd_handles_new_records():
    """
    Test SCD Type 2 correctly inserts brand new records.
    New players that didn't exist in day1 should be inserted as current.
    """
    # ARRANGE - existing data has 2 players
    df_existing = pd.DataFrame({
        "player_id": [1, 2],
        "name": ["Messi", "Ronaldo"],
        "current_club_id": [131, 418],
        "position": ["Forward", "Forward"],
        "effective_date": [pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-01")],
        "end_date": [pd.NaT, pd.NaT],
        "is_current": [True, True]
    })
    
    # day2 - new player Mbappe arrives
    df_new = pd.DataFrame({
        "player_id": [1, 2, 3],          # player 3 is new
        "name": ["Messi", "Ronaldo", "Mbappe"],
        "current_club_id": [131, 418, 631],  # Mbappe at Chelsea
        "position": ["Forward", "Forward", "Forward"]
    })
    
    # ACT
    result = apply_scd_type2(
        df_existing=df_existing,
        df_new=df_new,
        key_column="player_id",
        tracked_columns=["current_club_id", "position"]
    )
    
    # ASSERT
    # should have 3 rows - 2 existing + 1 new
    assert len(result) == 3
    
    # new player Mbappe should be current
    mbappe = result[result["player_id"] == 3]
    assert len(mbappe) == 1
    assert mbappe["is_current"].iloc[0] == True
    assert mbappe["end_date"].isna().all()
    assert mbappe["current_club_id"].iloc[0] == 631  # at Chelsea


def test_scd_unchanged_records_not_duplicated():
    """
    Test that unchanged records are not duplicated.
    If nothing changes for a player, they should still have only one record.
    """
    # ARRANGE - existing data
    df_existing = pd.DataFrame({
        "player_id": [1],
        "name": ["Messi"],
        "current_club_id": [131],        # Barcelona
        "position": ["Forward"],
        "effective_date": [pd.Timestamp("2024-01-01")],
        "end_date": [pd.NaT],
        "is_current": [True]
    })
    
    # day2 - nothing changed for Messi
    df_new = pd.DataFrame({
        "player_id": [1],
        "name": ["Messi"],
        "current_club_id": [131],        # still at Barcelona
        "position": ["Forward"]          # same position
    })
    
    # ACT
    result = apply_scd_type2(
        df_existing=df_existing,
        df_new=df_new,
        key_column="player_id",
        tracked_columns=["current_club_id", "position"]
    )
    
    # ASSERT - still only 1 record, not duplicated
    assert len(result) == 1
    assert result["is_current"].iloc[0] == True
    assert result["end_date"].isna().all()