import pandas as pd
from pathlib import Path
from loguru import logger


def generate_day2_data(day1_path: str, day2_path: str) -> None:
    """
    Generate day2 snapshot by modifying day1 data.
    Simulates real-world data arrival for incremental load testing.
    
    Changes made:
    - Added 3 new game records with future dates
    - Added new player appearances for new games
    - Updated 2 player club transfers (for SCD Type 2 testing)
    - Updated 3 player market values
    - Added is_deleted flag (soft delete simulation)
    
    Args:
        day1_path: Path to day1 CSV files
        day2_path: Path to save day2 CSV files
    """
    # create day2 folder if it doesn't exist
    Path(day2_path).mkdir(parents=True, exist_ok=True)
    
    # load all day1 CSVs into dataframes
    players = pd.read_csv(f"{day1_path}/players.csv", low_memory=False)
    games = pd.read_csv(f"{day1_path}/games.csv", low_memory=False)
    appearances = pd.read_csv(f"{day1_path}/appearances.csv", low_memory=False)
    clubs = pd.read_csv(f"{day1_path}/clubs.csv", low_memory=False)
    competitions = pd.read_csv(f"{day1_path}/competitions.csv", low_memory=False)
    player_valuations = pd.read_csv(f"{day1_path}/player_valuations.csv", low_memory=False)
    
    # ---- GAMES: add 3 new game records ----
    # simulates new matches being played after day1
    # using future dates and fake game_ids to avoid conflicts with existing data
    new_games = pd.DataFrame({
        "game_id": [9999991, 9999992, 9999993],       # fake IDs that don't exist in day1
        "competition_id": ["GB1", "ES1", "L1"],        # Premier League, La Liga, Bundesliga
        "season": [2025, 2025, 2025],
        "round": ["Matchday 38", "Matchday 38", "Matchday 34"],
        "date": ["2026-05-20", "2026-05-21", "2026-05-22"],  # future dates
        "home_club_id": [631, 418, 16],                # Chelsea, Real Madrid, Borussia Dortmund
        "away_club_id": [11, 131, 27],                 # Arsenal, Barcelona, Bayern Munich
        "home_club_goals": [2, 1, 3],
        "away_club_goals": [1, 1, 0],
        "home_club_name": ["Chelsea", "Real Madrid", "Borussia Dortmund"],
        "away_club_name": ["Arsenal", "Barcelona", "Bayern Munich"],
        "attendance": [40000, 80000, 75000],
        "competition_type": ["domestic_league", "domestic_league", "domestic_league"]
    })
    # append new games to existing games
    games_day2 = pd.concat([games, new_games], ignore_index=True)
    
    # ---- APPEARANCES: add appearances for new games ----
    # each new game needs at least one appearance record
    # using real player_ids from day1 to maintain referential integrity
    new_appearances = pd.DataFrame({
        "appearance_id": ["app_9999991_1", "app_9999992_1", "app_9999993_1"],
        "game_id": [9999991, 9999992, 9999993],  # matches the new game_ids above
        "player_id": [
            players["player_id"].iloc[0],   # first player in dataset
            players["player_id"].iloc[1],   # second player
            players["player_id"].iloc[2]    # third player
        ],
        "player_club_id": [631, 418, 16],
        "player_current_club_id": [631, 418, 16],
        "date": ["2026-05-20", "2026-05-21", "2026-05-22"],
        "player_name": [
            players["name"].iloc[0],
            players["name"].iloc[1],
            players["name"].iloc[2]
        ],
        "competition_id": ["GB1", "ES1", "L1"],
        "yellow_cards": [0, 1, 0],
        "red_cards": [0, 0, 0],
        "goals": [1, 0, 2],
        "assists": [0, 1, 1],
        "minutes_played": [90, 90, 90]
    })
    appearances_day2 = pd.concat([appearances, new_appearances], ignore_index=True)
    
    # ---- PLAYERS: simulate 2 club transfers (SCD Type 2 test) ----
    # these changes will trigger SCD Type 2 in the pipeline
    # old records will be closed, new records inserted with updated club
    players_day2 = players.copy()
    
    # player at index 10 transfers to Chelsea
    players_day2.loc[players_day2["player_id"] == players["player_id"].iloc[10],
                     "current_club_id"] = 631  # Chelsea
    
    # player at index 20 transfers to Real Madrid
    players_day2.loc[players_day2["player_id"] == players["player_id"].iloc[20],
                     "current_club_id"] = 418  # Real Madrid
    
    # add soft delete flag to all players - False by default
    # simulates a player being removed from the dataset (e.g. retired)
    players_day2["is_deleted"] = False
    players_day2.loc[players_day2["player_id"] == players["player_id"].iloc[50],
                     "is_deleted"] = True  # this player is soft deleted
    
    # ---- PLAYER VALUATIONS: add 3 new valuation records ----
    # simulates market value updates - common in football datasets
    # these are NEW rows, not updates to existing ones
    player_valuations_day2 = player_valuations.copy()
    
    new_valuations = pd.DataFrame({
        "player_id": [
            players["player_id"].iloc[0],
            players["player_id"].iloc[1],
            players["player_id"].iloc[2]
        ],
        "date": ["2026-05-19", "2026-05-19", "2026-05-19"],  # today's date
        "market_value_in_eur": [150000000, 120000000, 90000000],  # updated values
        "current_club_name": [
            players["current_club_name"].iloc[0],
            players["current_club_name"].iloc[1],
            players["current_club_name"].iloc[2]
        ],
        "current_club_id": [
            players["current_club_id"].iloc[0],
            players["current_club_id"].iloc[1],
            players["current_club_id"].iloc[2]
        ],
        "player_club_domestic_competition_id": [
            players["current_club_domestic_competition_id"].iloc[0],
            players["current_club_domestic_competition_id"].iloc[1],
            players["current_club_domestic_competition_id"].iloc[2]
        ]
    })
    # append new valuations to existing ones - don't overwrite
    player_valuations_day2 = pd.concat([player_valuations, new_valuations], ignore_index=True)
    
    # ---- SAVE ALL DAY2 FILES ----
    # save modified tables as new CSVs in day2 folder
    games_day2.to_csv(f"{day2_path}/games.csv", index=False)
    appearances_day2.to_csv(f"{day2_path}/appearances.csv", index=False)
    players_day2.to_csv(f"{day2_path}/players.csv", index=False)
    player_valuations_day2.to_csv(f"{day2_path}/player_valuations.csv", index=False)
    
    # clubs and competitions don't change between day1 and day2 - copy as is
    clubs.to_csv(f"{day2_path}/clubs.csv", index=False)
    competitions.to_csv(f"{day2_path}/competitions.csv", index=False)
    
    logger.info(f"Day2 data generated successfully in {day2_path}")
    logger.info(f"New games: {len(new_games)}")
    logger.info(f"New appearances: {len(new_appearances)}")
    logger.info(f"Updated player transfers: 2")
    logger.info(f"New valuations: {len(new_valuations)}")


if __name__ == "__main__":
    # run this script directly to generate day2 data
    # python src/ingestion/generate_day2.py
    generate_day2_data("data/day1", "data/day2")