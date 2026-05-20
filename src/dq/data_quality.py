import pandas as pd
from pathlib import Path
from loguru import logger
import json
from datetime import datetime


def check_nulls(df: pd.DataFrame, table_name: str, columns: list) -> dict:
    """
    Check for null values in critical columns.
    Required by assessment section 6 - Null Checks.
    """
    results = {}
    
    for col in columns:
        # skip if column doesn't exist - avoid crashing the whole pipeline
        if col not in df.columns:
            logger.warning(f"{table_name}: column '{col}' not found for null check")
            continue
        
        # count how many nulls are in this column
        null_count = df[col].isnull().sum()
        passed = bool(null_count == 0)  # bool() ensures True/False not numpy bool
        
        # store result for the DQ report
        results[col] = {
            "passed": passed,
            "null_count": int(null_count),  # int() because numpy int is not JSON serializable
            "total_rows": len(df)
        }
        
        if not passed:
            logger.warning(f"{table_name}: {null_count} null values found in '{col}'")
        else:
            logger.info(f"{table_name}: null check passed for '{col}'")
    
    return results  # e.g. {"player_id": {"passed": True, "null_count": 0, "total_rows": 1000}}


def check_valid_ranges(df: pd.DataFrame, table_name: str, range_config: dict) -> dict:
    """
    Check that numeric columns are within valid ranges.
    Required by assessment section 6 - Valid Ranges (minutes_played >= 0, goals >= 0).
    """
    results = {}
    
    for col, rules in range_config.items():
        if col not in df.columns:
            logger.warning(f"{table_name}: column '{col}' not found for range check")
            continue
        
        # start with no failed rows
        failed_rows = pd.Series([False] * len(df))
        
        # mark rows that violate the min rule e.g. goals < 0
        if "min" in rules:
            failed_rows = failed_rows | (df[col] < rules["min"])
        # mark rows that violate the max rule e.g. goals > 100
        if "max" in rules:
            failed_rows = failed_rows | (df[col] > rules["max"])
        
        failed_count = failed_rows.sum()
        passed = bool(failed_count == 0)  # bool() ensures True/False not numpy bool
        
        results[col] = {
            "passed": passed,
            "failed_count": int(failed_count),
            "total_rows": len(df)
        }
        
        if not passed:
            logger.warning(f"{table_name}: {failed_count} rows failed range check for '{col}'")
        else:
            logger.info(f"{table_name}: range check passed for '{col}'")
    
    return results


def check_referential_integrity(df_child: pd.DataFrame, df_parent: pd.DataFrame,
                                 child_key: str, parent_key: str, 
                                 table_name: str) -> dict:
    """
    Check that foreign keys exist in the parent table.
    Required by assessment section 6 - Referential Integrity.
    e.g. player_id in appearances must exist in players table.
    """
    # get all unique keys from both tables as sets for fast comparison
    child_keys = set(df_child[child_key].dropna().astype(str))
    parent_keys = set(df_parent[parent_key].dropna().astype(str))
    
    # orphaned keys = keys in child that don't exist in parent
    # e.g. appearance references a player_id that doesn't exist in players
    orphaned_keys = child_keys - parent_keys
    passed = bool(len(orphaned_keys) == 0)  # bool() ensures True/False not numpy bool
    
    result = {
        "passed": passed,
        "orphaned_count": len(orphaned_keys),
        "total_rows": len(df_child)
    }
    
    if not passed:
        logger.warning(f"{table_name}: {len(orphaned_keys)} orphaned keys found in '{child_key}'")
    else:
        logger.info(f"{table_name}: referential integrity check passed for '{child_key}'")
    
    return result


def check_duplicates(df: pd.DataFrame, table_name: str, key_columns: list) -> dict:
    """
    Check for duplicate records based on key columns.
    Bonus check required by assessment section 6.
    """
    # duplicated() returns True for each row that is a duplicate
    duplicate_count = df.duplicated(subset=key_columns).sum()
    passed = bool(duplicate_count == 0)  # bool() ensures True/False not numpy bool
    
    result = {
        "passed": passed,
        "duplicate_count": int(duplicate_count),
        "total_rows": len(df)
    }
    
    if not passed:
        logger.warning(f"{table_name}: {duplicate_count} duplicate rows found")
    else:
        logger.info(f"{table_name}: duplicate check passed")
    
    return result


def generate_dq_report(results: dict, log_dir: str = "logs") -> str:
    """
    Generate a structured DQ report as a JSON file per pipeline run.
    Required by assessment section 6 - DQ report / structured logs per pipeline run.
    """
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    
    # count total checks and passed checks
    total_checks = 0
    passed_checks = 0
    
    for table_results in results.values():
        if isinstance(table_results, dict):
            # check if it's a single check result (has 'passed' key directly)
            if "passed" in table_results:
                total_checks += 1
                if table_results["passed"] == True:
                    passed_checks += 1
            else:
                # it's a dict of column checks
                for col_result in table_results.values():
                    if isinstance(col_result, dict) and "passed" in col_result:
                        total_checks += 1
                        if col_result["passed"] == True:
                            passed_checks += 1
    
    report = {
        "run_date": datetime.now().isoformat(),
        "results": results,
        "summary": {
            "total_checks": total_checks,
            "passed": passed_checks,
            "failed": total_checks - passed_checks  # how many checks failed
        }
    }
    
    # unique filename per run using timestamp
    report_path = f"{log_dir}/dq_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # save report as JSON - human readable with indent=2
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info(f"DQ report saved to {report_path}")
    return report_path


def run_dq_checks(config: dict, dataframes: dict) -> dict:
    """
    Run all data quality checks defined in config.
    Main entry point for DQ - called by the Airflow DAG.
    """
    dq_config = config["data_quality"]  # read DQ rules from config
    results = {}
    
    # null checks - reads which columns to check from config
    for table_name, columns in dq_config["null_check_columns"].items():
        if table_name in dataframes:
            results[f"{table_name}_null_checks"] = check_nulls(
                dataframes[table_name], table_name, columns
            )
    
    # valid range checks - reads range rules from config
    for table_name, range_rules in dq_config["valid_ranges"].items():
        if table_name in dataframes:
            results[f"{table_name}_range_checks"] = check_valid_ranges(
                dataframes[table_name], table_name, range_rules
            )
    
    # referential integrity - appearances.player_id must exist in players
    # hardcoded here because it's a specific business rule, not configurable
    if "appearances" in dataframes and "players" in dataframes:
        results["appearances_referential_integrity"] = check_referential_integrity(
            dataframes["appearances"], dataframes["players"],
            "player_id", "player_id", "appearances"
        )
    
    # duplicate checks on primary keys
    if "appearances" in dataframes:
        results["appearances_duplicates"] = check_duplicates(
            dataframes["appearances"], "appearances", ["appearance_id"]
        )
    if "games" in dataframes:
        results["games_duplicates"] = check_duplicates(
            dataframes["games"], "games", ["game_id"]
        )
    
    # generate JSON report for this run
    generate_dq_report(results, config["paths"]["logs"])
    
    logger.info("DQ checks complete.")
    return results