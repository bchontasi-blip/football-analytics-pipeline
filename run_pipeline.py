from src.utils.config_loader import load_config
from src.utils.logger import setup_logger
from src.ingestion.ingestor import run_ingestion
from src.dq.data_quality import run_dq_checks
from src.silver.silver_layer import run_silver
from src.gold.gold_layer import run_gold

if __name__ == "__main__":
    # load config from configs/config.yaml
    config = load_config()
    
    # setup logger - writes to console and logs/ folder
    setup_logger(log_dir=config["paths"]["logs"])
    
    # step 1 - ingest CSVs from day1 to Bronze parquet
    print("Step 1: Ingestion...")
    dataframes = run_ingestion(config, source_key="data_day1")
    
    # step 2 - run data quality checks on Bronze data
    print("Step 2: Data Quality checks...")
    run_dq_checks(config, dataframes)
    
    # step 3 - transform Bronze to Silver star schema
    print("Step 3: Silver transformation...")
    silver_tables = run_silver(config, dataframes)
    
    # step 4 - aggregate Silver to Gold business tables
    print("Step 4: Gold aggregation...")
    run_gold(config, silver_tables)
    
    print("Pipeline complete.")
    