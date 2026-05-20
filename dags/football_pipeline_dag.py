from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  # operator that runs Python functions
from airflow.utils.dates import days_ago

import pandas as pd
from pathlib import Path

# import our pipeline modules - all the code we built
from src.utils.config_loader import load_config
from src.utils.logger import setup_logger
from src.ingestion.ingestor import run_ingestion
from src.dq.data_quality import run_dq_checks
from src.silver.silver_layer import run_silver
from src.gold.gold_layer import run_gold
from src.ingestion.generate_day2 import generate_day2_data

# load config and setup logger when DAG is parsed by Airflow
config = load_config()
setup_logger(log_dir=config["paths"]["logs"])

# ---- DAG DEFAULT ARGUMENTS ----
# these apply to every task in the DAG unless overridden
default_args = {
    "owner": "bryan",                  # who owns this DAG
    "depends_on_past": False,          # don't wait for previous run to succeed
    "email_on_failure": False,         # no email alerts for now
    "email_on_retry": False,
    "retries": config["airflow"]["retries"],  # 3 retries from config
    "retry_delay": timedelta(minutes=config["airflow"]["retry_delay_minutes"])  # 5 min between retries
}

# ---- DAG DEFINITION ----
# the DAG object is the container for all tasks
# Idempotent execution: all tasks overwrite their output parquet files on each run.
# Re-running the DAG will not cause duplicate data — Bronze, Silver and Gold
# parquet files are fully replaced, not appended to.

dag = DAG(
    dag_id="football_analytics_pipeline",  # unique name in Airflow UI
    default_args=default_args,
    description="Football Analytics ETL Pipeline - Bronze to Gold",
    schedule_interval=config["airflow"]["schedule_interval"],  # @hourly from config - configurable
    start_date=days_ago(1),   # start from yesterday so it runs immediately
    catchup=False,            # don't backfill missed runs - important for incremental loads
    tags=["football", "etl", "medallion"]  # tags for filtering in Airflow UI
)

# ---- TASK FUNCTIONS ----
# Airflow needs plain Python functions to wrap our pipeline modules
# each function is one step in the pipeline

def task_ingestion_day1():
    """Ingest day1 data from CSV to Bronze parquet - first load"""
    run_ingestion(config, source_key="data_day1")

def task_dq_checks():
    """Run data quality checks on Bronze data"""
    dataframes = run_ingestion(config, source_key="data_day1")
    run_dq_checks(config, dataframes)

def task_silver():
    """Read from Bronze, transform to Silver parquet"""
    dataframes = run_ingestion(config, source_key="data_day1")
    run_silver(config, dataframes)
    # automatically writes to data/silver/

def task_gold():
    """Read from Silver parquet, aggregate to Gold - no reprocessing"""
    silver_path = config["paths"]["silver"]
    
    # read silver tables from disk - written by task_silver
    silver_tables = {}
    for table in ["fact_appearances", "fact_games", "player_valuations"]:
        parquet_file = Path(silver_path) / f"{table}.parquet"
        silver_tables[table] = pd.read_parquet(parquet_file)
    
    run_gold(config, silver_tables)

def task_generate_day2():
    """Generate day2 data to simulate incremental load"""
    generate_day2_data(
        day1_path=config["paths"]["data_day1"],
        day2_path=config["paths"]["data_day2"]
    )

def task_ingestion_day2():
    """Ingest day2 data - incremental load"""
    run_ingestion(config, source_key="data_day2")

# ---- AIRFLOW TASKS ----
# PythonOperator wraps each function as an Airflow task
# task_id must be unique within the DAG

generate_day2 = PythonOperator(
    task_id="generate_day2_data",      # name shown in Airflow UI
    python_callable=task_generate_day2, # function to run
    dag=dag                             # which DAG this task belongs to
)

ingestion_day1 = PythonOperator(
    task_id="ingestion_day1",
    python_callable=task_ingestion_day1,
    dag=dag
)

dq_checks = PythonOperator(
    task_id="data_quality_checks",
    python_callable=task_dq_checks,
    dag=dag
)

silver_transform = PythonOperator(
    task_id="silver_transformation",
    python_callable=task_silver,
    dag=dag
)

gold_aggregation = PythonOperator(
    task_id="gold_aggregation",
    python_callable=task_gold,
    dag=dag
)

ingestion_day2 = PythonOperator(
    task_id="ingestion_day2",
    python_callable=task_ingestion_day2,
    dag=dag
)

# ---- DAG PIPELINE FLOW ----
# >> operator defines the order of execution
# this matches exactly what the assessment requires in section 9:
# ingestion → DQ → Silver → Gold
# then we add: generate day2 → ingestion day2 for incremental load demo
ingestion_day1 >> dq_checks >> silver_transform >> gold_aggregation >> generate_day2 >> ingestion_day2