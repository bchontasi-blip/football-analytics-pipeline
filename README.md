# Football Analytics Pipeline

ETL pipeline built on the Kaggle Football Dataset using a Bronze → Silver → Gold architecture.

The goal of this project was to simulate a realistic data engineering workflow including orchestration, data quality validation, historical tracking (SCD Type 2), and incremental processing.

---

## Why I Built It This Way

Before diving into setup instructions, I want to explain the key decisions behind this pipeline. These aren't arbitrary choices,  each one came from thinking about what would make sense in a real production environment.

### Pandas over PySpark

The dataset is around 200MB in total (~1.8 million appearance records, 88k games, 47k players). That's comfortably within what Pandas handles in memory on a standard laptop. For this dataset size, Pandas felt like the simpler tradeoff.

Spark would become more attractive once processing time, memory pressure, or distributed execution started becoming constraints.

### Parquet over CSV

I chose Parquet because the project is analytics-oriented and benefits from columnar storage and typed schemas.

Delta would probably be my next step if transactional guarantees or repeated incremental writes became requirements.

### SCD Type 2 for players and player_valuations only

I only applied SCD Type 2 to these two tables because they're the only ones where historical tracking genuinely matters:

- **players** — players transfer between clubs, change positions, and their market value fluctuates. You want to know where a player was at any point in time.
- **player_valuations** — this is literally a table of historical market values. SCD Type 2 is the natural fit.

The other tables don't need it. `games` and `appearances` are immutable historical facts — a match result doesn't change after the final whistle. `clubs` and `competitions` are stable reference data.

### Config-driven architecture

Most runtime parameters (file paths, table names, DQ rules, Airflow schedule, and mappings) are externalised in `configs/config.yaml`.

This allows changing pipeline behaviour without modifying transformation logic.

### Why loguru over Python's built-in logging

Python's `logging` module works fine but requires a lot of boilerplate to get clean structured output. Loguru gives the same result in one line of setup, with automatic timestamps, log levels, and file rotation. In production I'd use CloudWatch or Datadog, but for a self-contained local pipeline loguru was the pragmatic choice.

---

## What the Pipeline Actually Does

```
CSV files (Kaggle)
    ↓
Bronze — raw ingestion, minimal transformation, stored as Parquet
    ↓
Data Quality — null checks, range checks, referential integrity, duplicate detection
    ↓
Silver — cleaned, deduplicated, type-cast data in Star Schema
    ↓
Gold — aggregated business-ready tables
```

### Bronze Layer
Reads the 6 CSVs from `data/day1/`, adds two metadata columns (`_ingested_at` and `_source_file`), validates the schema, and writes to `data/bronze/` as Parquet. No business logic here, Bronze is the source of truth. If something goes wrong downstream, you can always reprocess from Bronze.

### Silver Layer
Builds the Star Schema:

**Dimension tables:**
- `dim_players` — player profiles with normalised positions (Attack → Forward, Midfield → Midfielder, etc.) and country names standardised to ISO 3166-1 alpha-2 codes (France → FR, England → GB-ENG, etc.)
- `dim_clubs` — club master data
- `dim_competitions` — competition master data
- `dim_date` — generated from match dates, not from a source CSV. Includes year, month, quarter, day of week, is_weekend, and season (e.g. 2023/24)

**Fact tables:**
- `fact_appearances` — one row per player per game, with goals, assists, minutes, cards
- `fact_games` — one row per match, with derived `match_outcome` (home_win / away_win / draw) and `season_year`

### Gold Layer
Three business-ready aggregation tables:
- `player_performance` — total goals, assists, minutes, and matches per player per season
- `club_performance` — wins, losses, draws, and goals scored per club per season (home and away combined)
- `player_valuation_trend` — market value history with a 3-period rolling average per player

---

## Data Quality

Four checks run on every pipeline execution, with results written to a JSON report in `logs/`:

| Check | Rule | Purpose |
|-------|------|-----|
| Null check | `player_id` and `game_id` must not be null | Without these keys, records are useless for any join |
| Valid ranges | `minutes_played >= 0`, `goals >= 0` | Negative values are physically impossible |
| Referential integrity | `player_id` in appearances must exist in players | Orphaned records cause broken analytics |
| Duplicate detection | `appearance_id` and `game_id` must be unique | Duplicates inflate aggregations |

**One issue found in the real dataset:** `player_id 380365` has 2 appearances but no corresponding record in the players table — a genuine inconsistency in the Transfermarkt source data. The pipeline flags it, logs it, and continues without crashing.

In production I'd add a quarantine layer to isolate these records for investigation rather than just logging them.

The DQ report is generated as a JSON file in `logs/` on every pipeline run. Example output:

```json
{
  "run_date": "2026-05-20T23:28:38",
  "summary": {
    "total_checks": 8,
    "passed": 7,
    "failed": 1
  }
}
```

The one failing check is a known data inconsistency in the Transfermarkt source — `player_id 380365` exists in appearances but not in players.

---

## Incremental Load & SCD Type 2

The pipeline simulates real-world incremental data arrival via a `data/day2/` snapshot. I verified the counts directly:

| Table | Day 1 | Day 2 | Difference |
|-------|-------|-------|------------|
| players | 47,637 | 47,637 | +0 (2 transfers tracked via SCD) |
| games | 88,271 | 88,274 | +3 new matches |
| appearances | 1,877,839 | 1,877,842 | +3 new appearances |
| clubs | 796 | 796 | +0 |
| competitions | 67 | 67 | +0 |
| player_valuations | 507,815 | 507,818 | +3 new valuations |

The day2 snapshot includes:
- 3 new game records (Chelsea vs Arsenal, Real Madrid vs Barcelona, Borussia Dortmund vs Bayern Munich) with future dates verified against the dataset's max date of 2026-05-07
- 2 player club transfers to test SCD Type 2 — the pipeline correctly closes the old record (`end_date` set, `is_current = False`) and inserts a new one
- 3 new market valuations to update the rolling average in Gold
- 1 soft-deleted player (`is_deleted = True`) to simulate record removal

---

## Tech Stack

| Component | Technology | Why |
|-----------|------------|-----|
| Language | Python 3.11 | Matches the Docker image version |
| Processing | Pandas 3.0 | Sufficient for ~200MB dataset |
| File format | Parquet (PyArrow) | Columnar, compressed, schema-enforced |
| Orchestration | Apache Airflow 2.8.1 | Industry standard, DAG-based |
| Containerisation | Docker + Compose | Full reproducibility |
| Testing | pytest | Simpler than unittest, industry standard |
| Logging | Loguru | Clean structured logs with minimal boilerplate |
| Config | YAML | Human-readable, no hardcoded values |

---

## Project Structure

```
football-analytics-pipeline/
├── dags/
│   └── football_pipeline_dag.py    # Airflow DAG — 6 tasks in order
├── src/
│   ├── ingestion/
│   │   ├── ingestor.py             # reads CSVs, writes Bronze parquet
│   │   └── generate_day2.py        # generates incremental snapshot
│   ├── bronze/
│   │   └── bronze_layer.py         # schema validation, parquet reads
│   ├── silver/
│   │   ├── silver_layer.py         # cleaning, star schema transformations
│   │   └── scd.py                  # SCD Type 2 logic
│   ├── gold/
│   │   └── gold_layer.py           # business aggregations
│   ├── dq/
│   │   └── data_quality.py         # DQ checks + JSON report per run
│   └── utils/
│       ├── config_loader.py        # reads config.yaml into a dict
│       └── logger.py               # loguru setup
├── tests/
│   ├── test_transformations.py     # 5 tests
│   ├── test_dq.py                  # 8 tests
│   └── test_incremental.py         # 4 tests — SCD Type 2 scenarios
├── configs/
│   └── config.yaml                 # all pipeline parameters, no hardcoded values
├── data/
│   ├── day1/                       # initial Kaggle CSV snapshot
│   ├── day2/                       # simulated incremental data arrival
│   ├── bronze/                     # raw parquet files
│   ├── silver/                     # star schema parquet files
│   └── gold/                       # aggregated business tables
├── .dockerignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── run_pipeline.py                 # run pipeline locally without Airflow
└── README.md
```

---

## Setup & Running

### Prerequisites

- Python 3.11
- Docker Desktop
- Git

### Download Dataset

Download the Kaggle dataset and place the following files inside `data/day1/`:

- `players.csv`
- `games.csv`
- `appearances.csv`
- `clubs.csv`
- `competitions.csv`
- `player_valuations.csv`

### Option 1 — Docker + Airflow (recommended)

```bash
docker-compose up --build
```

Open:

```text
http://localhost:8080
```

Login:

```text
admin / admin
```

Trigger:

```text
football_analytics_pipeline
```

Expected output:
- Bronze parquet tables
- Silver star schema
- Gold aggregation tables
- DQ report in `logs/`

### Option 2 — Run locally

```bash
git clone https://github.com/bchontasi-blip/football-analytics-pipeline.git
cd football-analytics-pipeline

python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Mac/Linux

pip install -r requirements.txt
python run_pipeline.py
```

### Run Tests

```bash
pytest tests/ -v
```

Expected: **17 passed**

---

## Airflow DAG

The DAG follows the flow required by the assessment, with two additional tasks for the incremental load demo:

```
ingestion_day1 → data_quality_checks → silver_transformation → gold_aggregation → generate_day2_data → ingestion_day2
```

Each task is a `PythonOperator` wrapping the relevant pipeline module. Tasks read from and write to disk (parquet files) rather than passing dataframes in memory — this makes each task independently restartable without reprocessing upstream layers.

Configured via `config.yaml`:
- Schedule: `@hourly` (configurable)
- Retries: 3 (configurable)
- Retry delay: 5 minutes (configurable)

**Idempotent execution** — all tasks overwrite their output parquet files on each run. Re-running the DAG will not cause duplicate data — Bronze, Silver and Gold parquet files are fully replaced, not appended to.
---

## Future Improvements

**Quarantine layer** — invalid records are currently logged and reported in the DQ JSON. In production, they'd be written to a `data/quarantine/` folder for manual review, keeping bad data visible without stopping the pipeline.

**Delta format** — in a production Databricks environment, I'd replace Parquet with Delta for ACID transactions, time travel, and better support for incremental updates. The architecture supports this swap with minimal changes.

**PySpark at scale** — the Pandas to PySpark swap is intentionally straightforward. Each transformation function takes a dataframe and returns a dataframe. Switching the engine means changing the `config.yaml` engine setting and the import statements — the business logic stays the same.

**Great Expectations for DQ** — the current DQ checks are custom-built and work well, but in production I'd consider replacing them with Great Expectations for a more standardised, declarative approach to data quality with built-in reporting.

**CI/CD pipeline** — adding a GitHub Actions workflow to run the pytest suite on every pull request would catch regressions early and give confidence before merging changes.