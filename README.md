### DAG Tasks
| Task | Description |
|------|-------------|
| ingestion_day1 | Ingest day1 CSVs to Bronze parquet |
| data_quality_checks | Run null, range, referential integrity and duplicate checks |
| silver_transformation | Clean and transform Bronze to Silver star schema |
| gold_aggregation | Aggregate Silver to Gold business tables |
| generate_day2_data | Generate incremental data snapshot |
| ingestion_day2 | Ingest day2 CSVs — incremental load |

---

## Data Quality

The pipeline runs the following checks on every run:

| Check | Rule |
|-------|------|
| Null check | player_id and game_id must not be null |
| Valid ranges | minutes_played >= 0, goals >= 0 |
| Referential integrity | player_id in appearances must exist in players |
| Duplicate detection | appearance_id and game_id must be unique |

A structured DQ report is generated as a JSON file in `logs/` on every pipeline run.

---

## Incremental Load & SCD Type 2

The pipeline supports incremental data arrival simulated via a `data/day2/` snapshot containing:
- 3 new game records with future dates
- 2 player club transfers (triggers SCD Type 2)
- 3 new player market valuations
- 1 soft-deleted player (is_deleted flag)

SCD Type 2 is implemented for `players` and `player_valuations`, tracking changes to `current_club_id`, `position`, and `market_value_in_eur` with `effective_date`, `end_date`, and `is_current` columns.

---

## Future Improvements

- **Quarantine layer** — Invalid records are currently logged and reported. In production, a quarantine table would isolate bad records for manual review without stopping the pipeline.
- **Delta format** — In a production Databricks environment, Delta would replace Parquet for ACID transaction support and time travel.
- **PySpark swap** — The architecture is designed to allow swapping Pandas for PySpark with minimal refactoring when the dataset scales to GB+.
- **Metabase/Superset** — An open-source reporting tool could be integrated to visualise Gold layer tables.