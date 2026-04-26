# Grid Pulse AWS Data Pipeline

Grid Pulse is an end-to-end AWS sports analytics pipeline built to ingest NFL game and play-by-play data, transform nested JSON into curated parquet datasets, and expose insights through Athena, Redshift, QuickSight, and Airflow orchestration.

## Project Overview

This project simulates a production-style AWS data pipeline using NFL data from ESPN public APIs.

The pipeline:
- pulls raw game and play-level JSON using AWS Lambda
- stores raw data in Amazon S3
- archives previous active data automatically before each new run
- uses AWS Glue crawler and Glue ETL to transform nested JSON
- writes analytics-ready curated parquet outputs to S3
- queries curated data in Athena
- loads selected outputs into Redshift
- visualizes results in QuickSight
- orchestrates the complete workflow using Amazon MWAA (Airflow)

## Architecture

Pipeline flow:

1. Archive current active raw and curated files
2. Pull latest NFL data using Lambda
3. Store raw JSON in S3
4. Run Glue raw crawler
5. Run Glue ETL job
6. Refresh Athena curated partitions
7. Query analytics tables in Athena
8. Load warehouse-ready data into Redshift
9. Build dashboards in QuickSight
10. Schedule and orchestrate using Airflow

## AWS Services Used

- AWS Lambda
- Amazon S3
- AWS Glue
- Amazon Athena
- Amazon Redshift Serverless
- Amazon QuickSight
- Amazon MWAA (Managed Workflows for Apache Airflow)
- AWS Step Functions (workflow prototype)

## S3 Layout

```text
raw/games/
raw/plays/
curated/games/
curated/plays/
archive/<timestamp>/raw/games/
archive/<timestamp>/raw/plays/
archive/<timestamp>/curated/games/
archive/<timestamp>/curated/plays/
```

## Key Features

- Date-range-based NFL ingestion from ESPN public endpoints
- Automatic archive of previous active data before each run
- Nested JSON flattening with Glue PySpark ETL
- Stable Athena curated tables with fixed names
- Redshift warehouse integration
- QuickSight dashboards for game and play-level analysis
- Airflow orchestration using MWAA

## Final Validated Output

- 64 games
- 3680 plays

## Main Analytics Outputs

- game result summary
- wins by team
- play type distribution
- scoring plays by game
- momentum by game

## Challenges Solved

- Lambda environment variable configuration issues
- Glue schema mismatches from nested JSON
- unstable curated crawler table names
- QuickSight Athena/S3 permission issues
- Redshift cross-region S3 COPY handling
- parquet partition-column mismatch in Redshift loads
- MWAA execution role permission issues

## Repository Structure

```text
lambda/               Lambda functions
glue/                 Glue ETL script
airflow/              MWAA DAG
athena/               Athena DDL and SQL queries
redshift/             Redshift DDL and load SQL
step_functions/       State machine JSON
docs/                 setup notes, summary, issues, fixes
architecture/         screenshots and diagrams
```

## Notes

Stable Athena curated tables were created manually so the pipeline does not depend on long, unstable crawler-generated table names.

After each ETL run, curated partitions are refreshed using:

```sql
MSCK REPAIR TABLE gridiron_insights_db.games_curated;
MSCK REPAIR TABLE gridiron_insights_db.plays_curated;
```

## Future Improvements

- team-level dimension tables
- cleaner joined analytical views for QuickSight
- fully automated Redshift load with partition-aware handling
- alerting and notifications
- support for additional sports/leagues
