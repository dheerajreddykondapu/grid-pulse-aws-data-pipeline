# Setup Guide

## 1. Create S3 bucket
Create a bucket for raw, curated, archive, and query result storage.

## 2. Create Lambda functions
- ingestion Lambda
- archive Lambda
- optional Athena repair Lambda

Configure environment variables and IAM permissions.

## 3. Create Glue raw crawler
Point the crawler to:
- raw/games/
- raw/plays/

## 4. Create Glue ETL job
Upload the PySpark script and configure the job to write parquet to:
- curated/games/
- curated/plays/

## 5. Create stable Athena curated tables
Manually create:
- games_curated
- plays_curated

Refresh partitions after ETL with MSCK REPAIR TABLE.

## 6. Create Redshift Serverless
Create warehouse tables and configure IAM access for S3 parquet loads.

## 7. Create QuickSight datasets
Build datasets from Athena and create visuals for:
- game results
- wins by team
- play types
- scoring plays
- momentum

## 8. Create MWAA environment
Upload the DAG file, configure permissions, and trigger the pipeline from Airflow.
