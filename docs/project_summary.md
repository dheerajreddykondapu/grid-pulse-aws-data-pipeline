# Project Summary

I built this project to simulate a production-style AWS analytics pipeline using NFL data as the use case.

The project starts with a Lambda function that calls ESPN public APIs and stores raw scoreboard and play-level summary JSON in S3. Before every new run, an archive Lambda moves the previous active raw and curated data into timestamped archive folders so the active pipeline always starts cleanly.

I used AWS Glue to crawl the raw schema and run a PySpark ETL job that flattens nested JSON into curated parquet outputs. The ETL creates two curated layers: a game-level dataset and a play-level dataset. I also engineered analytical fields such as winner team, simplified play type, scoring indicators, and momentum score.

For SQL analytics, I used Athena with stable curated tables created manually on top of curated S3 paths. I did this because the Glue crawler was generating unstable long table names for curated outputs. After each ETL run, I refresh partitions instead of relying on a curated crawler.

I built dashboards in QuickSight using Athena datasets and also created Redshift warehouse tables for game-level and play-level loading. Redshift loading required handling cross-region S3 access and parquet partition-column behavior.

Finally, I orchestrated the workflow in Amazon MWAA using Airflow to automate archive, ingestion, raw crawling, ETL, and Athena partition refresh steps.
