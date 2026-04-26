# Pipeline Flow

Final automated workflow:

1. Archive current active raw and curated files
2. Pull latest NFL data from ESPN using Lambda
3. Store raw JSON in S3
4. Run Glue raw crawler
5. Run Glue ETL to create curated parquet outputs
6. Refresh Athena partitions for games_curated
7. Refresh Athena partitions for plays_curated
8. Query Athena / load Redshift / visualize in QuickSight
