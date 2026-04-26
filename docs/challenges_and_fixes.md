# Challenges and Fixes

## 1. Lambda environment variable error
The ingestion Lambda initially failed because BUCKET_NAME was not configured.
Fix: added BUCKET_NAME, SPORT, LEAGUE, DATE_RANGE, and MAX_GAMES as Lambda environment variables.

## 2. Only one game was ingested initially
The first version relied on the default scoreboard output and only returned one game.
Fix: updated the Lambda to use a date range and configurable game limit.

## 3. Glue schema mismatch
The ETL initially failed because the games raw file structure changed after wrapping scoreboard metadata.
Fix: updated the ETL to read from scoreboard_response.events and extract nested event-level fields.

## 4. Curated crawler generated unstable long table names
Glue crawler created long hashed table names for curated datasets, which made Athena and QuickSight harder to manage.
Fix: created stable Athena external tables manually for games_curated and plays_curated.

## 5. Redshift cross-region S3 issue
Redshift COPY failed because the S3 bucket was in Ohio and Redshift was in Virginia.
Fix: added region 'us-east-2' to COPY commands.

## 6. Redshift parquet column mismatch
COPY into Redshift failed because partition columns were not present in the parquet file body.
Fix: used staging-table logic and loaded partition-derived columns separately.

## 7. QuickSight access issue
QuickSight failed to query Athena because it lacked the required Athena/S3 permissions.
Fix: enabled Athena, Glue, and S3 bucket access in QuickSight security settings.

## 8. MWAA Lambda invocation issue
Airflow failed because the MWAA execution role did not have lambda:InvokeFunction permission.
Fix: updated the MWAA execution role to allow Lambda, Glue, Athena, and S3 access.
