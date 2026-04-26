# Crawler Notes

## Raw crawler
The raw crawler is used to inspect and register schema for:
- `raw/games/`
- `raw/plays/`

## Curated crawler
The curated crawler was originally used, but it generated unstable long table names for curated outputs.

## Final approach
The final design keeps the raw crawler, but uses manually created stable Athena external tables for:
- `games_curated`
- `plays_curated`

After each ETL run, partitions are refreshed using:

```sql
MSCK REPAIR TABLE gridiron_insights_db.games_curated;
MSCK REPAIR TABLE gridiron_insights_db.plays_curated;
```
