SELECT COUNT(*) AS total_games
FROM gridiron_insights_db.games_curated;

SELECT COUNT(*) AS total_plays
FROM gridiron_insights_db.plays_curated;

SELECT *
FROM gridiron_insights_db.games_curated
LIMIT 10;

SELECT *
FROM gridiron_insights_db.plays_curated
LIMIT 10;
