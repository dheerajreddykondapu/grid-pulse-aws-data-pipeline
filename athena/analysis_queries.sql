SELECT
  game_id,
  game_date,
  season,
  week,
  team1_name,
  team2_name,
  team1_score,
  team2_score,
  winner_team,
  game_status
FROM gridiron_insights_db.games_curated
ORDER BY game_date
LIMIT 25;

SELECT
  play_type_simple,
  COUNT(*) AS play_count
FROM gridiron_insights_db.plays_curated
GROUP BY play_type_simple
ORDER BY play_count DESC;

SELECT
  game_id,
  COUNT(*) AS scoring_plays
FROM gridiron_insights_db.plays_curated
WHERE scoring_play = true
GROUP BY game_id
ORDER BY scoring_plays DESC;

SELECT
  winner_team,
  COUNT(*) AS wins
FROM gridiron_insights_db.games_curated
GROUP BY winner_team
ORDER BY wins DESC;

SELECT
  game_id,
  SUM(momentum_score) AS total_momentum
FROM gridiron_insights_db.plays_curated
GROUP BY game_id
ORDER BY total_momentum DESC;
