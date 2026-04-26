DROP VIEW IF EXISTS gridiron_insights_db.games_curated;
DROP VIEW IF EXISTS gridiron_insights_db.plays_curated;

CREATE EXTERNAL TABLE IF NOT EXISTS gridiron_insights_db.games_curated (
  game_id string,
  game_date timestamp,
  season int,
  week int,
  team1_id string,
  team1_name string,
  team1_abbr string,
  team1_score int,
  team2_id string,
  team2_name string,
  team2_abbr string,
  team2_score int,
  game_status string,
  winner_team string
)
PARTITIONED BY (
  dt date
)
STORED AS PARQUET
LOCATION 's3://grid-pulse/curated/games/';

MSCK REPAIR TABLE gridiron_insights_db.games_curated;

CREATE EXTERNAL TABLE IF NOT EXISTS gridiron_insights_db.plays_curated (
  drive_id string,
  play_id string,
  play_text string,
  period int,
  clock string,
  home_score int,
  away_score int,
  scoring_play boolean,
  possession_team_id string,
  play_type string,
  yards_gained int,
  is_turnover boolean,
  play_sequence int,
  momentum_score int,
  play_type_simple string
)
PARTITIONED BY (
  dt date,
  game_id string
)
STORED AS PARQUET
LOCATION 's3://grid-pulse/curated/plays/';

MSCK REPAIR TABLE gridiron_insights_db.plays_curated;
