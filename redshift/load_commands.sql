copy stg_fact_games
from 's3://grid-pulse/curated/games/'
iam_role 'arn:aws:iam::183631349712:role/service-role/AmazonRedshift-CommandsAccessRole-20260419T233511'
format as parquet
region 'us-east-2';

truncate table fact_games;

insert into fact_games (
    game_id, game_date, season, week,
    team1_id, team1_name, team1_abbr, team1_score,
    team2_id, team2_name, team2_abbr, team2_score,
    game_status, winner_team, dt
)
select
    game_id, game_date, season, week,
    team1_id, team1_name, team1_abbr, team1_score,
    team2_id, team2_name, team2_abbr, team2_score,
    game_status, winner_team,
    cast(game_date as date) as dt
from stg_fact_games;

copy stg_fact_plays
from 's3://grid-pulse/curated/plays/'
iam_role 'arn:aws:iam::183631349712:role/service-role/AmazonRedshift-CommandsAccessRole-20260419T233511'
format as parquet
region 'us-east-2';

truncate table fact_plays_loaded;

insert into fact_plays_loaded
select *
from stg_fact_plays;
