create table if not exists fact_games (
    game_id varchar(32),
    game_date timestamp,
    season int,
    week int,
    team1_id varchar(32),
    team1_name varchar(200),
    team1_abbr varchar(20),
    team1_score int,
    team2_id varchar(32),
    team2_name varchar(200),
    team2_abbr varchar(20),
    team2_score int,
    game_status varchar(100),
    winner_team varchar(200),
    dt date
);

create table if not exists fact_plays (
    game_id varchar(32),
    drive_id varchar(32),
    play_id varchar(32),
    play_text varchar(5000),
    period int,
    clock varchar(20),
    home_score int,
    away_score int,
    scoring_play boolean,
    possession_team_id varchar(32),
    play_type varchar(200),
    yards_gained int,
    is_turnover boolean,
    play_sequence int,
    momentum_score int,
    play_type_simple varchar(50),
    dt date
);

drop table if exists stg_fact_games;

create table stg_fact_games (
    game_id varchar(32),
    game_date timestamp,
    season int,
    week int,
    team1_id varchar(32),
    team1_name varchar(200),
    team1_abbr varchar(20),
    team1_score int,
    team2_id varchar(32),
    team2_name varchar(200),
    team2_abbr varchar(20),
    team2_score int,
    game_status varchar(100),
    winner_team varchar(200)
);

drop table if exists stg_fact_plays;

create table stg_fact_plays (
    drive_id varchar(32),
    play_id varchar(32),
    play_text varchar(5000),
    period int,
    clock varchar(20),
    home_score int,
    away_score int,
    scoring_play boolean,
    possession_team_id varchar(32),
    play_type varchar(200),
    yards_gained int,
    is_turnover boolean,
    play_sequence int,
    momentum_score int,
    play_type_simple varchar(50)
);

drop table if exists fact_plays_loaded;

create table fact_plays_loaded (
    drive_id varchar(32),
    play_id varchar(32),
    play_text varchar(5000),
    period int,
    clock varchar(20),
    home_score int,
    away_score int,
    scoring_play boolean,
    possession_team_id varchar(32),
    play_type varchar(200),
    yards_gained int,
    is_turnover boolean,
    play_sequence int,
    momentum_score int,
    play_type_simple varchar(50)
);
