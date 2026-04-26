select count(*) as fact_games_rows from fact_games;
select count(*) as stg_fact_plays_rows from stg_fact_plays;
select count(*) as fact_plays_loaded_rows from fact_plays_loaded;

select * from fact_games limit 10;
select * from fact_plays_loaded limit 10;
