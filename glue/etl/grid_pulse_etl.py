import sys
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["BUCKET"]

raw_games_path = f"s3://{BUCKET}/raw/games/"
raw_plays_path = f"s3://{BUCKET}/raw/plays/"
curated_games_path = f"s3://{BUCKET}/curated/games/"
curated_plays_path = f"s3://{BUCKET}/curated/plays/"

raw_games_df = spark.read.json(raw_games_path)
raw_plays_df = spark.read.json(raw_plays_path)

games_df = (
    raw_games_df
    .select(
        F.explode_outer("scoreboard_response.events").alias("event")
    )
    .select(
        F.col("event.id").cast("string").alias("game_id"),
        F.col("event.date").alias("game_date"),
        F.col("event.season.year").cast("int").alias("season"),
        F.col("event.week.number").cast("int").alias("week"),
        F.col("event.competitions")[0]["competitors"][0]["team"]["id"].cast("string").alias("team1_id"),
        F.col("event.competitions")[0]["competitors"][0]["team"]["displayName"].alias("team1_name"),
        F.col("event.competitions")[0]["competitors"][0]["team"]["abbreviation"].alias("team1_abbr"),
        F.col("event.competitions")[0]["competitors"][0]["score"].cast("int").alias("team1_score"),
        F.col("event.competitions")[0]["competitors"][1]["team"]["id"].cast("string").alias("team2_id"),
        F.col("event.competitions")[0]["competitors"][1]["team"]["displayName"].alias("team2_name"),
        F.col("event.competitions")[0]["competitors"][1]["team"]["abbreviation"].alias("team2_abbr"),
        F.col("event.competitions")[0]["competitors"][1]["score"].cast("int").alias("team2_score"),
        F.col("event.status.type.description").alias("game_status")
    )
    .dropDuplicates(["game_id"])
    .withColumn("game_date", F.to_timestamp("game_date"))
    .withColumn("dt", F.to_date("game_date"))
    .withColumn(
        "winner_team",
        F.when(F.col("team1_score") > F.col("team2_score"), F.col("team1_name"))
         .when(F.col("team2_score") > F.col("team1_score"), F.col("team2_name"))
         .otherwise(F.lit("Tie"))
    )
)

plays_df = (
    raw_plays_df
    .select(
        F.col("header.id").cast("string").alias("game_id"),
        F.explode_outer("drives.previous").alias("drive")
    )
    .select(
        F.col("game_id"),
        F.col("drive.id").cast("string").alias("drive_id"),
        F.explode_outer("drive.plays").alias("play")
    )
    .select(
        F.col("game_id"),
        F.col("drive_id"),
        F.col("play.id").cast("string").alias("play_id"),
        F.col("play.text").alias("play_text"),
        F.col("play.period.number").cast("int").alias("period"),
        F.col("play.clock.displayValue").alias("clock"),
        F.col("play.start.down").cast("int").alias("start_down"),
        F.col("play.start.distance").cast("int").alias("start_distance"),
        F.col("play.start.yardLine").cast("int").alias("start_yardline"),
        F.col("play.end.down").cast("int").alias("end_down"),
        F.col("play.end.distance").cast("int").alias("end_distance"),
        F.col("play.end.yardLine").cast("int").alias("end_yardline"),
        F.col("play.homeScore").cast("int").alias("home_score"),
        F.col("play.awayScore").cast("int").alias("away_score"),
        F.col("play.scoringPlay").cast("boolean").alias("scoring_play"),
        F.col("play.statYardage").cast("int").alias("yards_gained"),
        F.col("play.possession.id").cast("string").alias("possession_team_id"),
        F.col("play.type.text").alias("play_type"),
        F.col("play.sequenceNumber").cast("int").alias("play_sequence"),
        F.col("play.isTurnover").cast("boolean").alias("is_turnover"),
        F.col("play.wallclock").alias("wallclock")
    )
    .dropDuplicates(["game_id", "play_id"])
    .withColumn("wallclock", F.to_timestamp("wallclock"))
    .withColumn("dt", F.to_date("wallclock"))
    .withColumn(
        "play_type_simple",
        F.when(F.lower(F.col("play_type")).contains("pass"), F.lit("Pass"))
         .when(F.lower(F.col("play_type")).contains("run"), F.lit("Run"))
         .when(F.lower(F.col("play_type")).contains("punt"), F.lit("Punt"))
         .when(F.lower(F.col("play_type")).contains("field goal"), F.lit("Field Goal"))
         .otherwise(F.lit("Other"))
    )
    .withColumn(
        "momentum_score",
        F.when(F.col("scoring_play") == True, F.lit(7))
         .when(F.col("is_turnover") == True, F.lit(3))
         .otherwise(F.lit(0))
    )
)

games_df.write.mode("overwrite").partitionBy("dt").parquet(curated_games_path)
plays_df.write.mode("overwrite").partitionBy("dt", "game_id").parquet(curated_plays_path)

job.commit()
