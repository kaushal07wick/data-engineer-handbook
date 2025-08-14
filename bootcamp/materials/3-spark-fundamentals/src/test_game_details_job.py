import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

from ..jobs.arrays_metrics_job import dedupe_game_details

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("dedupe_game_details_test") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_dedupe_game_details(spark):
    # Sample games data
    games_data = [
        (1, datetime(2023, 1, 1), "2023", 100),
        (2, datetime(2023, 1, 2), "2023", 101),
    ]
    games_df = spark.createDataFrame(games_data, ["game_id", "game_date_est", "season", "home_team_id"])

    # Sample game_details with duplicates
    game_details_data = [
        (1, 10, 1000, "Player A", "PG"),
        (1, 10, 1000, "Player A", "PG"),  # duplicate
        (1, 11, 1001, "Player B", "SG"),
        (2, 12, 1002, "Player C", "SF")
    ]
    game_details_df = spark.createDataFrame(game_details_data, ["game_id", "team_id", "player_id", "player_name", "start_position"])

    # Run dedupe function
    deduped_df = dedupe_game_details(spark, game_details_df, games_df)

    # Collect results
    result = deduped_df.collect()

    # Check that duplicates are removed
    assert len(result) == 3

    # Check that the correct rows are kept
    kept_player_ids = [row.player_id for row in result]
    assert 1000 in kept_player_ids
    assert 1001 in kept_player_ids
    assert 1002 in kept_player_ids

    # Check join values are present
    for row in result:
        assert hasattr(row, "game_date_est")
        assert hasattr(row, "season")
        assert hasattr(row, "home_team_id")
