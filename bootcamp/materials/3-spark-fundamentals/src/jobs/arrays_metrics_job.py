from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

def dedupe_game_details(spark, game_details_df, games_df):
    """
    Deduplicate game_details based on game_id, team_id, player_id.
    Keep the earliest game_date_est if duplicates exist.
    """
    # Join with games to get game_date_est
    joined_df = game_details_df.join(
        games_df.select("game_id", "game_date_est", "season", "home_team_id"),
        on="game_id",
        how="left"
    )

    # Deduplicate using ROW_NUMBER
    window_spec = Window.partitionBy("game_id", "team_id", "player_id").orderBy("game_date_est")
    deduped_df = joined_df.withColumn("row_num", row_number().over(window_spec)) \
                          .filter(col("row_num") == 1) \
                          .drop("row_num")

    return deduped_df

# Example usage:
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("dedupe_game_details").getOrCreate()
    
    game_details_df = spark.table("game_details")
    games_df = spark.table("games")
    
    deduped_df = dedupe_game_details(spark, game_details_df, games_df)
    deduped_df.show(10, truncate=False)
