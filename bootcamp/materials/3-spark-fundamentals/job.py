from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, broadcast, desc


def run_spark_job():
    spark = SparkSession.builder \
        .appName("spark_fundamentals_week") \
        .master("local[2]") \
        .getOrCreate()

    # Disable automatic broadcast join threshold
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    # Load datasets
    match_details = spark.read.option("header", True).option("inferSchema", True).csv("data/match_details.csv")
    matches = spark.read.option("header", True).option("inferSchema", True).csv("data/matches.csv")
    medal_matches_players = spark.read.option("header", True).option("inferSchema", True).csv("data/medals_matches_players.csv")
    medals = spark.read.option("header", True).option("inferSchema", True).csv("data/medals.csv")
    maps = spark.read.option("header", True).option("inferSchema", True).csv("data/maps.csv")

    # === Bucketed tables ===
    match_details.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_match_details")
    matches.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_matches")
    medal_matches_players.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_medals_players")

    # === Partitioned tables for optimization experiments ===
    # 1️⃣ Partition by playlist_id
    matches.write.partitionBy("playlist_id").mode("overwrite").saveAsTable("partitioned_matches_playlist")

    # 2️⃣ Partition by mapid
    matches.write.partitionBy("mapid").mode("overwrite").saveAsTable("partitioned_matches_map")

    # 3️⃣ Partition by playlist_id and mapid together
    matches.write.partitionBy("playlist_id", "mapid").mode("overwrite").saveAsTable("partitioned_matches_playlist_map")

    # Read back bucketed tables
    bucketed_match_details = spark.table("bucketed_match_details")
    bucketed_matches = spark.table("bucketed_matches")
    bucketed_medals_players = spark.table("bucketed_medals_players")

    # Broadcast small tables
    medals_broadcast = broadcast(medals)
    maps_broadcast = broadcast(maps)

    # Aliases
    md = bucketed_match_details.alias("md").sortWithinPartitions("match_id")
    m = bucketed_matches.alias("m").sortWithinPartitions("match_id")
    mm = bucketed_medals_players.alias("mm").sortWithinPartitions("match_id")
    med = medals_broadcast.alias("med")
    maps_b = maps_broadcast.alias("maps")

    # Join tables
    joined_df = md \
        .join(m, md["match_id"] == m["match_id"], "inner") \
        .join(mm, md["match_id"] == mm["match_id"], "left") \
        .join(med, mm["medal_id"] == med["medal_id"], "left") \
        .join(maps_b, m["mapid"] == maps_b["mapid"], "left")

    # Analytical queries
    avg_kills_df = joined_df.groupBy("md.player_gamertag") \
        .agg(avg("md.player_total_kills").alias("avg_kills")) \
        .sort(desc("avg_kills"))

    playlist_counts_df = joined_df.groupBy("m.playlist_id") \
        .agg(count("*").alias("num_matches")) \
        .sort(desc("num_matches"))

    map_counts_df = joined_df.groupBy("maps.name") \
        .agg(count("*").alias("num_matches")) \
        .sort(desc("num_matches"))

    killing_spree_df = joined_df.filter(col("med.name") == "Killing Spree") \
        .groupBy("maps.name") \
        .agg(count("*").alias("num_killing_spree")) \
        .sort(desc("num_killing_spree"))

    # Show results
    print("=== Avg Kills per Player ===")
    avg_kills_df.show()

    print("=== Playlist Counts ===")
    playlist_counts_df.show()

    print("=== Map Counts ===")
    map_counts_df.show()

    print("=== Killing Spree per Map ===")
    killing_spree_df.show()

    spark.stop()


if __name__ == "__main__":
    run_spark_job()
