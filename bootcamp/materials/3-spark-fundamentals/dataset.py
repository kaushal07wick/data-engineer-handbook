from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from datetime import datetime

spark = SparkSession.builder.appName("GamingExample").getOrCreate()

# -------------------------
# 1. match_details: player performance per match
# -------------------------
match_details_schema = StructType([
    StructField("match_id", IntegerType(), False),
    StructField("player_id", IntegerType(), False),
    StructField("kills", IntegerType(), True),
    StructField("deaths", IntegerType(), True),
    StructField("assists", IntegerType(), True)
])

match_details_data = [
    (1, 101, 5, 2, 1),
    (1, 102, 3, 4, 2),
    (2, 101, 7, 1, 3),
    (2, 103, 2, 5, 0)
]

match_details = spark.createDataFrame(match_details_data, schema=match_details_schema)

# -------------------------
# 2. matches: a row per match
# -------------------------
matches_schema = StructType([
    StructField("match_id", IntegerType(), False),
    StructField("map_id", IntegerType(), True),
    StructField("date", DateType(), True)
])

matches_data = [
    (1, 201, datetime.strptime("2025-08-01", "%Y-%m-%d").date()),
    (2, 202, datetime.strptime("2025-08-02", "%Y-%m-%d").date())
]

matches = spark.createDataFrame(matches_data, schema=matches_schema)

# -------------------------
# 3. medals_matches_players: medal type per player per match
# -------------------------
medals_matches_players_schema = StructType([
    StructField("match_id", IntegerType(), False),
    StructField("player_id", IntegerType(), False),
    StructField("medal_id", IntegerType(), False)
])

medals_matches_players_data = [
    (1, 101, 301),
    (1, 102, 302),
    (2, 101, 303),
    (2, 103, 301)
]

medals_matches_players = spark.createDataFrame(medals_matches_players_data, schema=medals_matches_players_schema)

# -------------------------
# 4. medals: all medal types
# -------------------------
medals_schema = StructType([
    StructField("medal_id", IntegerType(), False),
    StructField("medal_name", StringType(), True)
])

medals_data = [
    (301, "Gold"),
    (302, "Silver"),
    (303, "Bronze")
]

medals = spark.createDataFrame(medals_data, schema=medals_schema)

# -------------------------
# 5. maps: all map types
# -------------------------
maps_schema = StructType([
    StructField("map_id", IntegerType(), False),
    StructField("map_name", StringType(), True)
])

maps_data = [
    (201, "Desert Arena"),
    (202, "Forest Ruins")
]

maps = spark.createDataFrame(maps_data, schema=maps_schema)

# -------------------------
# Show sample data
# -------------------------
match_details.show()
matches.show()
medals_matches_players.show()
medals.show()
maps.show()

# -------------------------
# Write parquet files
# -------------------------
match_details.write.mode("overwrite").parquet("match_details.parquet")
matches.write.mode("overwrite").parquet("matches.parquet")
medals_matches_players.write.mode("overwrite").parquet("medals_matches_players.parquet")
medals.write.mode("overwrite").parquet("medals.parquet")
maps.write.mode("overwrite").parquet("maps.parquet")
