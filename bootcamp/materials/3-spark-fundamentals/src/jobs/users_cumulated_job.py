from pyspark.sql import SparkSession

# First create the table (no PK constraints)
create_table_sql = """
CREATE TABLE IF NOT EXISTS users_cumulated (
    user_id STRING,
    dates_active ARRAY<DATE>, -- list of dates when the user was active
    date DATE
)
"""

# Transformation query
insert_sql = """
WITH yesterday AS (
    SELECT *
    FROM users_cumulated
    WHERE date = DATE('2021-01-30')
),
today AS (
    SELECT 
        CAST(user_id AS STRING) as user_id,
        DATE(CAST(event_time AS TIMESTAMP)) as date_active
    FROM events
    WHERE DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
      AND user_id IS NOT NULL
    GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
)
SELECT 
    COALESCE(t.user_id, y.user_id) AS user_id,
    CASE 
        WHEN y.dates_active IS NULL THEN array(t.date_active)
        WHEN t.date_active IS NULL THEN y.dates_active
        ELSE concat(y.dates_active, array(t.date_active))
    END AS dates_active,
    COALESCE(t.date_active, date_add(y.date, 1)) AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
"""

def do_users_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("events")
    spark.sql(create_table_sql)  # Ensure table exists
    return spark.sql(insert_sql)

def main():
    spark = SparkSession.builder \
        .master("local") \
        .appName("users_cumulated") \
        .enableHiveSupport() \
        .getOrCreate()
    
    output_df = do_users_transformation(spark, spark.table("events"))
    output_df.write.mode("overwrite").insertInto("users_cumulated")
