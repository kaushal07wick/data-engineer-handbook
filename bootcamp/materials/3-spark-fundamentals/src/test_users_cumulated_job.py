from chispa.dataframe_comparer import *
from ..jobs.users_cumulated_job import do_users_transformation
from collections import namedtuple
from datetime import date, datetime

User = namedtuple("User", "user_id event_time")

def test_user_cumulated_generation(spark):
    input_data = [
        User(1, datetime(2023, 1, 31, 0, 0))
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = do_users_transformation(spark, input_dataframe)

    expected_schema = ["user_id", "dates_active", "date"]
    expected_data = [
        ("1", [date(2023, 1, 31)], date(2023, 1, 31))
    ]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
