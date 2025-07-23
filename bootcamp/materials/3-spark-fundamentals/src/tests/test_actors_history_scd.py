from collections import namedtuple

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
    IntegerType,
)

from ..jobs.actors_history_scd import do_actors_history_scd_transformation

ActorYearly = namedtuple("ActorYearly", ["actor_id", "actor_name", "current_year", "quality_class", "is_active"])
ActorHistory = namedtuple("ActorHistory", ["actor_id", "actor_name", "quality_class", "is_active", "start_year", "end_year", "current_year"])


def test_do_actors_history_scd_transformation(spark):
    input_data = [
        ActorYearly(101, 'Harrison Ford', 1975, 'good', True),
        ActorYearly(101, 'Harrison Ford', 1976, 'good', True),
        ActorYearly(101, 'Harrison Ford', 1977, 'excellent', True),

        ActorYearly(202, 'Carrie Fisher', 1975, 'good', True),
        ActorYearly(202, 'Carrie Fisher', 1976, 'average', False),
        ActorYearly(202, 'Carrie Fisher', 1977, 'average', False),

        ActorYearly(303, 'Mark Hamill', 1975, 'average', True),
        ActorYearly(303, 'Mark Hamill', 1976, 'average', True),
        ActorYearly(303, 'Mark Hamill', 1977, 'average', True),

        ActorYearly(404, 'Alec Guinness', 1977, 'excellent', True),

        ActorYearly(505, 'Peter Cushing', 1978, 'good', True),
    ]
    source_df = spark.createDataFrame(input_data)

    actual_df = do_actors_history_scd_transformation(spark, source_df)

    expected_data = [
        ActorHistory(101, 'Harrison Ford', 'good', True, 1975, 1976, 1977),
        ActorHistory(101, 'Harrison Ford', 'excellent', True, 1977, 1977, 1977),

        ActorHistory(202, 'Carrie Fisher', 'good', True, 1975, 1975, 1977),
        ActorHistory(202, 'Carrie Fisher', 'average', False, 1976, 1977, 1977),

        ActorHistory(303, 'Mark Hamill', 'average', True, 1975, 1977, 1977),

        ActorHistory(404, 'Alec Guinness', 'excellent', True, 1977, 1977, 1977),
    ]
    expected_schema = StructType([
        StructField("actor_id", LongType(), True),
        StructField("actor_name", StringType(), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("start_year", LongType(), True),
        StructField("end_year", LongType(), True),
        StructField("current_year", IntegerType(), False) # Match IntegerType and nullable=False
    ])
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=True,
        ignore_nullable=True
    )
