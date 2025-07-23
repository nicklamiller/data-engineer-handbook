from pyspark.sql import SparkSession


def do_daily_aggregate_transformation(spark, dataframe, ds):
    query = f"""
    SELECT
		host,
		CAST(event_time AS DATE) AS date,
		COUNT(1) AS hits,
		COUNT(DISTINCT user_id) AS unique_visitors
	FROM events_deduped
	WHERE CAST(event_time AS DATE) = '{ds}'
	GROUP BY host, CAST(event_time AS DATE)
    """
    dataframe.createOrReplaceTempView("events_deduped")
    return spark.sql(query)

def main():
    spark = SparkSession.builder.appName("test_daily_aggregate").getOrCreate()
    dataframe = spark.read.table("events")
    result = do_daily_aggregate_transformation(spark, dataframe, "2023-01-31")
    result.show()
