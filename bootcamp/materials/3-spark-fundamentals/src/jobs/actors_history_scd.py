from pyspark.sql import SparkSession


def do_actors_history_scd_transformation(spark, dataframe):
    query = """
    WITH with_previous AS (
        SELECT
            actor_id,
            actor_name,
            current_year,
            quality_class,
            is_active,
            LAG(quality_class, 1) OVER(PARTITION BY actor_id ORDER BY current_year) AS quality_class_previous,
            LAG(is_active, 1) OVER(PARTITION BY actor_id ORDER BY current_year) AS is_active_previous
        FROM actors
        WHERE current_year <= 1977
        ORDER BY actor_name ASC	
    )
    , with_indicators AS (
        SELECT
            *,
            CASE
                WHEN is_active <> is_active_previous OR quality_class <> quality_class_previous THEN 1
                ELSE 0
            END AS change_indicator
        FROM with_previous
    )
    , with_streaks AS (
        SELECT
            *,
            SUM(change_indicator) OVER(PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
        FROM with_indicators
    )
    SELECT
        actor_id,
        MAX(actor_name) AS actor_name,
        quality_class,
        is_active,
        MIN(current_year) AS start_year,
        MAX(current_year) AS end_year,
        1977 AS current_year
    FROM with_streaks
    GROUP BY
        actor_id,
        streak_identifier,
        quality_class,
        is_active;
    """
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)


def main():
    spark = SparkSession.builder.appName("test_actors_history_scd").getOrCreate()
    dataframe = spark.read.table("actors")
    result = do_actors_history_scd_transformation(spark, dataframe)
    result.show()
