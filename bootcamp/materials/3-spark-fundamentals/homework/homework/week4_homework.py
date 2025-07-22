from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


### Build spark session + config options
spark = (
    SparkSession
    .builder
    .appName("homework")
    .master("local[*]") # Use local mode
    .config("spark.driver.memory", "6g")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.parquet.block.size", "16m")
    .config("spark.sql.iceberg.write.target-file-size-bytes", "134217728")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)

NUM_BUCKETS = 16


### Define CSV reader
def read_csv(
    file_name: str,
    data_dir: str = "/home/iceberg/data"
) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(f"{data_dir}/{file_name}")
    )


### Create DataFrames based on bucketed tables

spark.sql("DROP TABLE IF EXISTS bootcamp.matches_bucketed_hw")
(
    read_csv("matches.csv")
    .select("match_id", "is_team_game", "playlist_id", "completion_date", "mapid")
    .limit(1000)
    .write.mode("overwrite")
    .partitionBy("completion_date")
    .bucketBy(NUM_BUCKETS, "match_id")
    .saveAsTable("bootcamp.matches_bucketed_hw")
)
matches_bucketed = spark.table("bootcamp.matches_bucketed_hw")


spark.sql("DROP TABLE IF EXISTS bootcamp.match_details_bucketed_hw")
(
    read_csv("match_details.csv")
    .select("match_id", "player_gamertag", "player_total_kills")
    .limit(1000)
    .write.mode("overwrite")
    .bucketBy(NUM_BUCKETS, "match_id")
    .saveAsTable("bootcamp.match_details_bucketed_hw")
)
match_details_bucketed = spark.table("bootcamp.match_details_bucketed_hw")


spark.sql("DROP TABLE IF EXISTS bootcamp.medals_matches_players_bucketed_hw")
(
    read_csv("medals_matches_players.csv")
    .select("match_id", "player_gamertag", "medal_id")
    .limit(1000)
    .write.mode("overwrite")
    .bucketBy(NUM_BUCKETS, "match_id")
    .saveAsTable("bootcamp.medals_matches_players_bucketed_hw")
)
medals_matches_players_bucketed = spark.read.table("bootcamp.medals_matches_players_bucketed_hw")

### Read in smaller datasets
medals = read_csv("medals.csv")
maps = read_csv("maps.csv")


### Answer analytics questions
# Which player averages the most kills per game?
(
    match_details_bucketed
    .groupBy("player_gamertag")
    .agg(
        (F.sum("player_total_kills") / F.count("match_id")).alias("kills_per_game")
    )
    .sort(F.col("kills_per_game").desc())
).show()

# Which playlist gets played the most?
(
    matches_bucketed
    .groupBy("playlist_id")
    .agg(
        F.count("match_id").alias("num_matches"),
    )
    .sort(F.col("num_matches").desc())
).show()

# Which map gets played the most?
(
    matches_bucketed
    .groupBy("mapid")
    .agg(
        F.count("match_id").alias("num_matches"),
    )
    .join(F.broadcast(maps), on="mapid")
    .sort(F.col("num_matches").desc())
).show()

# Which map do players get the most Killing Spree medals on?
agg_sdf = (
    matches_bucketed
    .join(match_details_bucketed, on="match_id")
    .join(medals_matches_players_bucketed, on=["match_id", "player_gamertag"])
    .join(
        F.broadcast(
            medals.withColumnRenamed("name", "medal_name")
            .withColumnRenamed("description", "medal_description")
        ), on="medal_id"
    )
    .join(
        F.broadcast(
            maps.withColumnRenamed("name", "map_name")
            .withColumnRenamed("description", "map_description")
        ), on="mapid"
    )
    .join(F.broadcast(maps), on="mapid")
)
(
    agg_sdf
    .filter(F.col("medal_name") == "Killing Spree")
    .groupBy("mapid", "map_name")
    .agg(
        F.count("match_id").alias("num_matches"),
    )
    .sort(F.col("num_matches").desc())
).show()


### Try different `.sortWithinPartitions`
start_df = agg_sdf.repartition(4, F.col("completion_date")).withColumn("completion_date", F.col("completion_date").cast("timestamp")) \
    
first_sort_df = start_df.sortWithinPartitions(F.col("completion_date"), F.col("mapid"))

second_sort_df = start_df.sortWithinPartitions(F.col("completion_date"), F.col("playlist_id"))

start_df.write.mode("overwrite").saveAsTable("bootcamp.agg_sdf_unsorted")
first_sort_df.write.mode("overwrite").saveAsTable("bootcamp.agg_sdf_sorted1")
second_sort_df.write.mode("overwrite").saveAsTable("bootcamp.agg_sdf_sorted2")

# NOTE: the below should be ran in a jupyter notebook cell, it is improper python syntax so is commented out here
# NOTE: from the results below, can see that sorting by `playlist_id` within partitions results in the most compression
# %%sql

# SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
# FROM bootcamp.agg_sdf_unsorted.files
# UNION ALL    
# SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted1'
# FROM bootcamp.agg_sdf_sorted1.files
# UNION ALL    
# SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted2' 
# FROM bootcamp.agg_sdf_sorted2.files
