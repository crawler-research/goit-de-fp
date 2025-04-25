from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp
from prefect import flow, task

# @task(cache_policy="NO_CACHE")
def silver_to_gold():
    spark = SparkSession.builder.appName("Silver to Gold").getOrCreate()

    bio_df = spark.read.parquet("silver/athlete_bio")
    results_df = spark.read.parquet("silver/athlete_event_results")

    combined_df = bio_df.join(results_df, "athlete_id")

    combined_df = combined_df.withColumn("weight", combined_df["weight"].cast("float")) \
                             .withColumn("height", combined_df["height"].cast("float"))

    aggregated_df = combined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg("weight").alias("average_weight"),
        avg("height").alias("average_height")
    ).withColumn("timestamp", current_timestamp())

    aggregated_df.write.mode("overwrite").parquet("gold/avg_stats")

    spark.stop()
#

if __name__ == "__main__":
    silver_to_gold()