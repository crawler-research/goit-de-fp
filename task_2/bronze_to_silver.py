import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from prefect import flow, task

# @task(cache_policy="NO_CACHE")

def sanitize_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

def bronze_to_silver():
    spark = SparkSession.builder.appName("Bronze to Silver").getOrCreate()

    bio_data_df = spark.read.parquet("bronze/athlete_bio")
    event_results_df = spark.read.parquet("bronze/athlete_event_results")

    sanitize_text_udf = udf(sanitize_text, StringType())

    for column in bio_data_df.columns:
        bio_data_df = bio_data_df.withColumn(column, sanitize_text_udf(col(column)))

    for column in event_results_df.columns:
        event_results_df = event_results_df.withColumn(column, sanitize_text_udf(col(column)))

    bio_data_cleaned = bio_data_df.dropDuplicates()
    event_results_cleaned = event_results_df.dropDuplicates()

    bio_data_cleaned.write.mode("overwrite").parquet("silver/athlete_bio")
    event_results_cleaned.write.mode("overwrite").parquet("silver/athlete_event_results")

    spark.stop()


if __name__ == "__main__":
    bronze_to_silver()