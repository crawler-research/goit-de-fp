import requests
from pyspark.sql import SparkSession
from prefect import flow, task

# @task(cache_policy="NO_CACHE")
def download_data(local_file_path):
    url = "https://ftp.goit.study/neoversity/"
    response = requests.get(url + local_file_path + ".csv")
    if response.status_code == 200:
        with open(local_file_path + ".csv", 'wb') as file:
            file.write(response.content)
        print(f"Downloaded: {local_file_path}.csv")
    else:
        exit(f"Download failed. Status code: {response.status_code}")

def load_csv_to_bronze():
    spark = SparkSession.builder.appName("Landing to Bronze").getOrCreate()

    download_data("athlete_bio")
    download_data("athlete_event_results")

    athlete_bio_df = spark.read.csv("athlete_bio.csv", header=True, inferSchema=True)
    athlete_event_results_df = spark.read.csv("athlete_event_results.csv", header=True, inferSchema=True)

    athlete_bio_df.write.mode("overwrite").parquet("bronze/athlete_bio")
    athlete_event_results_df.write.mode("overwrite").parquet("bronze/athlete_event_results")

    spark.stop()

if __name__ == "__main__":
    load_csv_to_bronze()