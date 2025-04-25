from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 11),
    'retries' : 1
}

# dag = DAG('maxkus_data_processing_pipeline', default_args=default_args, schedule_interval='@daily')

dags_dir = os.path.dirname(os.path.abspath(__file__))

with DAG(
    dag_id='maxkuz_data_processing_pipeline',
    default_args=default_args,
    description='Maksym',
    schedule_interval=None,
    catchup=False,
    tags=["maxkuz"]
) as dag:
    bronze_task = SparkSubmitOperator(
        application=os.path.join(dags_dir, 'landing_to_bronze.py'),
        task_id='load_csv_to_bronze',
        conn_id='spark-default', 
        verbose=1,
    )
    silver_task = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application=os.path.join(dags_dir, 'bronze_to_silver.py'),
        conn_id='spark-default',
        verbose=1,
    )

    gold_task = SparkSubmitOperator(
        task_id='silver_to_gold',
        application=os.path.join(dags_dir, 'silver_to_gold.py'),
        conn_id='spark-default',
        verbose=1,
    )

bronze_task >> silver_task >> gold_task