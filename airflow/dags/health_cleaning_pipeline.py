from datetime import datetime, timedelta
from airflow import DAG
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
import os
from datetime import datetime
import sys
from pathlib import Path
from minio import Minio
import subprocess
from dotenv import load_dotenv

load_dotenv()

bucket_name = "rogerlake"
minio_endpoint = os.getenv("MINIO_ENDPOINT", "172.18.0.2:9000")
print(f"------Connecting to MinIO at: {minio_endpoint}")
print(f"Using bucket: {bucket_name}")

# Ensure Python can find the 'generator' module
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/generator')

from generator.minio_client import create_minio_client
# Define environment variables
RAW_PREFIX = "health_records/_raw_data_/health_record_raw_"
PROCESSED_PREFIX = "health_records/_cleaned_/health_data_"

client = create_minio_client()
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
parquet_object = "health_records/_cleaned_/health_data_{timestamp}.parquet"



def get_latest_raw_file(minio_client):
    """Retrieve the latest raw data file from MinIO."""
    raw_files = list(minio_client.list_objects(bucket_name, prefix=RAW_PREFIX))
    if not raw_files:
        raise FileNotFoundError(" No raw files found in MinIO!")
    # Extract timestamps from filenames
    latest_file = max(raw_files, key=lambda x: x.last_modified)
    return latest_file.object_name

# Function to generate processed file path with timestamp
def get_processed_parquet_path():
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"{PROCESSED_PREFIX}{timestamp}.parquet"


# Default args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    "health_data_etl",
    default_args=default_args,
    description="ETL pipeline for processing health records with Spark",
    schedule_interval="@daily",
    catchup=False,
    tags=["spark", "minio", "etl"],
)

#  Run Spark Job
# spark_transform_task = SparkSubmitOperator(
#     task_id="run_spark_transformation",
#     application="/opt/spark/work/health_data_processing.py",  # Path to Spark script
#     conn_id="spark_default",
#     conf={"spark.executor.memory": "4g", "spark.driver.memory": "2g"},
#     application_args=[MINIO_RAW_PATH, MINIO_PROCESSED_PATH],
#     dag=dag,
# )




def run_spark_job():
    """Dynamically fetch the latest file and run Spark transformation."""
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "healthstream_minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET"),
        secure=False
    )

    latest_raw_file = get_latest_raw_file(client)
    processed_parquet_file = get_processed_parquet_path()


    spark_command = [
        "spark-submit",
        "--master", "spark://healthstream-spark-master:7077",
        "--executor-memory", "4g",
        "--driver-memory", "2g",
        "/opt/spark/jobs/transform_health_data.py",
        f"s3a://{bucket_name}/{latest_raw_file}",  # Latest raw file
        f"s3a://{bucket_name}/{processed_parquet_file}"  # Processed output
    ]
    subprocess.run(spark_command, check=True)

spark_transform_task = PythonOperator(
    task_id="run_spark_transformation",
    python_callable=run_spark_job,
    dag=dag,
)


#  Verify Output
def verify_output():
    """Check if Parquet file exists in MinIO."""
    if client.stat_object(bucket_name, parquet_object):
        print(f" Processed data available: {parquet_object}")
    else:
        raise FileNotFoundError(" Parquet file not found!")

verify_output_task = PythonOperator(
    task_id="verify_parquet_output",
    python_callable=verify_output,
    dag=dag,
)

# Task Dependencies
spark_transform_task >> verify_output_task
