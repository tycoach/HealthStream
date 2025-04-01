from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import datetime
import sys
from dotenv import load_dotenv
from airflow.models import Variable
import sys
from minio.error import S3Error

load_dotenv()

bucket_name = "rogerlake"

minio_endpoint = Variable.get("MINIO_ENDPOINT")
access_key = Variable.get("MINIO_ACCESS_KEY")
print(f"Using access", access_key)
secret_key = Variable.get("MINIO_SECRET")
print(f"------Connecting to MinIO at: {minio_endpoint}")
print(f"Using bucket: {bucket_name}")

#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../spark')))

# Ensure Python #can find the 'generator' module
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/generator')
sys.path.append('/opt/airflow/spark')


from generator.minio_client import create_minio_client
from spark.jobs.transform_health_data import main
# Define environment variables
RAW_PREFIX = "health_records/_raw_data_/health_record_raw_"
PROCESSED_PREFIX = "health_records/_cleaned_/health_data_"
BUCKET_NAME = "rogerlake"
TRANSFORM_SCRIPT_PATH = '/home/airflow/spark/jobs/transform_health_data.py'

client = create_minio_client()
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
parquet_object = "health_records/_cleaned_/health_data_{timestamp}.parquet"






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
    schedule_interval="15 10 * * *",
    catchup=False,
    tags=["spark", "minio", "etl"],
)




def get_latest_raw_file(**kwargs):
    """Retrieve the latest raw data file from MinIO."""
    # Import here to avoid loading during DAG definition
    from generator.minio_client import create_minio_client
    
    bucket_name = BUCKET_NAME
    client = create_minio_client()
    
    raw_files = list(client.list_objects(bucket_name, prefix=RAW_PREFIX))
    if not raw_files:
        raise FileNotFoundError("No raw files found in MinIO!")
    
    # Extract timestamps from filenames
    latest_file = max(raw_files, key=lambda x: x.last_modified)
    return f"s3a://{bucket_name}/{latest_file.object_name}"

def get_processed_parquet_path(**kwargs):
    """Generate processed file path with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"s3a://{BUCKET_NAME}/{PROCESSED_PREFIX}{timestamp}.parquet"




# Fetch Latest Raw File
fetch_latest_raw_file_task = PythonOperator(
    task_id="fetch_latest_raw_file",
    python_callable=get_latest_raw_file,
    do_xcom_push=True,  # Save result in XCom for later use
    dag=dag
)

# Generate Processed File Path
generate_parquet_path_task = PythonOperator(
    task_id="generate_parquet_path",
    python_callable=get_processed_parquet_path,
    do_xcom_push=True,  # Save result in XCom for later use
    dag=dag
)


transform_health_data_task = PythonOperator(
    task_id='transform_health_data',
    python_callable=main,
    op_kwargs={
        'input_path': '{{ ti.xcom_pull(task_ids="fetch_latest_raw_file") }}',  # Input path from XCom
        'output_path': '{{ ti.xcom_pull(task_ids="generate_parquet_path") }}',  # Output path from XCom
        'bucket': 'rogerlake',  # MinIO/S3 bucket name
    },
    dag=dag,
)



# Task Dependencies
fetch_latest_raw_file_task >> generate_parquet_path_task >> transform_health_data_task 