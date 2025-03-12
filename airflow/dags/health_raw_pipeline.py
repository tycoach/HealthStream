from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
#from generator.main import generate_and_stream_to_minio
import os
from dotenv import load_dotenv
import sys
from pathlib import Path
from minio import Minio

load_dotenv()

bucket_name = "rogerlake"
minio_endpoint = os.getenv("MINIO_ENDPOINT", "172.18.0.2:9000")
print(f"------Connecting to MinIO at: {minio_endpoint}")
print(f"Using bucket: {bucket_name}")

# Ensure Python can find the 'generator' module
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/generator')




from generator.main import generate_and_stream_to_minio

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='health_raw_upload_pipeline',
    default_args=default_args,
    description='Generate and upload synthetic health data to MinIO',
    schedule_interval='0 10 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['healthstream', 'minio', 'automation'],
) as dag:

    generate_and_upload_task = PythonOperator(
        task_id='generate_and_upload_to_minio',
        python_callable=generate_and_stream_to_minio,
        op_kwargs={
            'rows': 100000,
            'batch_size': 10000,
            'bucket_name': bucket_name
        }
    )

    generate_and_upload_task