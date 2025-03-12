from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
#from generator.main import generate_and_stream_to_minio
from dotenv import load_dotenv
import os
from generator.main import generate_and_stream_to_minio

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))


minio_endpoint = os.getenv('MINIO_ENDPOINT')
access_key =os.getenv('MINIO_ACCESS_KEY')
secret_key = os.getenv('MINIO_SECRET')
bucket_name = os.getenv('MINIO_BUCKET_NAME')

load_dotenv()

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
            'minio_endpoint': minio_endpoint,
            'access_key': access_key,
            'secret_key': secret_key,
            'bucket_name': bucket_name,
            'secure': False
        }
    )

    generate_and_upload_task