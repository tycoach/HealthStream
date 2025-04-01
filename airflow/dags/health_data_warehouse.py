from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os


# Add project directory to path to import custom modules
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))


sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/generator')
sys.path.append('/opt/airflow/utils')

from generator.minio_client import create_minio_client
from utils.extract_transform import execute_etl
# MinIO configuration
client = create_minio_client()
bucket_name = 'rogerlake'
# PostgreSQL connection ID (configured in Airflow connections)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    "start_date": datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define the DAG
dag = DAG(
    'minio_to_postgres_etl',
    default_args=default_args,
    description='ETL process from MinIO to PostgreSQL using modular Python code',
    schedule_interval= "30 10 * * *",  # Every day at 10:30 AM
    catchup=False,
)

def etl_task_function(**kwargs):
    """Wrapper function for execute_etl to be used with PythonOperator"""
    return execute_etl()

# Create PythonOperator Task
load_data_task = PythonOperator(
    task_id='load_data_to_postgres',
    python_callable=etl_task_function,
    dag=dag,
)

# Define task dependencies
load_data_task