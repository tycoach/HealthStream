import boto3
import io
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from generator.minio_client import create_minio_client
from datetime import datetime
import psycopg2
import os
from dotenv import load_dotenv
import logging
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("connector.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("connector")

client = create_minio_client()
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
#conn_id = "POSTGRES_CONN_ID"
data_prefix = "health_records/_cleaned_/"
bucket_name = "rogerlake"

# def get_postgres_engine(conn_id):
#     """Create and return a SQLAlchemy engine using Airflow connection"""
#     pg_hook = PostgresHook(postgres_conn_id=conn_id)
#     conn_uri = pg_hook.get_uri()
#     return create_engine(conn_uri)

# def get_postgres_connection(conn_id):
#     """Get a PostgreSQL connection using Airflow connection""" 
#     pg_hook = PostgresHook(postgres_conn_id=conn_id)
#     return pg_hook.get_conn()



# Database connection parameters 
DB_PARAMS = {
    'host': os.getenv('POSTGRES_HOST'),
    'database': 'healthstream_db',
    'user': os.getenv('POSTGRES_USER'),
    'password':os.getenv('POSTGRES_PASSWORD') ,
    'port': os.getenv('POSTGRES_PORT') 
}

def create_engine():
    """Create postgres engine from DB parameters"""
    return f"postgresql://{DB_PARAMS['user']}:{DB_PARAMS['password']}@{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['database']}"

def connect_to_db():
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_PARAMS['host'],
            database=DB_PARAMS['database'],
            user=DB_PARAMS['user'],
            password=DB_PARAMS['password'],
            port=DB_PARAMS['port']
        )
        logger.info("Successfully connected to PostgreSQL database")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL database: {e}")
        raise


def load_latest_data_from_minio(bucket_name, prefix):
    """
    Load the latest cleaned data from MinIO into a Pandas DataFrame.
    """
    # List all objects in the specified bucket and prefix
    objects = client.list_objects(bucket_name=bucket_name, prefix=prefix, recursive=True)
    
    # Extract the latest file based on last_modified timestamp
    latest_file = None
    latest_modified = None
    
    for obj in objects:
        if latest_modified is None or obj.last_modified > latest_modified:
            latest_file = obj
            latest_modified = obj.last_modified

    if latest_file is None:
        raise ValueError(f"No objects found in MinIO bucket '{bucket_name}' with prefix '{prefix}'")

    latest_key = latest_file.object_name  # Get the file path
    print(f"Latest File Found: {latest_key}")  # Debugging info

    # Download the latest file
    data = client.get_object(bucket_name=bucket_name, object_name=latest_key)
    file_stream = io.BytesIO(data.read())  # Convert to in-memory stream

    # Read file into DataFrame based on extension
    if latest_key.endswith('.csv'):
        df = pd.read_csv(file_stream)
    elif latest_key.endswith('.parquet'):
        df = pd.read_parquet(file_stream)
    else:
        raise ValueError(f"Unsupported file format: {latest_key}")

    return df