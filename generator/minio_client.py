# MinIO connection and upload utilities

from minio import Minio
from minio.error import S3Error
import os
from dotenv import load_dotenv
import logging
import io
#from airflow.models import Variable
load_dotenv()



# minio_endpoint = os.getenv("MINIO_ENDPOINT", "172.18.0.2:9000")
# access_key = os.environ.get('MINIO_ACCESS_KEY')
# secret_key = os.environ.get('MINIO_SECRET')
bucket_name = os.environ.get('MINIO_BUCKET_NAME')
# minio_endpoint = Variable.get("MINIO_ENDPOINT")
# access_key = Variable.get("MINIO_ACCESS_KEY")
# secret_key = Variable.get("MINIO_SECRET")
access_key = "DkqjHf8ugAgcXBzu39PK"
bucket_name="rogerlake"
secret_key="Z0IQSAUl0mm1V3JMgOLu7ytCHxGS7ken3IFmjjRc"
minio_endpoint="172.19.0.2:9000"

logging.info(f"MinIO Endpoint: {minio_endpoint}")
logging.info(f"MinIO Bucket: {bucket_name}")

def create_minio_client():
    client = Minio(minio_endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    print(f"Created Minio client: {client}")
    return client

def ensure_bucket(client, bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket already exists: {bucket_name}")


import io

def upload_to_minio(client, bucket_name, object_name, buffer, content_type='text/csv'):
    """
    Uploads data to MinIO, ensuring proper handling of different buffer types.
    """
    #  Convert buffer to BytesIO if necessary
    if isinstance(buffer, io.StringIO):
        # Convert StringIO (text) → BytesIO (binary)
        buffer = io.BytesIO(buffer.getvalue().encode('utf-8'))
    elif isinstance(buffer, bytes):
        # Wrap bytes in a BytesIO object
        buffer = io.BytesIO(buffer)
    elif not isinstance(buffer, io.BytesIO):
        # Convert string to BytesIO
        buffer = io.BytesIO(str(buffer).encode('utf-8'))

    buffer.seek(0)
    #  Upload file
    client.put_object(
        bucket_name,
        object_name,
        data=buffer,
        length=len(buffer.getvalue()),  # Get byte length
        content_type=content_type
    )
    print(f" Uploaded {object_name} to {bucket_name}")


# def upload_to_minio(client, bucket_name, object_name, buffer, content_type='text/csv'):
#     client.put_object(
#         bucket_name,
#         object_name,
#         data=buffer,
#         length=len(buffer.getvalue()),
#         content_type=content_type
#     )
#     print(f"Uploaded {object_name} to {bucket_name}")