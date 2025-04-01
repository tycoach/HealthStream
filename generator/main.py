import uuid
import pandas as pd
from datetime import datetime
from faker import Faker
from tqdm import tqdm
from .data_generator import generate_batches
from .minio_client import create_minio_client, ensure_bucket, upload_to_minio
from .metadata_manager import create_metadata
from .schema import get_health_record_schema
from dotenv import load_dotenv
import io
from airflow.models import Variable

load_dotenv()

##variables
# MinIO configuration
minio_endpoint = Variable.get("MINIO_ENDPOINT")
access_key = Variable.get("MINIO_ACCESS_KEY")
secret_key = Variable.get("MINIO_SECRET")
bucket_name = "rogerlake"


def generate_and_stream_to_minio(rows=10000, batch_size=1000, bucket_name=bucket_name):
    """Generate synthetic data and stream it to MinIO"""

    fake = Faker()
    run_id = uuid.uuid4().hex
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    schema = get_health_record_schema()
    client = create_minio_client()
    ensure_bucket(client, bucket_name)

    df_all = pd.DataFrame(columns=[field['name'] for field in schema])  # Empty DataFrame

    try:
        # Collect all batches into a single DataFrame
        for buffer in tqdm(generate_batches(fake, rows, batch_size), desc="Generating Data"):
            df_batch = pd.read_csv(buffer)  # Read batch into DataFrame
            df_all = pd.concat([df_all, df_batch], ignore_index=True)  # Append batch
        
        # Convert final DataFrame to CSV buffer
        final_buffer = io.BytesIO()
        df_all.to_csv(final_buffer, index=False)
        final_buffer.seek(0)

        # Upload **only if successful**
        object_name = f"health_records/_raw_data_/health_record_raw_{timestamp}.csv"
        upload_to_minio(client, bucket_name, object_name, final_buffer)

        # Upload metadata
        metadata_buffer = create_metadata(
            run_id, timestamp, rows, batch_size, total_batches=1, schema_length=len(schema)
        )
        metadata_object_name = f"health_records/_metadata_/metadata_{timestamp}.json"
        upload_to_minio(client, bucket_name, metadata_object_name, metadata_buffer, content_type='application/json')

        print(f"✔ Completed: {rows} rows in 1 file")
        return {
            "status": "success",
            "timestamp": timestamp,
            "total_rows": rows,
            "batches_uploaded": 1,
            "bucket": bucket_name,
            "file_name": object_name,
        }
    
    except Exception as e:
        print(f"workflow Failed: {str(e)}")
        return {
            "status": "failed",
            "error": str(e)
        }

if __name__ == "__main__":
    result = generate_and_stream_to_minio(rows=100000, batch_size=1000)
    print(result)