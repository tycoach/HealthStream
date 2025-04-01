from .connectors import  load_latest_data_from_minio,create_engine
from .dimension_loaders import (
    process_dim_date, process_dim_patient, process_dim_hospital,
    process_dim_physician, process_dim_diagnosis
)
from .facts_loaders import process_fact_health_records, process_fact_daily_patient_metrics
from generator.minio_client import create_minio_client
from datetime import datetime

conn_id = "POSTGRES_CONN_ID"
client = create_minio_client()
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
data_prefix = "health_records/_cleaned_/"
bucket_name = "rogerlake"

def execute_etl():
    """Main ETL function to load data from MinIO to PostgreSQL"""
    try:
       
        
        # Load data from MinIO
        print("Loading data from MinIO...")
        df = load_latest_data_from_minio(bucket_name, data_prefix)
        print(f"Loaded {len(df)} records from MinIO")
        
        # Create SQLAlchemy engine
        #engine = get_postgres_connection(conn_id)
        create_engine()
        
        # Process dimension tables
        print("Processing dimension tables...")
        date_df = process_dim_date(df)
        patient_keys = process_dim_patient(df)
        hospital_keys = process_dim_hospital(df)
        physician_keys = process_dim_physician(df,hospital_keys)
        diagnosis_keys = process_dim_diagnosis(df)
        
        # Process fact tables
        print("Processing fact tables...")
        process_fact_health_records(df,patient_keys, hospital_keys, 
                                    physician_keys, diagnosis_keys, date_df)
        process_fact_daily_patient_metrics(df,patient_keys, date_df)
        
        print("ETL process completed successfully!")
        return True
        
    except Exception as e:
        print(f"Error in ETL process: {str(e)}")
        raise
