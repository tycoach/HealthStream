import pandas as pd
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
        logging.FileHandler("fact_loaders.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("FactLoaders")

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


# def get_postgres_conn(postgres_conn_id="your_postgres_conn"):
#     """Helper function to get PostgreSQL connection and cursor."""
#     pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
#     conn = pg_hook.get_conn()
#     cursor = conn.cursor()
#     return conn, cursor


def process_fact_health_records(df, patient_keys, hospital_keys, 
                                physician_keys, diagnosis_keys, date_df):
    """Process and load fact health records table using Postgres Hook in Airflow"""
    # Create a date key map with Python native types
    date_key_map = {str(date): int(key) for date, key in date_df.set_index('full_date')['date_key'].to_dict().items()}
    
    fact_health_df = pd.DataFrame()
    
    # Join with patient dimension
    fact_health_df['patient_id'] = df['patient_id']
    fact_health_df = fact_health_df.merge(
        patient_keys[['patient_key', 'patient_id']], 
        on='patient_id', 
        how='left'
    )
    
    # Join with hospital dimension
    fact_health_df['hospital_id'] = df['hospital_id']
    fact_health_df = fact_health_df.merge(
        hospital_keys[['hospital_key', 'hospital_id']], 
        on='hospital_id', 
        how='left'
    )
    
    # Extract physician_id safely
    fact_health_df['physician_id'] = df['primary_physician'].apply(
        lambda x: x.split('-')[0].strip() if isinstance(x, str) and '-' in x else None
    )
    
    # Join with physician dimension
    fact_health_df = fact_health_df.merge(
        physician_keys[['physician_key', 'physician_id']], 
        on='physician_id', 
        how='left'
    )
    
    # Extract diagnosis_code safely
    fact_health_df['diagnosis_code'] = df['diagnosis'].apply(
        lambda x: x.split('-')[0].strip() if isinstance(x, str) and '-' in x else None
    )
    
    # Join with diagnosis dimension
    fact_health_df = fact_health_df.merge(
        diagnosis_keys[['diagnosis_key', 'diagnosis_code']], 
        on='diagnosis_code', 
        how='left'
    )
    
    # Convert dates to string format for mapping
    fact_health_df['admission_date'] = pd.to_datetime(df['admission_date'], errors='coerce')
    fact_health_df['discharge_date'] = pd.to_datetime(df['discharge_date'], errors='coerce')
    
    # Map date strings to date keys
    fact_health_df['admission_date_key'] = fact_health_df['admission_date'].apply(
        lambda x: date_key_map.get(x.strftime('%Y-%m-%d')) if pd.notna(x) else None
    )
    fact_health_df['discharge_date_key'] = fact_health_df['discharge_date'].apply(
        lambda x: date_key_map.get(x.strftime('%Y-%m-%d')) if pd.notna(x) else None
    )
    
    # Calculate length of stay
    fact_health_df['length_of_stay'] = (fact_health_df['discharge_date'] - 
                                        fact_health_df['admission_date']).dt.days
    
    # Add additional metrics
    fact_health_df['total_cost'] = 0  # Default value
    
    # Handle medication count safely
    fact_health_df['medication_count'] = df['medications'].apply(
        lambda x: x.count(',') + 1 if isinstance(x, str) else 0
    )
    
    # Handle procedure count safely
    fact_health_df['procedure_count'] = df['procedure'].apply(
        lambda x: x.count(',') + 1 if isinstance(x, str) else 0
    )
    
    # Null handling - explicitly convert to Python int/float types
    fact_health_df['medication_count'] = fact_health_df['medication_count'].fillna(0).astype(int)
    fact_health_df['procedure_count'] = fact_health_df['procedure_count'].fillna(0).astype(int)
    fact_health_df['length_of_stay'] = fact_health_df['length_of_stay'].fillna(0).astype(int)
    fact_health_df['total_cost'] = fact_health_df['total_cost'].fillna(0).astype(float)
    
    # Fill NaN values in foreign keys
    for col in ['patient_key', 'hospital_key', 'physician_key', 'diagnosis_key', 
                'admission_date_key', 'discharge_date_key']:
        fact_health_df[col] = fact_health_df[col].fillna(-1).astype(int)
    
    # Keep only needed columns
    fact_health_df = fact_health_df[[ 
        'patient_key', 'hospital_key', 'physician_key', 'diagnosis_key',
        'admission_date_key', 'discharge_date_key', 'length_of_stay',
        'total_cost', 'medication_count', 'procedure_count'
    ]]
    
    # Connect to database
    conn = connect_to_db()
    cursor = conn.cursor()
    
    try:
        # Check what the PK columns are
        pk_columns_query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'fact_health_records'::regclass AND i.indisprimary;
        """
        cursor.execute(pk_columns_query)
        pk_columns = [row[0] for row in cursor.fetchall()]
        print(f"Existing primary key columns: {pk_columns}")
        
        # Since the PK is 'record_key', we can't use ON CONFLICT
        # Use a simple INSERT without ON CONFLICT
        insert_query = """
            INSERT INTO fact_health_records (patient_key, hospital_key, physician_key, diagnosis_key,
                                             admission_date_key, discharge_date_key, length_of_stay, 
                                             total_cost, medication_count, procedure_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        
        # Convert to list of tuples with Python native types
        records = []
        for _, row in fact_health_df.iterrows():
            record = (
                int(row['patient_key']),
                int(row['hospital_key']),
                int(row['physician_key']),
                int(row['diagnosis_key']),
                int(row['admission_date_key']),
                int(row['discharge_date_key']),
                int(row['length_of_stay']),
                float(row['total_cost']),
                int(row['medication_count']),
                int(row['procedure_count'])
            )
            records.append(record)
        
        cursor.executemany(insert_query, records)
        conn.commit()
        print(f"Inserted {len(fact_health_df)} records into fact_health_records")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting records: {e}")
    finally:
        cursor.close()
        conn.close()
    
    return fact_health_df

def process_fact_daily_patient_metrics(df, patient_keys, date_df):
    """Process and load fact daily patient metrics table """
    fact_metrics_df = pd.DataFrame()
    
    # Join with patient dimension
    fact_metrics_df['patient_id'] = df['patient_id']
    fact_metrics_df = fact_metrics_df.merge(
        patient_keys[['patient_key', 'patient_id']], 
        on='patient_id', 
        how='left'
    )
    
    # Map date keys - using admission date as the date for metrics
    date_key_map = {str(date): int(key) for date, key in date_df.set_index('full_date')['date_key'].to_dict().items()}
    
    # Convert dates and map to keys
    fact_metrics_df['admission_date'] = pd.to_datetime(df['admission_date'], errors='coerce')
    fact_metrics_df['date_key'] = fact_metrics_df['admission_date'].apply(
        lambda x: date_key_map.get(x.strftime('%Y-%m-%d')) if pd.notna(x) else None
    )
    
    # Add health metrics with explicit type conversion
    fact_metrics_df['height_cm'] = pd.to_numeric(df['height_cm'], errors='coerce')
    fact_metrics_df['weight_kg'] = pd.to_numeric(df['weight_kg'], errors='coerce')
    fact_metrics_df['bmi'] = pd.to_numeric(df['bmi'], errors='coerce')
    
    # Fill NaN values with sensible defaults
    fact_metrics_df['height_cm'] = fact_metrics_df['height_cm'].fillna(0)
    fact_metrics_df['weight_kg'] = fact_metrics_df['weight_kg'].fillna(0)
    fact_metrics_df['bmi'] = fact_metrics_df['bmi'].fillna(0)
    
    # Add placeholder metrics (not available in the source data)
    fact_metrics_df['blood_pressure_systolic'] = None
    fact_metrics_df['blood_pressure_diastolic'] = None
    fact_metrics_df['heart_rate'] = None
    fact_metrics_df['temperature'] = None
    fact_metrics_df['oxygen_saturation'] = None
    
    # Fill NaN values in foreign keys
    fact_metrics_df['patient_key'] = fact_metrics_df['patient_key'].fillna(-1).astype(int)
    fact_metrics_df['date_key'] = fact_metrics_df['date_key'].fillna(-1).astype(int)
    
    # Keep only needed columns
    fact_metrics_df = fact_metrics_df[[ 
        'patient_key', 'date_key', 'height_cm', 'weight_kg', 'bmi',
        'blood_pressure_systolic', 'blood_pressure_diastolic', 'heart_rate',
        'temperature', 'oxygen_saturation'
    ]]
    
    # Check if there's a unique constraint on the composite key
    conn = connect_to_db()
    cursor = conn.cursor()
    
    # Check the primary key structure
    try:
        pk_columns_query = """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = 'fact_daily_patient_metrics'::regclass AND i.indisprimary;
        """
        cursor.execute(pk_columns_query)
        pk_columns = [row[0] for row in cursor.fetchall()]
        print(f"Existing primary key columns: {pk_columns}")
    except Exception as e:
        print(f"Error checking primary key: {e}")
        pk_columns = []

    # Simple insert without ON CONFLICT
    insert_query = """
        INSERT INTO fact_daily_patient_metrics (patient_key, date_key, height_cm, weight_kg, bmi,
                                                blood_pressure_systolic, blood_pressure_diastolic, heart_rate,
                                                temperature, oxygen_saturation)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    try:
        # Convert to list of tuples with Python native types
        records = []
        for _, row in fact_metrics_df.iterrows():
            record = (
                int(row['patient_key']),
                int(row['date_key']),
                float(row['height_cm']),
                float(row['weight_kg']),
                float(row['bmi']),
                row['blood_pressure_systolic'],  # Keeping as None
                row['blood_pressure_diastolic'],  # Keeping as None
                row['heart_rate'],  # Keeping as None
                row['temperature'],  # Keeping as None
                row['oxygen_saturation']  # Keeping as None
            )
            records.append(record)
        
        cursor.executemany(insert_query, records)
        conn.commit()
        print(f"Inserted {len(fact_metrics_df)} records into fact_daily_patient_metrics")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting records: {e}")
    finally:
        cursor.close()
        conn.close()
    
    return fact_metrics_df