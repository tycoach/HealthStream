from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime
import psycopg2
import os
from dotenv import load_dotenv
import logging

load_dotenv()
#postgres_conn = "POSTGRES_CONN_ID"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("dimension_loaders.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DimensionLoaders")

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


# def get_postgres_conn():
#     """Helper function to get PostgreSQL connection and cursor."""
#     # Establish a connection using PostgresHook
#     pg_hook = PostgresHook(postgres_conn_id=postgres_conn)
#     conn = pg_hook.get_conn()
#     cursor = conn.cursor()
#     return conn, cursor


def process_dim_date(df):
    """Process and load dimension date table"""
    # First convert date columns to datetime safely
    admission_dates = pd.to_datetime(df['admission_date'], errors='coerce').dropna()
    discharge_dates = pd.to_datetime(df['discharge_date'], errors='coerce').dropna()
    
    # Convert to Python datetime objects (not numpy.datetime64)
    all_dates = sorted(set([d.to_pydatetime() for d in admission_dates] + 
                           [d.to_pydatetime() for d in discharge_dates]))
    
    # Create dataframe
    date_df = pd.DataFrame({'full_date': all_dates})
    
    # Extract date components
    date_df['date_key'] = date_df['full_date'].apply(lambda x: int(x.strftime('%Y%m%d')))
    date_df['year'] = date_df['full_date'].apply(lambda x: x.year)
    date_df['month'] = date_df['full_date'].apply(lambda x: x.month)
    date_df['day'] = date_df['full_date'].apply(lambda x: x.day)
    date_df['quarter'] = date_df['full_date'].apply(lambda x: (x.month - 1) // 3 + 1)
    date_df['week_number'] = date_df['full_date'].apply(lambda x: x.isocalendar()[1])
    date_df['is_weekend'] = date_df['full_date'].apply(lambda x: x.weekday() >= 5)
    
    # Convert full_date to string format for database insertion
    date_df['full_date'] = date_df['full_date'].apply(lambda x: x.strftime('%Y-%m-%d'))

    # Get connection and cursor
    conn = connect_to_db()
    cursor = conn.cursor()
    
    # Check and create unique constraint if needed
    try:
        check_constraint_query = """
        SELECT COUNT(*) FROM pg_constraint 
        WHERE conname = 'dim_date_date_key_key' 
        AND conrelid = 'dim_date'::regclass;
        """
        cursor.execute(check_constraint_query)
        has_constraint = cursor.fetchone()[0] > 0
        
        if not has_constraint:
            print("Adding unique constraint to date_key")
            add_constraint_query = """
            ALTER TABLE dim_date ADD CONSTRAINT dim_date_date_key_key UNIQUE (date_key);
            """
            cursor.execute(add_constraint_query)
            conn.commit()
    except Exception as e:
        print(f"Error checking/creating constraint: {e}")

    insert_query = """
        INSERT INTO dim_date (date_key, full_date, year, month, day, quarter, week_number, is_weekend, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (date_key) DO NOTHING;
    """
    
    try:
        # Convert DataFrame to list of tuples with Python native types
        records = [(
            int(row['date_key']),
            row['full_date'],
            int(row['year']),
            int(row['month']),
            int(row['day']),
            int(row['quarter']),
            int(row['week_number']),
            bool(row['is_weekend'])
        ) for _, row in date_df.iterrows()]
        
        cursor.executemany(insert_query, records)
        conn.commit()
        print(f"Inserted {len(date_df)} records into dim_date")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting records: {e}")
    finally:
        cursor.close()
        conn.close()

    return date_df

def process_dim_patient(df):
    """Process and load dimension patient table"""
    current_date = datetime.now().strftime("%Y-%m-%d")
    far_future = "2099-12-31"
    
    patient_df = df[['patient_id', 'first_name', 'last_name', 'gender', 'date_of_birth', 'blood_type', 'age', 
                     'email', 'phone_number', 'city', 'state', 'country']].drop_duplicates()
    patient_df['is_active'] = True
    patient_df['effective_from'] = current_date
    patient_df['effective_to'] = far_future
    
    patient_df['current_age'] = pd.to_numeric(patient_df['age'], errors='coerce').fillna(0).astype(int)
    patient_df = patient_df.drop('age', axis=1)
    
    # Connect to database
    conn = connect_to_db()
    cursor = conn.cursor()

    # Check/add constraint
    try:
        check_constraint_query = """
        SELECT COUNT(*) FROM pg_constraint 
        WHERE conname = 'dim_patient_patient_id_key' 
        AND conrelid = 'dim_patient'::regclass;
        """
        cursor.execute(check_constraint_query)
        has_constraint = cursor.fetchone()[0] > 0
        
        if not has_constraint:
            print("Adding unique constraint to patient_id")
            add_constraint_query = """
            ALTER TABLE dim_patient ADD CONSTRAINT dim_patient_patient_id_key UNIQUE (patient_id);
            """
            cursor.execute(add_constraint_query)
            conn.commit()
    except Exception as e:
        print(f"Error checking/creating constraint: {e}")
        
    # Use individual inserts for better error handling
    insert_count = 0
    error_count = 0
    
    insert_query = """
        INSERT INTO dim_patient (patient_id, first_name, last_name, gender, date_of_birth, blood_type,
                                 current_age, email, phone_number, city, state, country, is_active, 
                                 effective_from, effective_to, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (patient_id) DO NOTHING;
    """
    
    # Process each patient individually
    for _, row in patient_df.iterrows():
        try:
            # Convert date_of_birth to string if valid, else None
            dob = None
            if pd.notna(row['date_of_birth']):
                try:
                    date_val = pd.to_datetime(row['date_of_birth'], errors='coerce')
                    if pd.notna(date_val):
                        dob = date_val.strftime('%Y-%m-%d')
                except:
                    # If any error occurs during conversion, set to None
                    dob = None
                    
            # Create record tuple with proper types
            record = (
                row['patient_id'],
                row['first_name'],
                row['last_name'],
                row['gender'],
                dob,  # This will be None for NaT values
                row['blood_type'],
                int(row['current_age']),
                row['email'],
                row['phone_number'],
                row['city'],
                row['state'],
                row['country'],
                bool(row['is_active']),
                row['effective_from'],
                row['effective_to']
            )
            
            # Execute insert for this record
            cursor.execute(insert_query, record)
            insert_count += 1
            
            # Commit every 100 records to avoid large transactions
            if insert_count % 100 == 0:
                conn.commit()
                print(f"Inserted {insert_count} records into dim_patient")
                
        except Exception as e:
            error_count += 1
            print(f"Error inserting patient {row['patient_id']}: {e}")
            # Continue processing other records
            continue
    
    # Final commit for any remaining records
    conn.commit()
    print(f"Inserted {insert_count} records into dim_patient (with {error_count} errors)")
    
    # Fetch patient keys
    try:
        cursor.execute("SELECT patient_key, patient_id FROM dim_patient")
        patient_keys_data = cursor.fetchall()
        patient_keys = pd.DataFrame(patient_keys_data, columns=['patient_key', 'patient_id'])
    except Exception as e:
        print(f"Error fetching patient keys: {e}")
        patient_keys = pd.DataFrame(columns=['patient_key', 'patient_id'])
    finally:
        cursor.close()
        conn.close()
    
    return patient_keys

def process_dim_hospital(df):
    """Process and load dimension hospital table using Postgres Hook in Airflow"""
    current_date = datetime.now().strftime("%Y-%m-%d")
    far_future = "2099-12-31"
    
    # Filter out rows with NULL hospital_id
    hospital_df = df[['hospital_id', 'hospital_name', 'city', 'state', 'country']].dropna(subset=['hospital_id']).drop_duplicates()
    
    # Add SCD fields
    hospital_df['is_active'] = True
    hospital_df['effective_from'] = current_date
    hospital_df['effective_to'] = far_future
    
    # Connect to database
    conn = connect_to_db()
    cursor = conn.cursor()
    
    # Check/create constraint
    try:
        check_constraint_query = """
        SELECT COUNT(*) FROM pg_constraint 
        WHERE conname = 'dim_hospital_hospital_id_key' 
        AND conrelid = 'dim_hospital'::regclass;
        """
        cursor.execute(check_constraint_query)
        has_constraint = cursor.fetchone()[0] > 0
        
        if not has_constraint:
            print("Adding unique constraint to hospital_id")
            add_constraint_query = """
            ALTER TABLE dim_hospital ADD CONSTRAINT dim_hospital_hospital_id_key UNIQUE (hospital_id);
            """
            cursor.execute(add_constraint_query)
            conn.commit()
    except Exception as e:
        print(f"Error checking/creating constraint: {e}")
    
    # Skip if dataframe is empty after filtering
    if hospital_df.empty:
        print("No valid hospital data after filtering NULL values")
        
        # Create a default hospital
        default_hospital = {
            'hospital_id': 'DEFAULT1',
            'hospital_name': 'Default Hospital',
            'city': 'Default City',
            'state': 'Default State',
            'country': 'Default Country',
            'is_active': True,
            'effective_from': current_date,
            'effective_to': far_future
        }
        
        insert_hospital_query = """
            INSERT INTO dim_hospital (hospital_id, hospital_name, city, state, country, is_active, 
                                      effective_from, effective_to)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (hospital_id) DO NOTHING;
        """
        
        try:
            cursor.execute(insert_hospital_query, (
                default_hospital['hospital_id'],
                default_hospital['hospital_name'],
                default_hospital['city'],
                default_hospital['state'],
                default_hospital['country'],
                default_hospital['is_active'],
                default_hospital['effective_from'],
                default_hospital['effective_to']
            ))
            conn.commit()
            print("Inserted default hospital record")
        except Exception as e:
            conn.rollback()
            print(f"Error inserting default hospital: {e}")
            
        # Fetch hospital keys
        cursor.execute("SELECT hospital_key, hospital_id FROM dim_hospital")
        hospital_keys_data = cursor.fetchall()
        hospital_keys = pd.DataFrame(hospital_keys_data, columns=['hospital_key', 'hospital_id'])
        cursor.close()
        conn.close()
        return hospital_keys

    insert_query = """
        INSERT INTO dim_hospital (hospital_id, hospital_name, city, state, country, is_active, 
                                  effective_from, effective_to)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (hospital_id) DO NOTHING;
    """
    
    try:
        # Convert to list of tuples
        records = [(
            row['hospital_id'],
            row['hospital_name'],
            row['city'],
            row['state'],
            row['country'],
            bool(row['is_active']),
            row['effective_from'],
            row['effective_to']
        ) for _, row in hospital_df.iterrows()]
        
        cursor.executemany(insert_query, records)
        conn.commit()
        print(f"Inserted {len(hospital_df)} records into dim_hospital")
        
        # Fetch hospital keys
        cursor.execute("SELECT hospital_key, hospital_id FROM dim_hospital")
        hospital_keys_data = cursor.fetchall()
        hospital_keys = pd.DataFrame(hospital_keys_data, columns=['hospital_key', 'hospital_id'])
        
    except Exception as e:
        conn.rollback()
        print(f"Error inserting records: {e}")
        hospital_keys = pd.DataFrame(columns=['hospital_key', 'hospital_id'])
    finally:
        cursor.close()
        conn.close()
    
    return hospital_keys

def process_dim_physician(df, hospital_keys):
    """Process and load dimension physician table"""
    current_date = datetime.now().strftime("%Y-%m-%d")
    far_future = "2099-12-31"
    
    # Check if df['primary_physician'] exists and is not empty
    if 'primary_physician' not in df.columns or df['primary_physician'].isna().all():
        print("No primary physician data found")
        return pd.DataFrame(columns=['physician_key', 'physician_id'])
    
    # Filter out NaN values before splitting
    valid_physicians = df[df['primary_physician'].notna()]
    
    # Create physician dataframe with proper error handling
    physician_records = []
    for _, row in valid_physicians.iterrows():
        try:
            parts = row['primary_physician'].split('-', 1)  # Split on first hyphen only
            if len(parts) >= 2:
                physician_records.append({
                    'physician_id': parts[0].strip(),
                    'physician_name': parts[1].strip()
                })
            else:
                # Handle case where there's no hyphen
                physician_records.append({
                    'physician_id': parts[0].strip(),
                    'physician_name': 'Unknown'
                })
        except (AttributeError, IndexError) as e:
            print(f"Error processing physician: {row['primary_physician']} - {e}")
    
    physician_df = pd.DataFrame(physician_records).drop_duplicates()
    
    # Check if we have any physicians after filtering
    if physician_df.empty:
        print("No valid physician data after processing")
        return pd.DataFrame(columns=['physician_key', 'physician_id'])
    
    # Add SCD fields
    physician_df['specialty'] = 'Unknown'  # Default value
    physician_df['is_active'] = True
    physician_df['effective_from'] = current_date
    physician_df['effective_to'] = far_future
    
    # Connect to database to check if we need to create a default hospital
    conn = connect_to_db()
    cursor = conn.cursor()
    
    # Check if hospital table has any entries
    cursor.execute("SELECT COUNT(*) FROM dim_hospital")
    hospital_count = cursor.fetchone()[0]
    
    # Check if hospital_keys has any rows
    if hospital_keys is None or hospital_keys.empty or hospital_count == 0:
        print("No hospital keys available, creating a default hospital")
        
        # Check if hospital_id constraint exists
        try:
            check_constraint_query = """
            SELECT COUNT(*) FROM pg_constraint 
            WHERE conname = 'dim_hospital_hospital_id_key' 
            AND conrelid = 'dim_hospital'::regclass;
            """
            cursor.execute(check_constraint_query)
            has_constraint = cursor.fetchone()[0] > 0
            
            if not has_constraint:
                print("Adding unique constraint to hospital_id")
                add_constraint_query = """
                ALTER TABLE dim_hospital ADD CONSTRAINT dim_hospital_hospital_id_key UNIQUE (hospital_id);
                """
                cursor.execute(add_constraint_query)
                conn.commit()
        except Exception as e:
            print(f"Error checking/creating constraint: {e}")
            has_constraint = False
        
        # Create a default hospital if none exists
        default_hospital = {
            'hospital_id': 'DEFAULT1',
            'hospital_name': 'Default Hospital',
            'city': 'Default City',
            'state': 'Default State',
            'country': 'Default Country',
            'is_active': True,
            'effective_from': current_date,
            'effective_to': far_future
        }
        
        # Check if we have the ON CONFLICT support
        if has_constraint:
            insert_hospital_query = """
                INSERT INTO dim_hospital (hospital_id, hospital_name, city, state, country, is_active, 
                                        effective_from, effective_to)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (hospital_id) DO NOTHING
                RETURNING hospital_key;
            """
        else:
            # If no constraint, just insert without ON CONFLICT
            insert_hospital_query = """
                INSERT INTO dim_hospital (hospital_id, hospital_name, city, state, country, is_active, 
                                        effective_from, effective_to)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING hospital_key;
            """
        
        try:
            cursor.execute(insert_hospital_query, (
                default_hospital['hospital_id'],
                default_hospital['hospital_name'],
                default_hospital['city'],
                default_hospital['state'],
                default_hospital['country'],
                default_hospital['is_active'],
                default_hospital['effective_from'],
                default_hospital['effective_to']
            ))
            conn.commit()
            
            # Get the inserted hospital key
            default_hospital_key = cursor.fetchone()
            if default_hospital_key:
                default_hospital_key = default_hospital_key[0]
            else:
                # If RETURNING didn't work, query for it
                cursor.execute("SELECT hospital_key FROM dim_hospital WHERE hospital_id = %s", (default_hospital['hospital_id'],))
                default_hospital_key = cursor.fetchone()[0]
            
            physician_df['hospital_key'] = default_hospital_key
        except Exception as e:
            conn.rollback()
            print(f"Error creating default hospital: {e}")
            # Use a placeholder, but this will fail if FK constraint exists
            physician_df['hospital_key'] = None
    else:
        # Use the first available hospital key
        first_hospital = hospital_keys.iloc[0]
        physician_df['hospital_key'] = first_hospital['hospital_key']
    
    # Check/add constraint for physician_id
    try:
        check_constraint_query = """
        SELECT COUNT(*) FROM pg_constraint 
        WHERE conname = 'dim_physician_physician_id_key' 
        AND conrelid = 'dim_physician'::regclass;
        """
        cursor.execute(check_constraint_query)
        has_constraint = cursor.fetchone()[0] > 0
        
        if not has_constraint:
            print("Adding unique constraint to physician_id")
            add_constraint_query = """
            ALTER TABLE dim_physician ADD CONSTRAINT dim_physician_physician_id_key UNIQUE (physician_id);
            """
            cursor.execute(add_constraint_query)
            conn.commit()
    except Exception as e:
        print(f"Error checking/creating constraint: {e}")
    
    insert_query = """
        INSERT INTO dim_physician (physician_id, physician_name, specialty, hospital_key, is_active, 
                                   effective_from, effective_to)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (physician_id) DO NOTHING;
    """
    
    physician_keys = None
    
    try:
        # Convert to list of tuples with Python native types
        records = [(
            row['physician_id'],
            row['physician_name'],
            row['specialty'],
            int(row['hospital_key']) if pd.notna(row['hospital_key']) else None,
            bool(row['is_active']),
            row['effective_from'],
            row['effective_to']
        ) for _, row in physician_df.iterrows()]
        
        cursor.executemany(insert_query, records)
        conn.commit()
        print(f"Inserted {len(physician_df)} records into dim_physician")
        
        # Fetch physician keys using the same connection before closing
        cursor.execute("SELECT physician_key, physician_id FROM dim_physician")
        physician_keys_data = cursor.fetchall()
        physician_keys = pd.DataFrame(physician_keys_data, columns=['physician_key', 'physician_id'])
        
    except Exception as e:
        conn.rollback()
        print(f"Error inserting records: {e}")
        physician_keys = pd.DataFrame(columns=['physician_key', 'physician_id'])
    finally:
        cursor.close()
        conn.close()
    
    return physician_keys

def process_dim_diagnosis(df):
    """Process and load dimension diagnosis table using Postgres Hook in Airflow"""
    # Check if diagnosis column exists and has data
    if 'diagnosis' not in df.columns or df['diagnosis'].isna().all():
        print("No diagnosis data found")
        return pd.DataFrame(columns=['diagnosis_key', 'diagnosis_code', 'diagnosis_name'])
    
    # Filter out NaN values before splitting
    valid_diagnoses = df[df['diagnosis'].notna()]
    
    # Create diagnosis dataframe with proper error handling for split operations
    diagnosis_records = []
    for _, row in valid_diagnoses.iterrows():
        try:
            parts = row['diagnosis'].split('-', 1)  # Split on first hyphen only
            if len(parts) >= 2:
                diagnosis_records.append({
                    'diagnosis_code': parts[0].strip(),
                    'diagnosis_name': parts[1].strip()
                })
            else:
                # Handle case where there's no hyphen
                diagnosis_records.append({
                    'diagnosis_code': parts[0].strip(),
                    'diagnosis_name': 'Unknown'
                })
        except (AttributeError, IndexError) as e:
            print(f"Error processing diagnosis: {row['diagnosis']} - {e}")
    
    diagnosis_df = pd.DataFrame(diagnosis_records).drop_duplicates()
    
    # Check if we have any diagnoses after filtering
    if diagnosis_df.empty:
        print("No valid diagnosis data after processing")
        return pd.DataFrame(columns=['diagnosis_key', 'diagnosis_code', 'diagnosis_name'])
    
    # Add category fields
    diagnosis_df['category'] = 'Unknown'  # Default value
    diagnosis_df['subcategory'] = 'Unknown'  # Default value
    
    # Connect to database
    conn = connect_to_db()
    cursor = conn.cursor()
    
    # Check if constraint exists
    try:
        check_constraint_query = """
        SELECT COUNT(*) FROM pg_constraint 
        WHERE conname = 'dim_diagnosis_diagnosis_code_key' 
        AND conrelid = 'dim_diagnosis'::regclass;
        """
        cursor.execute(check_constraint_query)
        has_constraint = cursor.fetchone()[0] > 0
        
        if not has_constraint:
            print("Adding unique constraint to diagnosis_code")
            add_constraint_query = """
            ALTER TABLE dim_diagnosis ADD CONSTRAINT dim_diagnosis_diagnosis_code_key UNIQUE (diagnosis_code);
            """
            cursor.execute(add_constraint_query)
            conn.commit()
    except Exception as e:
        print(f"Error checking/creating constraint: {e}")
    
    insert_query = """
        INSERT INTO dim_diagnosis (diagnosis_code, diagnosis_name, category, subcategory)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (diagnosis_code) DO NOTHING;
    """
    
    diagnosis_keys = None
    
    try:
        # Convert to list of tuples with Python native types
        records = [(
            row['diagnosis_code'],
            row['diagnosis_name'],
            row['category'],
            row['subcategory']
        ) for _, row in diagnosis_df.iterrows()]
        
        cursor.executemany(insert_query, records)
        conn.commit()
        print(f"Inserted {len(diagnosis_df)} records into dim_diagnosis")
        
        # Fetch diagnosis keys using the same connection before closing
        cursor.execute("SELECT diagnosis_key, diagnosis_code, diagnosis_name FROM dim_diagnosis")
        diagnosis_keys_data = cursor.fetchall()
        diagnosis_keys = pd.DataFrame(diagnosis_keys_data, columns=['diagnosis_key', 'diagnosis_code', 'diagnosis_name'])
        
    except Exception as e:
        conn.rollback()
        print(f"Error inserting records: {e}")
        diagnosis_keys = pd.DataFrame(columns=['diagnosis_key', 'diagnosis_code', 'diagnosis_name'])
    finally:
        cursor.close()
        conn.close()
    
    return diagnosis_keys