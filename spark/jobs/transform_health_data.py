# Spark job for cleaning & transforming data from health data source

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, regexp_replace, when, unix_timestamp, to_date
)
import os
from generator.minio_client import create_minio_client
from dotenv import load_dotenv
from datetime import datetime
import argparse
import boto3



load_dotenv()

##variables

minio_endpoint = os.getenv("MINIO_ENDPOINT", "172.19.0.2:9000")
access_key = os.getenv("MINIO_ACCESS_KEY")
secret_key = os.getenv("MINIO_SECRET_KEY")
bucket_name = "rogerlake"


def initialize_spark():
    """Initialize and return Spark session"""
    
    # Create Spark session with optimized S3 configuration
    spark = SparkSession.builder \
        .appName("HealthDataCleaning") \
        .config("spark.hadoop.fs.s3a.endpoint", "172.19.0.2:9000") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("fs.s3a.connection.ssl.enabled", "false") \
        .config("fs.s3a.path.style.access", "true") \
        .config("fs.s3a.attempts.maximum", "3") \
        .config("fs.s3a.connection.establish.timeout", "10000") \
        .config("fs.s3a.connection.timeout", "20000") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .getOrCreate()
    
    return spark

client = create_minio_client()
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")


def get_latest_file(client, bucket_name, prefix):
    """Get the most recent file with given prefix from MinIO."""
    try:
        objects = list(client.list_objects(bucket_name, prefix=prefix, recursive=True))
        if not objects:
            raise ValueError(f"No objects found with prefix '{prefix}' in bucket '{bucket_name}'")
        
        latest_object = max(objects, key=lambda obj: obj.last_modified)
        return latest_object.object_name
    except Exception as e:
        print(f"Error fetching latest file: {e}")
        raise


def clean_transform_data(spark, input_path):
    """Clean and transform the health data."""
    try:
        # Read CSV file into a Spark DataFrame with schema inference
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
        df = df.withColumn("admission_date", col("admission_date").cast("string"))
        df = df.withColumn("admission_date", to_date(unix_timestamp(col("admission_date"), "MM/dd/yyyy")))
        print(f"Initial record count: {df.count()}")
        print(f"Dataframe Schema: {df.printSchema()}")
        
        # Data Cleaning and Transformation
        df_cleaned = df \
            .withColumn("email", lower(trim(col("email")))) \
            .withColumn("phone_number", regexp_replace(col("phone_number"), "[^0-9]", "")) \
            .withColumn("gender", when(col("gender").isin(["Male", "Female", "Other"]), col("gender")).otherwise("Unknown")) \
            .withColumn("date_of_birth", to_date(unix_timestamp(col("date_of_birth"), "yyyy-MM-dd").cast("timestamp"))) \
            .withColumn("admission_date", to_date(unix_timestamp(col("admission_date"), "yyyy-MM-dd").cast("timestamp"))) \
            .withColumn("age", when(col("age").cast("int").between(0, 120), col("age")).otherwise(None)) \
            
        # Cache the DataFrame for better performance in subsequent operations
        df_cleaned.cache()
        
        # Remove duplicates (keep the latest by registration date)
        df_cleaned = df_cleaned.orderBy(col("admission_date").desc()).dropDuplicates(["email"])
        
        # Log final data stats
        print(f"Final record count after cleaning: {df_cleaned.count()}")
        
        # Return the cleaned DataFrame
        return df_cleaned
    except Exception as e:
        print(f"Error in data cleaning: {e}")
        raise



def rename_parquet_file(temp_output_path, final_output_path):
    """Move the generated Parquet file from temporary S3 folder to a single output file."""
    spark = SparkSession.builder.getOrCreate()

    # Extract S3 bucket and key from s3a:// URL
    temp_output_path = temp_output_path.replace("s3a://", "")
    final_output_path = final_output_path.replace("s3a://", "")

    bucket_name = temp_output_path.split("/")[0]
    temp_prefix = "/".join(temp_output_path.split("/")[1:])
    final_prefix = "/".join(final_output_path.split("/")[1:])

    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url="http://172.19.0.2:9000"
    )

    # List objects inside the temp folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=temp_prefix)
    if "Contents" not in response:
        raise FileNotFoundError(f"No files found in {temp_output_path}")

    for obj in response["Contents"]:
        key = obj["Key"]
        if key.endswith(".parquet") and "part-" in key:
            # Copy file to final location
            final_key = final_prefix
            s3.copy_object(
                Bucket=bucket_name,
                CopySource={"Bucket": bucket_name, "Key": key},
                Key=final_key,
            )
            print(f"Copied {key} -> {final_key}")

            # Delete original files
            s3.delete_object(Bucket=bucket_name, Key=key)
            print(f"Deleted {key}")

    # Delete the temporary folder
    s3.delete_object(Bucket=bucket_name, Key=temp_prefix)
    print(f"Deleted temporary S3 folder: {temp_output_path}")

def save_to_parquet(df, output_path):
    """Save DataFrame to a single Parquet file without Spark subfolders."""
    try:
        temp_output_path = f"{output_path}_temp"  # Temporary output directory

        # Repartition to 1 file and write to a temp directory
        df.repartition(1).write.mode("overwrite").parquet(temp_output_path)

        # Move and rename the file
        rename_parquet_file(temp_output_path, output_path)
        
        print(f"Successfully saved data to {output_path}")
    except Exception as e:
        print(f"Error saving to parquet: {e}")
        raise


def main(input_path=None, output_path=None, bucket="healthdata"):
    """Main function to orchestrate the ETL process."""
    spark = initialize_spark()
    try:
        # Determine input path
        if input_path is None:
            # Auto-detect latest file
            client = create_minio_client()
            latest_object = get_latest_file(client, bucket, "_raw_data_")
            input_path = f"s3a://{bucket}/{latest_object}"
        
        # Determine output path
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            output_path = f"s3a://{bucket}/cleaned_data/health_record_{timestamp}.parquet"
        
        print(f"Processing: {input_path} -> {output_path}")
        
        # Clean and transform data
        cleaned_df = clean_transform_data(spark, input_path)
        
        # Save to parquet
        save_to_parquet(cleaned_df, output_path)
        
        print("Data transformation completed successfully!")
    except Exception as e:
        print(f"Error in ETL process: {e}")
        raise
    finally:
        # Clean up
        spark.stop()

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Transform health data from CSV to Parquet')
    parser.add_argument('input_path', help='Input S3 path (can be auto-detected if not provided)',
                        nargs='?', default=None)
    parser.add_argument('output_path', help='Output S3 path for Parquet files',
                        nargs='?', default=None)
    parser.add_argument('--bucket', help='MinIO/S3 bucket name', 
                        default='healthdata')
    args = parser.parse_args()
    main(input_path=args.input_path, output_path=args.output_path, bucket=args.bucket)