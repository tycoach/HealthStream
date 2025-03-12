# Spark job for cleaning & transforming data from health data source

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, upper, regexp_replace, when, lit, current_date, unix_timestamp, to_date
)
from minio import Minio
import os
import io
import pandas as pd
from generator.minio_client import create_minio_client
from dotenv import load_dotenv
import io
from datetime import datetime

load_dotenv()


##variables

minio_endpoint = os.getenv("MINIO_ENDPOINT", "172.18.0.11:9000")
access_key = "BnAxnlNVrqlmjP74G9gN"
secret_key = "K5GlorHhGOgZ0PqM686CG13qG7pW1xrsfbxop9hG"
bucket_name = "rogerlake"

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("HealthDataCleaning") \
    .config("spark.hadoop.fs.s3a.endpoint", "172.18.0.11:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET")) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

client = create_minio_client()
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")


# Fetch the latest dataset from MinIO
objects = client.list_objects(bucket_name, prefix="health_records/_raw_data_", recursive=True)
latest_object = max(objects, key=lambda obj: obj.last_modified)  # Get the most recent file

#  Read CSV file into a Spark DataFrame
csv_path = f"s3a://{bucket_name}/{latest_object.object_name}"
df = spark.read.option("header", "true").csv(csv_path)

#  Data Cleaning and Transformation
df_cleaned = df \
    .withColumn("full_name", trim(col("full_name"))) \
    .withColumn("email", lower(trim(col("email")))) \
    .withColumn("phone_number", regexp_replace(col("phone_number"), "[^0-9]", "")) \
    .withColumn("gender", when(col("gender").isin(["Male", "Female", "Other"]), col("gender")).otherwise("Unknown")) \
    .withColumn("marital_status", when(col("marital_status").isin(["Single", "Married", "Divorced", "Widowed"]), col("marital_status")).otherwise("Unknown")) \
    .withColumn("dob", to_date(unix_timestamp(col("dob"), "yyyy-MM-dd").cast("timestamp"))) \
    .withColumn("registration_date", to_date(unix_timestamp(col("registration_date"), "yyyy-MM-dd").cast("timestamp"))) \
    .withColumn("age", when(col("age").cast("int").between(0, 120), col("age")).otherwise(None)) \
    .withColumn("balance", when(col("balance").cast("float") >= 0, col("balance")).otherwise(None))

# Remove duplicates (keep the latest by registration date)
df_cleaned = df_cleaned.orderBy(col("registration_date").desc()).dropDuplicates(["email"])

# Save the cleaned data as Parquet in MinIO
parquet_path = f"s3a://{bucket_name}/cleaned_data/health_record_{timestamp}.parquet"
df_cleaned.write.mode("overwrite").parquet(parquet_path)

print(f"--- Successfully cleaned and saved data to {parquet_path}")

# Stop Spark Session
spark.stop()
