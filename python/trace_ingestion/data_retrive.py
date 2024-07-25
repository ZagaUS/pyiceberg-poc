import json
import os
import boto3
from botocore.client import Config
from datetime import datetime, timezone
import pandas as pd

s3_endpoint = "http://minio-lb.apps.zagaopenshift.zagaopensource.com:9009"
access_key = "minioAdmin"
secret_key = "minio1234"
bucket_name = "messageingest"


s3 = boto3.client('s3',
                  endpoint_url=s3_endpoint,
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key,
                  config=Config(signature_version='s3v4'))

def list_objects_in_bucket(bucket_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            print("Objects in S3 bucket:")
            for obj in response['Contents']:
                print(f"Key: {obj['Key']}, Last Modified: {obj['LastModified']}")
            return response['Contents']
        else:
            print("No objects found in the bucket.")
            return []
    except Exception as e:
        print(f"Error listing objects in bucket: {e}")
        return []

def ensure_directory_exists(download_path):
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

def download_file(bucket_name, file_key, download_path):
    try:
        ensure_directory_exists(download_path)
        s3.download_file(bucket_name, file_key, download_path)
        print(f"File '{file_key}' downloaded successfully to '{download_path}'.")
        return download_path
    except Exception as e:
        print(f"Error downloading file '{file_key}': {e}")
        return None

def read_and_process_parquet(file_path):
    try:
        df = pd.read_parquet(file_path, engine='pyarrow')
        if 'tracedata' in df.columns:
            for index, row in df.iterrows():
                json_data = json.loads(row['tracedata'])  
                print(json.dumps(json_data, indent=2))  
        else:
            print("The 'tracedata' column is not found in the dataframe.")
    except Exception as e:
        print(f"Error reading or processing the parquet file: {e}")

def get_closest_parquet_file_key(objects, target_time):
    parquet_files = [obj for obj in objects if obj['Key'].endswith('.parquet')]
    if not parquet_files:
        return None
    
    closest_file = min(parquet_files, key=lambda obj: abs(obj['LastModified'] - target_time))
    return closest_file['Key']

target_time_str = "2024-07-23 05:55:06.173000+00:00"  
target_time = datetime.fromisoformat(target_time_str).replace(tzinfo=timezone.utc)

objects = list_objects_in_bucket(bucket_name)

closest_file_key = get_closest_parquet_file_key(objects, target_time)

if closest_file_key:
    download_path = 'downloaded_' + closest_file_key  
    downloaded_file_path = download_file(bucket_name, closest_file_key, download_path)

    if downloaded_file_path:
        read_and_process_parquet(downloaded_file_path)        
else:
    print("No parquet files found to download.")
