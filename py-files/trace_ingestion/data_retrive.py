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

# Initialize the S3 client with MinIO configurations
s3 = boto3.client('s3',
                  endpoint_url=s3_endpoint,
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key,
                  config=Config(signature_version='s3v4'))

def list_objects_in_bucket(bucket_name):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            return response['Contents']
        else:
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
        return download_path
    except Exception as e:
        print(f"Error downloading file '{file_key}': {e}")
        return None

def read_and_process_parquet(file_path):
    try:
        df = pd.read_parquet(file_path, engine='pyarrow')
        
        # Print column names to help diagnose issues
        # print(f"Columns in {file_path}: {df.columns.tolist()}")
        
        if 'tracedata' in df.columns:
            data = []
            for index, row in df.iterrows():
                json_data = json.loads(row['tracedata'])
                data.append(json_data)
            return data
        else:
            # print(f"The 'tracedata' column is not found in the dataframe from file: {file_path}.")
            return []
    except Exception as e:
        print(f"Error reading or processing the parquet file '{file_path}': {e}")
        return []
def filter_parquet_files_by_date(objects, from_date, to_date):
    parquet_files = [obj for obj in objects if obj['Key'].endswith('.parquet')]
    filtered_files = [obj for obj in parquet_files if from_date <= obj['LastModified'].date() <= to_date]
    return filtered_files

def get_date_range(from_date_str, to_date_str):
    from_date = datetime.strptime(from_date_str, '%Y-%m-%d').date()
    to_date = datetime.strptime(to_date_str, '%Y-%m-%d').date()
    return from_date, to_date

def retrieve_data(from_date_str, to_date_str):
    from_date, to_date = get_date_range(from_date_str, to_date_str)
    objects = list_objects_in_bucket(bucket_name)
    filtered_files = filter_parquet_files_by_date(objects, from_date, to_date)

    all_data = []
    for file in filtered_files:
        download_path = 'downloaded_' + file['Key']
        downloaded_file_path = download_file(bucket_name, file['Key'], download_path)

        if downloaded_file_path:
            data = read_and_process_parquet(downloaded_file_path)
            all_data.extend(data)
    
    return all_data

def final_data(from_date_str, to_date_str):
    data = retrieve_data(from_date_str, to_date_str)
    return json.dumps(data, indent=2)

from_date_str = "2024-07-22"  
to_date_str = "2024-07-23"    

# print(final_data(from_date_str, to_date_str))
