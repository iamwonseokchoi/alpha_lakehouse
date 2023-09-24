import boto3
from dotenv import load_dotenv
import os

load_dotenv()
aws_key = os.getenv('AWS_ACCESS_KEY')
aws_secret = os.getenv('AWS_ACCESS_KEY_SECRET')

session = boto3.Session(
    aws_access_key_id=aws_key,
    aws_secret_access_key=aws_secret,
    region_name='ap-northeast-2'
)

s3 = session.client('s3')

bucket_name = 'wonseokchoi-data-lake-project'
directory_path = 'lake/cassandra_replication/price/'

local_directory = './data/lake_replica/'
os.makedirs(local_directory, exist_ok=True)

paginator = s3.get_paginator('list_objects_v2')
for result in paginator.paginate(Bucket=bucket_name, Prefix=directory_path):
    for content in result.get('Contents', []):
        file_key = content['Key']
        local_file_path = os.path.join(local_directory, file_key[len(directory_path):])
        
        os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
        
        s3.download_file(bucket_name, file_key, local_file_path)
        print(f"Downloaded {file_key} to {local_file_path}")

print("Download complete.")
