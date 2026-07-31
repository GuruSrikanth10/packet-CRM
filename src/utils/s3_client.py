import os
import boto3
from pathlib import Path

s3_client = boto3.client('s3')

def upload_directory_to_s3(investigation_dir: str, bucket_name: str, prefix: str) -> bool:
    """Uploads the entire investigation directory to S3 mimicking agentic-fms."""
    try:
        path = Path(investigation_dir)
        for root, dirs, files in os.walk(path):
            for file in files:
                local_path = os.path.join(root, file)
                rel_path = os.path.relpath(local_path, path)
                s3_key = os.path.join(prefix, rel_path).replace("\\", "/")
                
                s3_client.upload_file(local_path, bucket_name, s3_key)
        print(f"Successfully uploaded {investigation_dir} to s3://{bucket_name}/{prefix}")
        return True
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return False
