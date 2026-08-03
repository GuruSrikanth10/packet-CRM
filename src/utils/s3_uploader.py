import os
import boto3
from botocore.exceptions import ClientError

def upload_logs_to_s3(event_id: str, logs: str) -> str:
    """
    Uploads raw logs to AWS S3 and returns the S3 URL.
    Falls back to a mock URL if S3_LOGS_BUCKET is not configured.
    """
    bucket_name = os.environ.get("S3_LOGS_BUCKET")
    
    # If S3 is not configured, gracefully mock it
    if not bucket_name:
        print("[S3 UPLOADER] ⚠️ S3_LOGS_BUCKET not found in .env. Mocking S3 Upload.")
        return f"s3://mock-bucket/logs/{event_id}/raw_elastic_logs.txt"
        
    try:
        s3_client = boto3.client("s3")
        file_key = f"logs/{event_id}/raw_elastic_logs.txt"
        
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=logs.encode('utf-8'),
            ContentType="text/plain"
        )
        
        s3_url = f"s3://{bucket_name}/{file_key}"
        print(f"[S3 UPLOADER] ✅ Logs successfully uploaded to: {s3_url}")
        return s3_url
        
    except ClientError as e:
        print(f"[S3 UPLOADER] ❌ Failed to upload logs to S3: {e}")
        return f"S3 Upload Failed: {e}"
    except Exception as e:
        print(f"[S3 UPLOADER] ❌ Unexpected S3 upload error: {e}")
        return f"S3 Upload Error: {e}"
