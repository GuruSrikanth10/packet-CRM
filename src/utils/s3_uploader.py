import os
from typing import Optional
import boto3
from botocore.exceptions import ClientError

def upload_logs_to_s3(event_id: str, logs: str) -> Optional[str]:
    """
    Uploads raw logs to AWS S3 and returns the S3 URL, or None if the
    upload could not happen (bucket not configured, or the upload failed).

    Callers must check for None rather than storing the return value
    directly -- this previously returned a fake s3://mock-bucket/... URL (or
    an "S3 Upload Failed: ..." string) that got persisted in the casebook as
    if it were a real, resolvable link while the actual log text was
    discarded (1.12).
    """
    bucket_name = os.environ.get("S3_LOGS_BUCKET")

    if not bucket_name:
        print("[S3 UPLOADER] S3_LOGS_BUCKET not configured; cannot upload logs.")
        return None

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
        print(f"[S3 UPLOADER] Logs successfully uploaded to: {s3_url}")
        return s3_url

    except ClientError as e:
        print(f"[S3 UPLOADER] Failed to upload logs to S3: {e}")
        return None
    except Exception as e:
        print(f"[S3 UPLOADER] Unexpected S3 upload error: {e}")
        return None
