import json
import boto3
from botocore.exceptions import ClientError
from .config import settings

s3_client = boto3.client('s3')

def get_casesheet_key(message_id: str) -> str:
    return f"casesheets/{message_id}.json"

def check_casesheet_exists(message_id: str) -> bool:
    """Check if a casesheet already exists in S3 for the given message ID."""
    try:
        s3_client.head_object(Bucket=settings.s3_bucket, Key=get_casesheet_key(message_id))
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        # If the error is not a 404, we log it and assume we shouldn't proceed
        # In a real app, this should be properly logged
        print(f"Error checking S3: {e}")
        return True # Fail-safe to avoid duplicate processing on S3 errors

def upload_casesheet(message_id: str, casesheet_data: dict) -> bool:
    """Upload the generated casesheet to S3."""
    try:
        s3_client.put_object(
            Bucket=settings.s3_bucket,
            Key=get_casesheet_key(message_id),
            Body=json.dumps(casesheet_data, indent=2),
            ContentType='application/json'
        )
        return True
    except ClientError as e:
        print(f"Failed to upload casesheet to S3: {e}")
        return False
