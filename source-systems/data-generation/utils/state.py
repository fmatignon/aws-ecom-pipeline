"""
State management for data generation using S3 logs + mode detection
"""

from botocore.exceptions import ClientError
from datetime import datetime, date
from typing import Optional, Dict
from dotenv import load_dotenv
from pathlib import Path
import boto3
import json
import os

# Load environment variables from .env file (for local development)
# In ECS, environment variables are set via container definition
env_path = Path(__file__).parent.parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)


def get_s3_client():
    """Get S3 client with credentials from environment"""
    aws_profile = os.getenv("AWS_PROFILE")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    # If access keys are explicitly provided, use them (even if profile is set)
    # This allows overriding profile with explicit credentials
    if aws_access_key and aws_secret_key:
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region,
        )
    elif aws_profile:
        # Try to use profile, but fall back to default if it fails
        try:
            session = boto3.Session(profile_name=aws_profile)
            # Test if credentials are available
            session.get_credentials()
        except Exception:
            # Profile doesn't exist or has no credentials, try default session
            session = boto3.Session()
    else:
        # No explicit credentials, use default session (will use default profile or env vars)
        session = boto3.Session()

    return session.client("s3", region_name=region)


def get_bucket_name() -> str:
    """Get S3 bucket name from environment"""
    bucket = os.getenv("S3_BUCKET_NAME")
    if not bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is required")
    return bucket


def get_log_prefix() -> str:
    """Get S3 prefix for operation logs"""
    return "logs/operations/"


def get_last_run_date() -> Optional[date]:
    """
    Read the most recent successful log file from S3 and return the last_processed_date

    Returns:
        date object representing the last processed date, or None if no logs exist
    """
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    prefix = get_log_prefix()

    try:
        # List all log files
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        log_files = []
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].endswith(".json"):
                        log_files.append(
                            {"key": obj["Key"], "last_modified": obj["LastModified"]}
                        )

        if not log_files:
            return None

        # Sort by last modified (most recent first) and filter for successful runs
        log_files.sort(key=lambda x: x["last_modified"], reverse=True)

        # Try to find the most recent successful run
        for log_file in log_files:
            try:
                response = s3_client.get_object(Bucket=bucket_name, Key=log_file["key"])
                log_data = json.loads(response["Body"].read().decode("utf-8"))

                # Only return if status is success
                if log_data.get("status") == "success":
                    last_processed_date_str = log_data.get("last_processed_date")
                    if last_processed_date_str:
                        # Parse date string (YYYY-MM-DD format)
                        return datetime.strptime(
                            last_processed_date_str, "%Y-%m-%d"
                        ).date()
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                # Skip invalid log files
                continue

        return None

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            raise ValueError(f"S3 bucket '{bucket_name}' does not exist")
        raise


def write_run_log(
    last_processed_date: date,
    status: str,
    records_processed: Optional[Dict[str, int]] = None,
    error_message: Optional[str] = None,
) -> str:
    """
    Write a completion log to S3

    Args:
        last_processed_date: The last date that was processed
        status: 'success' or 'failed'
        records_processed: Dictionary with counts of records processed
        error_message: Error message if status is 'failed'

    Returns:
        S3 key of the written log file
    """
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    prefix = get_log_prefix()

    # Generate log file name
    now = datetime.now()
    log_filename = f"run_{now.strftime('%Y-%m-%d')}_{now.strftime('%H%M%S')}.json"
    log_key = f"{prefix}{log_filename}"

    # Create log entry
    log_entry = {
        "run_date": now.isoformat(),
        "last_processed_date": last_processed_date.strftime("%Y-%m-%d"),
        "status": status,
        "records_processed": records_processed or {},
    }

    if error_message:
        log_entry["error_message"] = error_message

    # Upload to S3
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=log_key,
            Body=json.dumps(log_entry, indent=2),
            ContentType="application/json",
        )
        return log_key
    except ClientError as e:
        raise RuntimeError(f"Failed to write log to S3: {e}")
