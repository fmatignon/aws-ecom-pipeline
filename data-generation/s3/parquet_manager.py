"""
S3 Parquet file operations for payments and shipments
"""
import os
import boto3
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, date, timedelta
from typing import List, Optional
from io import BytesIO

# Load environment variables from .env file (for local development)
# In ECS, environment variables are set via container definition
env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)


def get_s3_client():
    """Get S3 client with credentials from environment"""
    aws_profile = os.getenv('AWS_PROFILE')
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    # If access keys are explicitly provided, use them (even if profile is set)
    # This allows overriding profile with explicit credentials
    if aws_access_key and aws_secret_key:
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
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
    
    return session.client('s3', region_name=region)


def get_bucket_name() -> str:
    """Get S3 bucket name from environment"""
    bucket = os.getenv('S3_BUCKET_NAME')
    if not bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is required")
    return bucket


PARQUET_CHUNK_SIZE = 100000  # Rows per Parquet file


def extract_date_from_timestamp(timestamp_str: str) -> Optional[str]:
    """Extract date (YYYY-MM-DD) from timestamp string"""
    try:
        if isinstance(timestamp_str, str):
            dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        else:
            dt = timestamp_str
        return dt.strftime('%Y-%m-%d')
    except Exception:
        return None


def read_existing_parquet_files(
    data_type: str,
    date_range: tuple
) -> pd.DataFrame:
    """
    Read existing Parquet files from S3 for date range
    
    Args:
        data_type: 'payments' or 'shipments'
        date_range: (start_date, end_date) as date objects
    
    Returns:
        DataFrame with all existing records
    """
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    start_date, end_date = date_range
    
    prefix = f"source/{data_type}/date="
    all_records = []
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        date_prefix = f"{prefix}{date_str}/"
        
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=date_prefix)
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if obj['Key'].endswith('.parquet'):
                            try:
                                response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
                                parquet_data = response['Body'].read()
                                table = pq.read_table(BytesIO(parquet_data))
                                df = table.to_pandas()
                                all_records.append(df)
                            except Exception as e:
                                print(f"Warning: Error reading {obj['Key']}: {e}")
                                continue
        except Exception as e:
            print(f"Warning: Error listing files for {date_str}: {e}")
        
        current_date += timedelta(days=1)
    
    if not all_records:
        return pd.DataFrame()
    
    return pd.concat(all_records, ignore_index=True)


def merge_parquet_data(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    key_column: str
) -> pd.DataFrame:
    """
    Merge and deduplicate Parquet data by key column
    
    Args:
        existing_df: Existing DataFrame
        new_df: New DataFrame to merge
        key_column: Column name to use for deduplication
    
    Returns:
        Merged DataFrame with duplicates removed (new data takes precedence)
    """
    if len(existing_df) == 0:
        return new_df.copy()
    
    if len(new_df) == 0:
        return existing_df.copy()
    
    # Combine and deduplicate (new data takes precedence)
    combined = pd.concat([existing_df, new_df], ignore_index=True)
    
    # Drop duplicates keeping last (new data)
    combined = combined.drop_duplicates(subset=[key_column], keep='last')
    
    return combined


def update_parquet_partitions(
    data_type: str,
    date: date,
    df: pd.DataFrame
):
    """
    Rewrite Parquet files for updated dates
    
    Args:
        data_type: 'payments' or 'shipments'
        date: Date to update
        df: DataFrame with all data for this date (merged)
    """
    if len(df) == 0:
        return
    
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    date_str = date.strftime('%Y-%m-%d')
    prefix = f"source/{data_type}/date={date_str}/"
    
    # Delete existing files for this date
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
    except Exception as e:
        print(f"Warning: Error deleting existing files for {date_str}: {e}")
    
    # Write new files
    append_new_parquet_partitions(data_type, date, df)


def append_new_parquet_partitions(
    data_type: str,
    date: date,
    df: pd.DataFrame
):
    """
    Create new Parquet files for new dates
    
    Args:
        data_type: 'payments' or 'shipments'
        date: Date for partition
        df: DataFrame with data for this date
    """
    if len(df) == 0:
        return
    
    s3_client = get_s3_client()
    bucket_name = get_bucket_name()
    date_str = date.strftime('%Y-%m-%d')
    
    # Split into chunks
    num_chunks = (len(df) + PARQUET_CHUNK_SIZE - 1) // PARQUET_CHUNK_SIZE
    
    for chunk_idx in range(num_chunks):
        start_idx = chunk_idx * PARQUET_CHUNK_SIZE
        end_idx = min((chunk_idx + 1) * PARQUET_CHUNK_SIZE, len(df))
        chunk_df = df.iloc[start_idx:end_idx]
        
        # S3 key
        s3_key = f"source/{data_type}/date={date_str}/data_{chunk_idx:04d}.parquet"
        
        # Convert to Parquet bytes
        parquet_buffer = BytesIO()
        chunk_df.to_parquet(
            parquet_buffer,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        parquet_buffer.seek(0)
        
        # Upload to S3
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=parquet_buffer.getvalue(),
                ContentType='application/octet-stream'
            )
        except Exception as e:
            raise RuntimeError(f"Failed to upload {s3_key}: {e}")


def update_payments_parquet(
    payments_df: pd.DataFrame,
    date_range: tuple
):
    """
    Update payments Parquet files for date range
    
    Args:
        payments_df: New/updated payments DataFrame
        date_range: (start_date, end_date) as date objects
    """
    if len(payments_df) == 0:
        return
    
    # Add date column
    payments_df = payments_df.copy()
    payments_df['date'] = payments_df['payment-date'].apply(extract_date_from_timestamp)
    payments_df = payments_df[payments_df['date'].notna()]
    
    if len(payments_df) == 0:
        return
    
    start_date, end_date = date_range
    
    # Process each date
    for date_str, date_df in payments_df.groupby('date'):
        partition_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        
        if partition_date < start_date or partition_date > end_date:
            continue
        
        # Remove date column before saving
        date_df_clean = date_df.drop(columns=['date'])
        
        # Read existing files for this date
        existing_df = read_existing_parquet_files('payments', (partition_date, partition_date))
        
        # Merge
        merged_df = merge_parquet_data(existing_df, date_df_clean, 'payment-id')
        
        # Update partition
        update_parquet_partitions('payments', partition_date, merged_df)


def update_shipments_parquet(
    shipments_df: pd.DataFrame,
    date_range: tuple
):
    """
    Update shipments Parquet files for date range
    
    Args:
        shipments_df: New/updated shipments DataFrame
        date_range: (start_date, end_date) as date objects
    """
    if len(shipments_df) == 0:
        return
    
    # Add date column
    shipments_df = shipments_df.copy()
    shipments_df['date'] = shipments_df['shipment-date'].apply(extract_date_from_timestamp)
    shipments_df = shipments_df[shipments_df['date'].notna()]
    
    if len(shipments_df) == 0:
        return
    
    start_date, end_date = date_range
    
    # Process each date
    for date_str, date_df in shipments_df.groupby('date'):
        partition_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        
        if partition_date < start_date or partition_date > end_date:
            continue
        
        # Remove date column before saving
        date_df_clean = date_df.drop(columns=['date'])
        
        # Read existing files for this date
        existing_df = read_existing_parquet_files('shipments', (partition_date, partition_date))
        
        # Merge
        merged_df = merge_parquet_data(existing_df, date_df_clean, 'tracking_number')
        
        # Update partition
        update_parquet_partitions('shipments', partition_date, merged_df)

