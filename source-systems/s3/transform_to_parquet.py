#!/usr/bin/env python3
"""
Transform payments and shipments CSV files to Parquet format
and upload them to S3 with date-based partitioning.

For ongoing operations:
- New data can be appended to existing date partitions
- Each date partition can have multiple Parquet files
- Format: source/{payments|shipments}/date={YYYY-MM-DD}/data_*.parquet
"""
import os
import sys
import boto3
import pandas as pd
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from io import BytesIO

# Load environment variables from project root .env file
env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
else:
    print(f"Warning: .env file not found at {env_path}")

# Initialize S3 client with AWS credentials from .env
aws_profile = os.getenv('AWS_PROFILE', '').strip()
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', '').strip()
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', '').strip()
region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1').strip()

# Set AWS_PROFILE in environment so boto3 can find it via default credential chain
if aws_profile:
    os.environ['AWS_PROFILE'] = aws_profile
if region:
    os.environ['AWS_DEFAULT_REGION'] = region

# Create session - boto3 will use AWS_PROFILE from environment automatically
if aws_access_key and aws_secret_key:
    # Use explicit credentials if provided
    session = boto3.Session(
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=region
    )
else:
    # Use default credential chain (will pick up AWS_PROFILE from environment)
    session = boto3.Session()

s3_client = session.client('s3', region_name=region)

# Configuration
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'aws-ecom-pipeline')
CSV_DIR = Path(__file__).parent.parent.parent / 'data-generation' / 'output'
PARQUET_CHUNK_SIZE = 100000  # Rows per Parquet file (for large datasets)


def extract_date_from_timestamp(timestamp_str: str) -> str:
    """Extract date (YYYY-MM-DD) from timestamp string"""
    try:
        dt = datetime.strptime(str(timestamp_str), '%Y-%m-%d %H:%M:%S')
        return dt.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Error parsing timestamp {timestamp_str}: {e}")
        return None


def check_s3_object_exists(s3_key: str) -> bool:
    """Check if a Parquet file already exists in S3"""
    try:
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise


def upload_parquet_to_s3(df: pd.DataFrame, s3_key: str) -> bool:
    """Upload a DataFrame as Parquet to S3"""
    try:
        # Convert DataFrame to Parquet bytes
        parquet_buffer = BytesIO()
        df.to_parquet(
            parquet_buffer,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
        parquet_buffer.seek(0)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        return True
    except Exception as e:
        print(f"Error uploading {s3_key}: {e}")
        return False


def transform_payments_to_parquet():
    """Transform payments CSV to Parquet files partitioned by date"""
    print("Processing payments CSV...")
    csv_path = CSV_DIR / 'payments.csv'
    
    if not csv_path.exists():
        raise FileNotFoundError(f"Payments CSV not found: {csv_path}")
    
    # Read CSV
    print("Reading payments CSV...")
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df):,} payment records")
    
    # Add date column for partitioning
    df['date'] = df['payment-date'].apply(extract_date_from_timestamp)
    df = df[df['date'].notna()]  # Remove rows with invalid dates
    
    # Group by date and upload as Parquet files
    uploaded_count = 0
    skipped_count = 0
    
    for date, date_df in df.groupby('date'):
        # Remove the date column before saving (it's in the partition path)
        date_df_clean = date_df.drop(columns=['date'])
        
        # Split into chunks if needed
        num_chunks = (len(date_df_clean) + PARQUET_CHUNK_SIZE - 1) // PARQUET_CHUNK_SIZE
        
        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * PARQUET_CHUNK_SIZE
            end_idx = min((chunk_idx + 1) * PARQUET_CHUNK_SIZE, len(date_df_clean))
            chunk_df = date_df_clean.iloc[start_idx:end_idx]
            
            # S3 key: source/payments/date={YYYY-MM-DD}/data_{chunk_idx}.parquet
            s3_key = f"source/payments/date={date}/data_{chunk_idx:04d}.parquet"
            
            # Check if already exists (for incremental updates)
            if check_s3_object_exists(s3_key):
                print(f"  Skipping {s3_key} (already exists)")
                skipped_count += len(chunk_df)
                continue
            
            # Upload Parquet file
            if upload_parquet_to_s3(chunk_df, s3_key):
                uploaded_count += len(chunk_df)
                print(f"  Uploaded {s3_key} ({len(chunk_df):,} records)")
    
    print(f"✓ Completed payments:")
    print(f"  Uploaded: {uploaded_count:,} records")
    print(f"  Skipped: {skipped_count:,} records")


def transform_shipments_to_parquet():
    """Transform shipments CSV to Parquet files partitioned by date"""
    print("Processing shipments CSV...")
    csv_path = CSV_DIR / 'shipments.csv'
    
    if not csv_path.exists():
        raise FileNotFoundError(f"Shipments CSV not found: {csv_path}")
    
    # Read CSV
    print("Reading shipments CSV...")
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df):,} shipment records")
    
    # Add date column for partitioning
    df['date'] = df['shipment-date'].apply(extract_date_from_timestamp)
    df = df[df['date'].notna()]  # Remove rows with invalid dates
    
    # Group by date and upload as Parquet files
    uploaded_count = 0
    skipped_count = 0
    
    for date, date_df in df.groupby('date'):
        # Remove the date column before saving (it's in the partition path)
        date_df_clean = date_df.drop(columns=['date'])
        
        # Split into chunks if needed
        num_chunks = (len(date_df_clean) + PARQUET_CHUNK_SIZE - 1) // PARQUET_CHUNK_SIZE
        
        for chunk_idx in range(num_chunks):
            start_idx = chunk_idx * PARQUET_CHUNK_SIZE
            end_idx = min((chunk_idx + 1) * PARQUET_CHUNK_SIZE, len(date_df_clean))
            chunk_df = date_df_clean.iloc[start_idx:end_idx]
            
            # S3 key: source/shipments/date={YYYY-MM-DD}/data_{chunk_idx}.parquet
            s3_key = f"source/shipments/date={date}/data_{chunk_idx:04d}.parquet"
            
            # Check if already exists (for incremental updates)
            if check_s3_object_exists(s3_key):
                print(f"  Skipping {s3_key} (already exists)")
                skipped_count += len(chunk_df)
                continue
            
            # Upload Parquet file
            if upload_parquet_to_s3(chunk_df, s3_key):
                uploaded_count += len(chunk_df)
                print(f"  Uploaded {s3_key} ({len(chunk_df):,} records)")
    
    print(f"✓ Completed shipments:")
    print(f"  Uploaded: {uploaded_count:,} records")
    print(f"  Skipped: {skipped_count:,} records")


def main():
    """Main function"""
    print("=" * 70)
    print("CSV to Parquet Transformer - Uploading to S3")
    print("=" * 70)
    print(f"S3 Bucket: {S3_BUCKET_NAME}")
    print(f"CSV Directory: {CSV_DIR}")
    print(f"Region: {region}")
    print(f"AWS Profile: {aws_profile if aws_profile else 'Using default credentials'}")
    print("=" * 70)
    print()
    
    try:
        # Check if bucket exists and is accessible
        try:
            # Try to list objects to verify access (more reliable than head_bucket)
            s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, MaxKeys=1)
            print(f"✓ Bucket '{S3_BUCKET_NAME}' is accessible")
        except s3_client.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == '404':
                print(f"Error: Bucket '{S3_BUCKET_NAME}' not found.")
            elif error_code == '403':
                print(f"Error: Access denied to bucket '{S3_BUCKET_NAME}'. Check your AWS credentials.")
            else:
                print(f"Error accessing bucket '{S3_BUCKET_NAME}': {e}")
            print("Please ensure the bucket exists and your AWS credentials are configured.")
            sys.exit(1)
        except Exception as e:
            print(f"Error: {e}")
            print(f"Bucket: {S3_BUCKET_NAME}")
            print(f"Region: {region}")
            print(f"AWS Profile: {aws_profile if aws_profile else 'Not set'}")
            print("Please ensure your AWS credentials are configured correctly.")
            sys.exit(1)
        
        # Transform payments
        transform_payments_to_parquet()
        print()
        
        # Transform shipments
        # transform_shipments_to_parquet()
        # print()
        
        print("=" * 70)
        print("✓ All transformations completed successfully!")
        print("=" * 70)
    
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

