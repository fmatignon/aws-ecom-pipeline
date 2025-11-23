"""
Lambda handler for Payments API
Reads Parquet files from S3 partitioned by date
"""
import json
import os
import boto3
import pyarrow.parquet as pq
from datetime import datetime, timedelta
from typing import Dict, Any, List
from io import BytesIO

# Initialize S3 client
s3_client = boto3.client('s3')

# Get environment variables
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
API_KEY_SECRET_ARN = os.environ.get('API_KEY_SECRET_ARN')

if not API_KEY_SECRET_ARN:
    raise ValueError("API_KEY_SECRET_ARN environment variable is required")

# Retrieve API key from Secrets Manager
secrets_client = boto3.client('secretsmanager')
try:
    response = secrets_client.get_secret_value(SecretId=API_KEY_SECRET_ARN)
    secret_data = json.loads(response['SecretString'])
    VALID_API_KEY = secret_data['api_key']
except Exception as e:
    raise ValueError(f"Failed to retrieve API key from Secrets Manager: {e}")
DEFAULT_LIMIT = 1000


def validate_api_key(event: Dict[str, Any]) -> bool:
    """Validate API key from request headers"""
    headers = event.get('headers', {})
    api_key = headers.get('x-api-key') or headers.get('X-Api-Key')
    return api_key == VALID_API_KEY


def parse_query_params(event: Dict[str, Any]) -> Dict[str, Any]:
    """Parse and validate query parameters"""
    query_params = event.get('queryStringParameters') or {}
    
    start_date_str = query_params.get('start_date')
    end_date_str = query_params.get('end_date')
    limit = int(query_params.get('limit', DEFAULT_LIMIT))
    offset = int(query_params.get('offset', 0))
    
    if not start_date_str or not end_date_str:
        raise ValueError("start_date and end_date are required")
    
    # Parse ISO8601 dates
    start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
    end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
    
    if start_date > end_date:
        raise ValueError("start_date must be before or equal to end_date")
    
    return {
        'start_date': start_date.date(),
        'end_date': end_date.date(),
        'limit': limit,
        'offset': offset,
    }


def list_parquet_files_by_date_range(start_date, end_date) -> list:
    """List all Parquet files in the date range - optimized to use prefix listing"""
    parquet_files = []
    
    # Use a broader prefix to list all files at once instead of day-by-day
    # This is much faster for large date ranges
    prefix = "source/payments/date="
    
    # List all objects with the prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix)
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.parquet'):
                    # Extract date from the key path: source/payments/date=YYYY-MM-DD/...
                    try:
                        # Key format: source/payments/date=YYYY-MM-DD/filename.parquet
                        key_parts = obj['Key'].split('/')
                        if len(key_parts) >= 3 and key_parts[2].startswith('date='):
                            file_date_str = key_parts[2].replace('date=', '')
                            file_date = datetime.strptime(file_date_str, '%Y-%m-%d').date()
                            
                            # Filter by date range
                            if start_date <= file_date <= end_date:
                                parquet_files.append(obj['Key'])
                    except (ValueError, IndexError):
                        # Skip files with unexpected format
                        continue
    
    return sorted(parquet_files)


def read_parquet_from_s3(s3_key: str) -> List[Dict]:
    """Read a Parquet file from S3 and return as list of dictionaries"""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        parquet_data = response['Body'].read()
        table = pq.read_table(BytesIO(parquet_data))
        # Convert to list of dictionaries
        return table.to_pylist()
    except Exception as e:
        print(f"Error reading {s3_key}: {e}")
        return []


def filter_by_date_range(records: List[Dict], start_date, end_date, date_column: str) -> List[Dict]:
    """Filter records by date range"""
    if not records:
        return records
    
    filtered = []
    for record in records:
        record_date = record.get(date_column)
        if record_date is None:
            continue
        
        # Convert to date if it's a datetime string or datetime object
        if isinstance(record_date, str):
            record_date = datetime.fromisoformat(record_date.replace('Z', '+00:00')).date()
        elif isinstance(record_date, datetime):
            record_date = record_date.date()
        
        # Filter by date range
        if start_date <= record_date <= end_date:
            filtered.append(record)
    
    return filtered


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Main Lambda handler"""
    try:
        # API key validation is handled by API Gateway
        
        # Parse query parameters
        params = parse_query_params(event)
        
        # For the first page (offset=0), we can optimize by reading files incrementally
        # without listing all files first. This avoids the slow S3 listing operation.
        # For subsequent pages, we need the full file list for accurate pagination.
        if params['offset'] == 0:
            # Optimized path: Read files incrementally without full listing
            # This is much faster for the first page
            parquet_files = []  # Will be populated incrementally
        else:
            # For pagination, we need the full file list
            parquet_files = list_parquet_files_by_date_range(
                params['start_date'],
                params['end_date']
            )
        
        if not parquet_files and params['offset'] > 0:
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                },
                'body': json.dumps({
                    'payments': [],
                    'count': 0,
                    'total_count': 0,
                    'limit': params['limit'],
                    'offset': params['offset'],
                    'has_more': False,
                    'next_offset': None,
                })
            }
        
        # Optimized: Read files incrementally and return first page ASAP to avoid API Gateway timeout
        # For large date ranges, we read files until we have enough records for the requested page
        filtered_records = []
        target_count = params['offset'] + params['limit']  # We need at least this many records
        files_read = 0
        
        # If offset=0, read files incrementally by date without listing all files first
        # Start from most recent dates and work backwards for faster results
        if params['offset'] == 0:
            # Read files day by day, starting from the end date and working backwards
            # This is faster since most data is likely recent
            current_date = params['end_date']
            while current_date >= params['start_date'] and len(filtered_records) < target_count:
                date_str = current_date.strftime('%Y-%m-%d')
                prefix = f"source/payments/date={date_str}/"
                
                # List files for this specific date only
                try:
                    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=prefix)
                    if 'Contents' in response:
                        for obj in response['Contents']:
                            if obj['Key'].endswith('.parquet'):
                                records = read_parquet_from_s3(obj['Key'])
                                if records:
                                    file_filtered = filter_by_date_range(
                                        records,
                                        params['start_date'],
                                        params['end_date'],
                                        'payment-date'
                                    )
                                    filtered_records.extend(file_filtered)
                                    files_read += 1
                                    
                                    # Stop if we have enough records
                                    if len(filtered_records) >= target_count:
                                        break
                except Exception as e:
                    print(f"Error listing files for {date_str}: {e}")
                
                if len(filtered_records) >= target_count:
                    break
                    
                current_date -= timedelta(days=1)  # Work backwards
            
            total_files = None  # Unknown for incremental reading
        else:
            # For pagination, use the pre-listed files
            total_files = len(parquet_files)
            # Read files in order until we have enough records for the requested page
            for parquet_file in parquet_files:
                files_read += 1
                records = read_parquet_from_s3(parquet_file)
                if records:
                    # Filter by exact date range immediately
                    file_filtered = filter_by_date_range(
                        records,
                        params['start_date'],
                        params['end_date'],
                        'payment-date'
                    )
                    filtered_records.extend(file_filtered)
                    
                    # If we have enough records for the requested page, we can stop reading early
                    # This significantly speeds up the first page response
                    if len(filtered_records) >= target_count:
                        break
        
        if not filtered_records:
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                },
                'body': json.dumps({
                    'payments': [],
                    'count': 0,
                    'total_count': 0,
                    'limit': params['limit'],
                    'offset': params['offset'],
                    'has_more': False,
                    'next_offset': None,
                })
            }
        
        # Sort by payment-date for consistent ordering
        filtered_records = sorted(
            filtered_records,
            key=lambda x: x.get('payment-date', '')
        )
        
        # Apply pagination
        start_idx = params['offset']
        end_idx = start_idx + params['limit']
        paginated_records = filtered_records[start_idx:end_idx]
        
        # Estimate total_count: if we read all files, use actual count
        # Otherwise, estimate based on files read vs total files
        if total_files is None:
            # Incremental reading: we don't know total, estimate conservatively
            total_count = len(filtered_records) + 1  # At least what we have, likely more
        elif files_read >= total_files:
            total_count = len(filtered_records)
        else:
            # Estimate: assume records are evenly distributed across files
            estimated_total = int(len(filtered_records) * (total_files / files_read))
            total_count = estimated_total
        
        # Convert datetime objects to strings for JSON serialization
        payments = []
        for payment in paginated_records:
            payment_dict = {}
            for key, value in payment.items():
                if isinstance(value, datetime):
                    payment_dict[key] = value.isoformat()
                else:
                    payment_dict[key] = value
            payments.append(payment_dict)
        
        # Determine if there are more results
        # If we read all files, use exact count; otherwise assume there might be more
        if total_files is None:
            # Incremental reading: assume there are more if we got a full page
            has_more = len(paginated_records) >= params['limit']
        elif files_read >= total_files:
            has_more = params['offset'] + params['limit'] < total_count
        else:
            # We stopped early, so there are definitely more results
            has_more = True
        next_offset = params['offset'] + params['limit'] if has_more else None
        
        # Return response
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
            },
            'body': json.dumps({
                'payments': payments,
                'count': len(payments),
                'total_count': total_count,
                'limit': params['limit'],
                'offset': params['offset'],
                'has_more': has_more,
                'next_offset': next_offset,
            }, default=str)
        }
    
    except ValueError as e:
        return {
            'statusCode': 400,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
            },
            'body': json.dumps({
                'error': 'Bad Request',
                'message': str(e)
            })
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
            },
            'body': json.dumps({
                'error': 'Internal Server Error',
                'message': str(e)
            })
        }
