"""
Lambda handler for Shipments API
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
VALID_API_KEY = os.environ.get('SHIPMENTS_API_KEY', 'demo-shipments-api-key-67890')
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
    """List all Parquet files in the date range"""
    parquet_files = []
    current_date = start_date
    
    while current_date <= end_date:
        # Format date as YYYY-MM-DD for partition path
        date_str = current_date.strftime('%Y-%m-%d')
        prefix = f"source/shipments/date={date_str}/"
        
        # List all Parquet files for this date
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        parquet_files.append(obj['Key'])
        
        current_date += timedelta(days=1)
    
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
        
        # List Parquet files in date range
        parquet_files = list_parquet_files_by_date_range(
            params['start_date'],
            params['end_date']
        )
        
        if not parquet_files:
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                },
                'body': json.dumps({
                    'shipments': [],
                    'count': 0,
                    'total_count': 0,
                    'limit': params['limit'],
                    'offset': params['offset'],
                    'has_more': False,
                    'next_offset': None,
                })
            }
        
        # Read and combine all Parquet files
        all_records = []
        for parquet_file in parquet_files:
            records = read_parquet_from_s3(parquet_file)
            if records:
                all_records.extend(records)
        
        if not all_records:
            return {
                'statusCode': 200,
                'headers': {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                },
                'body': json.dumps({
                    'shipments': [],
                    'count': 0,
                    'total_count': 0,
                    'limit': params['limit'],
                    'offset': params['offset'],
                    'has_more': False,
                    'next_offset': None,
                })
            }
        
        # Filter by exact date range (Parquet files are partitioned by date, but we filter for precision)
        filtered_records = filter_by_date_range(
            all_records,
            params['start_date'],
            params['end_date'],
            'shipment-date'
        )
        
        # Sort by shipment-date for consistent ordering
        filtered_records = sorted(
            filtered_records,
            key=lambda x: x.get('shipment-date', '')
        )
        
        # Get total count
        total_count = len(filtered_records)
        
        # Apply pagination
        start_idx = params['offset']
        end_idx = start_idx + params['limit']
        paginated_records = filtered_records[start_idx:end_idx]
        
        # Convert datetime objects to strings for JSON serialization
        shipments = []
        for shipment in paginated_records:
            shipment_dict = {}
            for key, value in shipment.items():
                if isinstance(value, datetime):
                    shipment_dict[key] = value.isoformat()
                else:
                    shipment_dict[key] = value
            shipments.append(shipment_dict)
        
        # Determine if there are more results
        has_more = params['offset'] + params['limit'] < total_count
        next_offset = params['offset'] + params['limit'] if has_more else None
        
        # Return response
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
            },
            'body': json.dumps({
                'shipments': shipments,
                'count': len(shipments),
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
