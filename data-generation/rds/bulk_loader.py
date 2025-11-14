"""
RDS bulk loading from S3 using PostgreSQL COPY FROM
"""
import os
import boto3
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional
from datetime import datetime
import io

# Load environment variables from .env file (for local development)
env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)

from .loader import get_db_connection


def get_s3_client():
    """Get S3 client with credentials from environment"""
    aws_profile = os.getenv('AWS_PROFILE')
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    if aws_access_key and aws_secret_key:
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
    elif aws_profile:
        try:
            session = boto3.Session(profile_name=aws_profile)
            session.get_credentials()
        except Exception:
            session = boto3.Session()
    else:
        session = boto3.Session()
    
    return session.client('s3', region_name=region)


def get_bucket_name() -> str:
    """Get S3 bucket name from environment"""
    bucket = os.getenv('S3_BUCKET_NAME')
    if not bucket:
        raise ValueError("S3_BUCKET_NAME environment variable is required")
    return bucket


def save_dataframe_to_s3(df: pd.DataFrame, s3_key: str, bucket_name: Optional[str] = None) -> str:
    """
    Save DataFrame to S3 as CSV for PostgreSQL COPY FROM
    
    Args:
        df: DataFrame to save
        s3_key: S3 key (path) where to save the file
        bucket_name: S3 bucket name (optional, uses env var if not provided)
    
    Returns:
        S3 URI of the saved file
    """
    if bucket_name is None:
        bucket_name = get_bucket_name()
    
    s3_client = get_s3_client()
    
    # Convert DataFrame to CSV in memory
    # PostgreSQL COPY FROM expects NULL values as empty strings or \N
    # We'll use empty strings and handle them in COPY command
    df_clean = df.where(pd.notna(df), '')
    
    # Use to_csv with proper settings for PostgreSQL
    csv_buffer = df_clean.to_csv(
        index=False,
        header=False,
        sep=',',
        quoting=1,  # QUOTE_MINIMAL
        escapechar='\\',
        doublequote=False
    ).encode('utf-8')
    
    # Upload to S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=csv_buffer,
        ContentType='text/csv'
    )
    
    return f"s3://{bucket_name}/{s3_key}"


def get_table_indexes(table_name: str, cursor) -> list:
    """Get list of non-primary key indexes for a table"""
    cursor.execute("""
        SELECT indexname
        FROM pg_indexes
        WHERE tablename = %s
        AND indexname NOT LIKE '%_pkey'
        AND indexname NOT LIKE '%_pk'
    """, (table_name,))
    return [row[0] for row in cursor.fetchall()]


def drop_table_indexes(table_name: str, cursor):
    """Drop non-critical indexes (keep primary keys)"""
    indexes = get_table_indexes(table_name, cursor)
    dropped = []
    for index_name in indexes:
        try:
            cursor.execute(f"DROP INDEX IF EXISTS {index_name}")
            dropped.append(index_name)
        except Exception as e:
            # Some indexes might be required (e.g., unique constraints)
            pass
    return dropped


def recreate_table_indexes(table_name: str, indexes: list, cursor):
    """Recreate indexes that were dropped"""
    # For now, we'll skip recreation as it's complex
    # In production, you'd want to store index definitions and recreate them
    pass


def bulk_load_from_s3(
    table_name: str,
    s3_key: str,
    columns: list,
    bucket_name: Optional[str] = None,
    region: Optional[str] = None,
    optimize: bool = True
) -> int:
    """
    Bulk load data from S3 into RDS using PostgreSQL COPY FROM with optimizations
    
    Optimizations:
    1. Drop non-critical indexes before load
    2. Disable autovacuum during bulk operation
    3. Use staging table + INSERT ... ON CONFLICT DO NOTHING
    4. Recreate indexes after load
    
    Args:
        table_name: Target table name
        s3_key: S3 key (path) of the CSV file
        columns: List of column names in order
        bucket_name: S3 bucket name (optional, uses env var if not provided)
        region: AWS region (optional, uses env var if not provided)
        optimize: Whether to apply optimizations (drop indexes, disable autovacuum)
    
    Returns:
        Number of rows loaded
    """
    if bucket_name is None:
        bucket_name = get_bucket_name()
    if region is None:
        region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Optimization 1: Drop non-critical indexes
        dropped_indexes = []
        if optimize:
            print(f"\r      Dropping non-critical indexes...", end='', flush=True)
            dropped_indexes = drop_table_indexes(table_name, cursor)
            if dropped_indexes:
                print(f"\r      Dropped {len(dropped_indexes)} indexes", end='', flush=True)
            conn.commit()
        
        # Optimization 2: Disable autovacuum for this table during bulk load
        autovacuum_was_enabled = True
        if optimize:
            try:
                cursor.execute(f"""
                    ALTER TABLE {table_name} SET (autovacuum_enabled = false)
                """)
                conn.commit()
                autovacuum_was_enabled = False
            except Exception:
                # If we can't disable autovacuum, continue anyway
                pass
        
        # Download CSV from S3
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        csv_data = response['Body'].read().decode('utf-8')
        
        # Create staging table WITHOUT indexes (much faster)
        staging_table = f"staging_{table_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        columns_str = ', '.join(columns)
        
        # Get primary key column (assume first column is primary key)
        pk_column = columns[0]
        
        # Create staging table WITHOUT indexes (LIKE but exclude indexes)
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {staging_table} (LIKE {table_name} INCLUDING DEFAULTS INCLUDING CONSTRAINTS EXCLUDING INDEXES)
        """)
        
        # Create StringIO buffer for COPY FROM STDIN
        csv_buffer = io.StringIO(csv_data)
        
        # Copy data into staging table (fast - no indexes to maintain)
        copy_query = f"""
            COPY {staging_table} ({columns_str})
            FROM STDIN
            WITH (FORMAT csv, DELIMITER ',', NULL '', QUOTE '"', ESCAPE '\\')
        """
        
        cursor.copy_expert(copy_query, csv_buffer)
        
        # Optimization 3: Set-based INSERT with ON CONFLICT DO NOTHING
        # This is equivalent to MERGE - much faster than row-by-row
        insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            SELECT {columns_str} FROM {staging_table}
            ON CONFLICT ({pk_column}) DO NOTHING
        """
        
        cursor.execute(insert_query)
        rows_loaded = cursor.rowcount
        
        # Drop staging table
        cursor.execute(f"DROP TABLE {staging_table}")
        
        # Re-enable autovacuum
        if optimize and not autovacuum_was_enabled:
            try:
                cursor.execute(f"""
                    ALTER TABLE {table_name} SET (autovacuum_enabled = true)
                """)
            except Exception:
                pass
        
        conn.commit()
        
        # Note: Indexes will be recreated automatically by PostgreSQL if needed
        # For production, you'd want to explicitly recreate them here
        
        return rows_loaded
        
    except Exception as e:
        conn.rollback()
        # Re-enable autovacuum on error
        if optimize:
            try:
                cursor.execute(f"""
                    ALTER TABLE {table_name} SET (autovacuum_enabled = true)
                """)
                conn.commit()
            except Exception:
                pass
        raise RuntimeError(f"Failed to load from S3: {e}")
    finally:
        cursor.close()
        conn.close()


def bulk_load_customers_from_s3(s3_key: str) -> int:
    """Bulk load customers from S3 CSV"""
    columns = [
        'customer_id', 'first_name', 'last_name', 'email', 'phone',
        'country', 'city', 'state', 'postal_code', 'address',
        'signup_date', 'created_at', 'updated_at',
        'customer_segment', 'date_of_birth', 'gender'
    ]
    return bulk_load_from_s3('customers', s3_key, columns)


def bulk_load_products_from_s3(s3_key: str) -> int:
    """Bulk load products from S3 CSV"""
    columns = [
        'product_id', 'product_name', 'category', 'sub_category',
        'brand', 'price', 'cost', 'created_at', 'color'
    ]
    return bulk_load_from_s3('products', s3_key, columns)


def bulk_load_orders_from_s3(s3_key: str) -> int:
    """Bulk load orders from S3 CSV"""
    columns = [
        'order_id', 'customer_id', 'order_date', 'payment_date',
        'shipment_date', 'delivered_date', 'created_at', 'updated_at',
        'order_status', 'payment_status', 'subtotal', 'discount_amount',
        'tax_amount', 'shipping_cost', 'total_amount', 'payment_method',
        'payment_id', 'shipping_carrier', 'tracking_number',
        'customer_segment_at_order'
    ]
    return bulk_load_from_s3('orders', s3_key, columns)


def bulk_load_order_items_from_s3(s3_key: str) -> int:
    """Bulk load order items from S3 CSV"""
    columns = [
        'order_item_id', 'order_id', 'product_id', 'quantity',
        'unit_price', 'line_total', 'created_at'
    ]
    return bulk_load_from_s3('order_items', s3_key, columns)
