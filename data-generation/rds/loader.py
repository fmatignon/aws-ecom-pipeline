"""
RDS loader functions to query existing data + mode detection
"""
import os
import pandas as pd
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
import boto3
import json
from datetime import datetime, date
from typing import Optional

# Load environment variables from .env file (for local development)
# In ECS, environment variables are set via container definition
env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)


def get_db_credentials():
    """Get database credentials from environment or Secrets Manager"""
    # Try environment variables first
    endpoint = os.getenv('RDS_ENDPOINT')
    db_name = os.getenv('RDS_DATABASE_NAME', 'ecommerce')
    username = os.getenv('RDS_USERNAME')
    password = os.getenv('RDS_PASSWORD')
    secret_arn = os.getenv('RDS_SECRET_ARN')
    
    # AWS Secrets Manager client
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    aws_profile = os.getenv('AWS_PROFILE')
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    if aws_profile:
        session = boto3.Session(profile_name=aws_profile)
    elif aws_access_key and aws_secret_key:
        session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region
        )
    else:
        session = boto3.Session()
    
    secrets_client = session.client('secretsmanager', region_name=region)
    
    # If secret ARN is provided, fetch from Secrets Manager
    if secret_arn and not password:
        try:
            response = secrets_client.get_secret_value(SecretId=secret_arn)
            secret = json.loads(response['SecretString'])
            username = secret.get('username', username)
            password = secret.get('password', password)
        except Exception as e:
            print(f"Warning: Could not fetch secret from Secrets Manager: {e}")
    
    if not all([endpoint, username, password]):
        raise ValueError(
            "Missing database credentials. Please set:\n"
            "  - RDS_ENDPOINT\n"
            "  - RDS_USERNAME (or use Secrets Manager)\n"
            "  - RDS_PASSWORD (or use Secrets Manager)\n"
            "  - RDS_SECRET_ARN (optional, for Secrets Manager)"
        )
    
    return {
        'host': endpoint,
        'database': db_name,
        'user': username,
        'password': password,
    }


def get_db_connection():
    """Get database connection with timeout settings"""
    db_config = get_db_credentials()
    # Add connection timeout
    db_config['connect_timeout'] = 10  # 10 seconds to establish connection
    conn = psycopg2.connect(**db_config)
    # Set statement timeout to 10 minutes (600 seconds) for bulk operations
    # Note: We'll handle timeouts at the operation level for better control
    with conn.cursor() as cursor:
        cursor.execute("SET statement_timeout = '600s'")
    conn.commit()
    return conn


def check_data_exists() -> bool:
    """
    Quick check if any data exists in RDS (for mode detection)
    
    Returns:
        True if any customers exist, False otherwise
    """
    try:
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM customers")
            count = cursor.fetchone()[0]
            cursor.close()
            return count > 0
        finally:
            conn.close()
    except Exception:
        # If table doesn't exist or connection fails, assume no data
        return False


def get_last_order_date() -> Optional[date]:
    """
    Get the most recent order creation date from RDS
    
    Returns:
        date object representing the last order date, or None if no orders exist
    """
    try:
        conn = get_db_connection()
        try:
            query = "SELECT MAX(order_date) FROM orders"
            df = pd.read_sql_query(query, conn)
            max_date = df.iloc[0, 0]
            if pd.isna(max_date):
                return None
            # Convert to date if it's a datetime
            if isinstance(max_date, datetime):
                return max_date.date()
            elif isinstance(max_date, date):
                return max_date
            else:
                # Parse string
                return datetime.strptime(str(max_date), '%Y-%m-%d').date()
        finally:
            conn.close()
    except Exception as e:
        print(f"Warning: Could not get last order date from RDS: {e}")
        return None


def load_existing_customers() -> pd.DataFrame:
    """
    Query all customers from RDS
    
    Returns:
        DataFrame with all customer records
    """
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                customer_id, first_name, last_name, email, phone,
                country, city, state, postal_code, address,
                signup_date, created_at, updated_at,
                customer_segment, date_of_birth, gender
            FROM customers
            ORDER BY customer_id
        """
        df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()


def load_existing_products() -> pd.DataFrame:
    """
    Query all products from RDS
    
    Returns:
        DataFrame with all product records
    """
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                product_id, product_name, category, sub_category,
                brand, price, cost, created_at, color
            FROM products
            ORDER BY product_id
        """
        df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()


def load_existing_orders(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Query orders in date range
    
    Args:
        start_date: Start date for order date range
        end_date: End date for order date range
    
    Returns:
        DataFrame with orders in the date range
    """
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                order_id, customer_id, order_date, payment_date,
                shipment_date, delivered_date, created_at, updated_at,
                order_status, payment_status, subtotal, discount_amount,
                tax_amount, shipping_cost, total_amount, payment_method,
                payment_id, shipping_carrier, tracking_number,
                customer_segment_at_order
            FROM orders
            WHERE order_date >= %s AND order_date <= %s
            ORDER BY order_id
        """
        df = pd.read_sql_query(
            query,
            conn,
            params=(start_date, end_date)
        )
        return df
    finally:
        conn.close()


def load_all_orders_for_updates_and_history(end_date: date) -> tuple:
    """
    Load all orders once and return both:
    1. Orders for updates (not in final states)
    2. All historical orders (for customer segments)
    
    This avoids loading orders twice, saving memory and time.
    
    Args:
        end_date: End date for historical orders
    
    Returns:
        tuple: (orders_for_updates_df, all_historical_orders_df)
    """
    conn = get_db_connection()
    try:
        # Load all orders up to end_date in one query
        query = """
            SELECT 
                order_id, customer_id, order_date, payment_date,
                shipment_date, delivered_date, created_at, updated_at,
                order_status, payment_status, subtotal, discount_amount,
                tax_amount, shipping_cost, total_amount, payment_method,
                payment_id, shipping_carrier, tracking_number,
                customer_segment_at_order
            FROM orders
            WHERE order_date <= %s
            ORDER BY order_id
        """
        all_orders_df = pd.read_sql_query(
            query,
            conn,
            params=(end_date,)
        )
        
        # Filter for updates (not in final states)
        orders_for_updates_df = all_orders_df[
            ~all_orders_df['order_status'].isin(['cancelled', 'refunded'])
        ].copy()
        
        return orders_for_updates_df, all_orders_df
    finally:
        conn.close()


def load_orders_for_updates() -> pd.DataFrame:
    """
    Query all orders that are NOT in final states (cancelled, refunded)
    These orders may need status updates
    
    DEPRECATED: Use load_all_orders_for_updates_and_history() instead to avoid loading twice
    
    Returns:
        DataFrame with orders that can be updated
    """
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                order_id, customer_id, order_date, payment_date,
                shipment_date, delivered_date, created_at, updated_at,
                order_status, payment_status, subtotal, discount_amount,
                tax_amount, shipping_cost, total_amount, payment_method,
                payment_id, shipping_carrier, tracking_number,
                customer_segment_at_order
            FROM orders
            WHERE order_status NOT IN ('cancelled', 'refunded')
            ORDER BY order_id
        """
        df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()


def load_all_historical_orders(end_date: date) -> pd.DataFrame:
    """
    Query all historical orders up to end_date (for customer segment calculation)
    
    DEPRECATED: Use load_all_orders_for_updates_and_history() instead to avoid loading twice
    
    Args:
        end_date: End date for historical orders
    
    Returns:
        DataFrame with all historical orders
    """
    conn = get_db_connection()
    try:
        query = """
            SELECT 
                order_id, customer_id, order_date, payment_date,
                shipment_date, delivered_date, created_at, updated_at,
                order_status, payment_status, subtotal, discount_amount,
                tax_amount, shipping_cost, total_amount, payment_method,
                payment_id, shipping_carrier, tracking_number,
                customer_segment_at_order
            FROM orders
            WHERE order_date <= %s
            ORDER BY order_id
        """
        df = pd.read_sql_query(
            query,
            conn,
            params=(end_date,)
        )
        return df
    finally:
        conn.close()


def load_existing_order_items(order_ids: list) -> pd.DataFrame:
    """
    Query order items for specific orders
    
    Args:
        order_ids: List of order IDs to fetch items for
    
    Returns:
        DataFrame with order items
    """
    if not order_ids:
        return pd.DataFrame()
    
    conn = get_db_connection()
    try:
        # Use parameterized query with tuple for IN clause
        placeholders = ','.join(['%s'] * len(order_ids))
        query = f"""
            SELECT 
                order_item_id, order_id, product_id, quantity,
                unit_price, line_total, created_at
            FROM order_items
            WHERE order_id IN ({placeholders})
            ORDER BY order_item_id
        """
        df = pd.read_sql_query(query, conn, params=tuple(order_ids))
        return df
    finally:
        conn.close()

