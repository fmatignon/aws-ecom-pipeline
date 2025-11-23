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
from utils.logging_utils import (
    log_section_start,
    log_section_complete,
    log_progress,
    log_error,
)

# Load environment variables from .env file (for local development)
# In ECS, environment variables are set via container definition
env_path = Path(__file__).parent.parent.parent.parent / ".env"
try:
    if env_path.exists():
        section = "Environment Loading"
        log_section_start(section)
        log_progress(section, f"Loading environment variables from {env_path}")
        load_dotenv(dotenv_path=env_path, override=True)
        log_section_complete(section, f"Loaded variables from {env_path}")
except Exception as exc:  # pragma: no cover - best-effort load
    log_error("Environment Loading", f"Failed to load .env: {exc}")


def get_db_credentials() -> dict[str, str]:
    """Get database credentials from Secrets Manager (same as ingestion pipeline)"""
    secret_arn = os.getenv("RDS_SECRET_ARN")

    if not secret_arn:
        raise ValueError("RDS_SECRET_ARN environment variable is required")

    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    section = "Secrets Manager Access"
    log_section_start(section)
    log_progress(
        section, f"Retrieving RDS credentials from Secrets Manager: {secret_arn}"
    )
    log_progress(section, f"Using region: {region}")

    try:
        session = boto3.Session()
        secrets_client = session.client("secretsmanager", region_name=region)

        response = secrets_client.get_secret_value(SecretId=secret_arn)
        secret = json.loads(response["SecretString"])

        # Required keys from the secret
        required_keys = ["host", "port", "username", "password"]
        missing_keys = [key for key in required_keys if key not in secret]
        if missing_keys:
            raise ValueError(f"RDS secret missing required keys: {', '.join(missing_keys)}")

        # Use database name from secret or fallback to default
        database_name = secret.get("dbname") or secret.get("database") or "ecommerce"

        log_progress(section, f"Successfully retrieved credentials for host: {secret['host']}")
        log_section_complete(section, "RDS credentials retrieved from Secrets Manager")

        return {
            "host": secret["host"],
            "port": int(secret["port"]),
            "database": database_name,
            "user": secret["username"],
            "password": secret["password"],
        }

    except Exception as exc:
        log_error(section, f"Failed to retrieve RDS credentials from Secrets Manager: {exc}")
        import traceback
        log_progress(section, f"Traceback: {traceback.format_exc()}")
        raise ValueError(
            "Failed to retrieve database credentials from Secrets Manager. "
            "Ensure RDS_SECRET_ARN is set and IAM permissions are correct."
        )


def get_db_connection():
    """Get database connection with timeout settings"""
    db_config = get_db_credentials()
    # Add connection timeout
    db_config["connect_timeout"] = 10  # 10 seconds to establish connection
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
    except Exception as e:
        log_error("check_data_exists", f"Could not connect to RDS: {e}")
        log_progress(
            "check_data_exists", "Assuming no existing data (local development mode)"
        )
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
                return datetime.strptime(str(max_date), "%Y-%m-%d").date()
        finally:
            conn.close()
    except Exception as e:
        log_error("get_last_order_date", f"Could not get last order date from RDS: {e}")
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
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
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
        all_orders_df = pd.read_sql_query(query, conn, params=(end_date,))

        # Filter for updates (not in final states)
        orders_for_updates_df = all_orders_df[
            ~all_orders_df["order_status"].isin(["cancelled", "refunded"])
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
        df = pd.read_sql_query(query, conn, params=(end_date,))
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
        placeholders = ",".join(["%s"] * len(order_ids))
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
