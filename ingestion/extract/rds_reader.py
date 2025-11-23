"""
RDS data extraction module.

Connects to PostgreSQL database using credentials from AWS Secrets Manager
and extracts new/changed records based on updated_at timestamps.
"""

from typing import List, Dict, Any, Optional
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

from config import Config
from utils.logging_utils import log_progress, log_error


def get_db_credentials() -> Dict[str, str]:
    """
    Retrieve database credentials from AWS Secrets Manager.

    Returns:
        Dict containing database connection parameters

    Raises:
        Exception: If credentials cannot be retrieved
    """
    try:
        return Config.get_rds_connection_details()
    except Exception as e:
        raise Exception(f"Failed to retrieve database credentials: {e}")


def extract_rds_data(entity: str, last_sync_timestamp: str) -> List[Dict[str, Any]]:
    """
    Extract data from RDS for a specific entity since the last sync timestamp.

    Args:
        entity: The entity name ('customers', 'products', 'orders')
        last_sync_timestamp: ISO format timestamp to extract records after

    Returns:
        List of dictionaries containing the extracted records
    """
    credentials = get_db_credentials()

    # Define queries for each entity
    # Note: Queries match actual database schema from snapshot.py
    queries = {
        "customers": """
            SELECT
                customer_id,
                first_name,
                last_name,
                email,
                phone,
                country,
                city,
                state,
                postal_code,
                address,
                signup_date,
                created_at,
                updated_at,
                customer_segment,
                date_of_birth,
                gender
            FROM customers
            WHERE updated_at >= %s
            ORDER BY updated_at ASC
        """,
        "products": """
            SELECT
                product_id,
                product_name,
                category,
                sub_category,
                brand,
                price,
                cost,
                created_at,
                color
            FROM products
            WHERE created_at >= %s
            ORDER BY created_at ASC
        """,
        "orders": """
            SELECT
                order_id,
                customer_id,
                order_date,
                payment_date,
                shipment_date,
                delivered_date,
                created_at,
                updated_at,
                order_status,
                payment_status,
                subtotal,
                discount_amount,
                tax_amount,
                shipping_cost,
                total_amount,
                payment_method,
                payment_id,
                shipping_carrier,
                tracking_number,
                customer_segment_at_order
            FROM orders
            WHERE updated_at >= %s
            ORDER BY updated_at ASC
        """,
    }

    if entity not in queries:
        raise ValueError(f"Unsupported entity: {entity}")

    records = []
    try:
        with psycopg2.connect(**credentials) as conn:
            # Use server-side cursor for large datasets to avoid memory issues and timeouts
            # This fetches data in batches instead of loading everything at once
            with conn.cursor(
                name=f"fetch_{entity}",  # Named cursor for server-side processing
                cursor_factory=RealDictCursor,
            ) as cursor:
                log_progress(
                    f"RDS Extraction - {entity}",
                    f"Executing query since {last_sync_timestamp}",
                )

                # Set cursor fetch size to process in batches (10000 records at a time)
                cursor.itersize = 10000

                cursor.execute(queries[entity], (last_sync_timestamp,))

                # Fetch rows in batches to avoid memory issues and allow progress tracking
                batch_size = 10000
                batch_count = 0

                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    # Convert rows to dictionaries and handle datetime serialization
                    for row in rows:
                        record = {}
                        for key, value in row.items():
                            if isinstance(value, datetime):
                                # Convert datetime to ISO format string
                                record[key] = value.isoformat()
                            else:
                                record[key] = value
                        records.append(record)
                    batch_count += 1
                    if batch_count % 10 == 0:  # Log progress every 100k records
                        log_progress(
                            f"RDS Extraction - {entity}",
                            f"Processed {len(records)} records so far...",
                        )

                log_progress(
                    f"RDS Extraction - {entity}", f"Extracted {len(records)} records"
                )

    except psycopg2.Error as e:
        log_error(f"RDS Extraction - {entity}", f"Database error: {e}")
        raise
    except Exception as e:
        log_error(f"RDS Extraction - {entity}", str(e))
        raise

    return records
