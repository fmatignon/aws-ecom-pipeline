#!/usr/bin/env python3
"""
Load CSV data into PostgreSQL RDS database.
Creates tables and bulk loads data from customers, products, orders, and order_items CSVs.
"""
import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from dotenv import load_dotenv
import boto3
import json

# Load environment variables from project root .env file
env_path = Path(__file__).parent.parent.parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
else:
    print(f"Warning: .env file not found at {env_path}")

# Configuration
CSV_DIR = Path(__file__).parent.parent.parent / 'data-generation' / 'output'

# AWS Secrets Manager client (for retrieving RDS credentials)
# Ensure AWS credentials are loaded from environment
region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
# Set AWS profile if specified in .env, otherwise use credentials from env vars
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
    session = boto3.Session()  # Use default credentials chain

secrets_client = session.client('secretsmanager', region_name=region)


def get_db_credentials():
    """Get database credentials from environment or Secrets Manager"""
    # Try environment variables first
    endpoint = os.getenv('RDS_ENDPOINT')
    db_name = os.getenv('RDS_DATABASE_NAME', 'ecommerce')
    username = os.getenv('RDS_USERNAME')
    password = os.getenv('RDS_PASSWORD')
    secret_arn = os.getenv('RDS_SECRET_ARN')
    
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


def create_tables(conn):
    """Create database tables"""
    cursor = conn.cursor()
    
    print("Creating tables...")
    
    # Customers table - drop if exists to recreate with correct schema
    cursor.execute("DROP TABLE IF EXISTS customers CASCADE")
    cursor.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            email VARCHAR(255),
            phone VARCHAR(50),
            country VARCHAR(100),
            city VARCHAR(100),
            state VARCHAR(100),
            postal_code VARCHAR(20),
            address TEXT,
            signup_date TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            customer_segment VARCHAR(50),
            date_of_birth DATE,
            gender VARCHAR(10)
        )
    """)
    
    # Products table - drop if exists to recreate with correct schema
    cursor.execute("DROP TABLE IF EXISTS products CASCADE")
    cursor.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name VARCHAR(500),
            category VARCHAR(100),
            sub_category VARCHAR(100),
            brand VARCHAR(100),
            price DECIMAL(10, 2),
            cost DECIMAL(10, 2),
            created_at TIMESTAMP,
            color VARCHAR(50)
        )
    """)
    
    # Orders table - drop if exists to recreate with correct schema
    cursor.execute("DROP TABLE IF EXISTS orders CASCADE")
    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER REFERENCES customers(customer_id),
            order_date TIMESTAMP,
            payment_date TIMESTAMP,
            shipment_date TIMESTAMP,
            delivered_date TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            order_status VARCHAR(50),
            payment_status VARCHAR(50),
            subtotal DECIMAL(10, 2),
            discount_amount DECIMAL(10, 2),
            tax_amount DECIMAL(10, 2),
            shipping_cost DECIMAL(10, 2),
            total_amount DECIMAL(10, 2),
            payment_method VARCHAR(50),
            payment_id DECIMAL,
            shipping_carrier VARCHAR(50),
            tracking_number DECIMAL,
            customer_segment_at_order VARCHAR(50)
        )
    """)
    
    # Order items table - drop if exists to recreate with correct schema
    cursor.execute("DROP TABLE IF EXISTS order_items CASCADE")
    cursor.execute("""
        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER REFERENCES orders(order_id),
            product_id INTEGER REFERENCES products(product_id),
            quantity INTEGER,
            unit_price DECIMAL(10, 2),
            line_total DECIMAL(10, 2),
            created_at TIMESTAMP
        )
    """)
    
    conn.commit()
    print("✓ Tables created successfully")


def load_csv_to_table(conn, csv_path: Path, table_name: str, chunk_size: int = 10000):
    """Load CSV data into database table"""
    print(f"Loading {table_name} from {csv_path.name}...")
    
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    cursor = conn.cursor()
    total_rows = 0
    
    # Read CSV in chunks
    for chunk_num, chunk_df in enumerate(pd.read_csv(csv_path, chunksize=chunk_size), 1):
        # Replace NaN with None for proper NULL handling
        chunk_df = chunk_df.where(pd.notna(chunk_df), None)
        
        # Get column names
        columns = list(chunk_df.columns)
        placeholders = ','.join(['%s'] * len(columns))
        
        # Prepare data tuples
        values = [tuple(row) for row in chunk_df.values]
        
        # Insert data using execute_values (handles multiple rows efficiently)
        insert_query = f"""
            INSERT INTO {table_name} ({','.join(columns)})
            VALUES %s
            ON CONFLICT DO NOTHING
        """
        
        try:
            execute_values(cursor, insert_query, values, page_size=chunk_size)
            conn.commit()
            total_rows += len(chunk_df)
            
            if chunk_num % 10 == 0:
                print(f"  Processed {total_rows:,} rows...")
        except Exception as e:
            conn.rollback()
            print(f"Error inserting chunk {chunk_num}: {e}")
            raise
    
    print(f"✓ Loaded {total_rows:,} rows into {table_name}")


def main():
    """Main function"""
    print("=" * 70)
    print("RDS Data Loader")
    print("=" * 70)
    print()
    
    try:
        # Get database credentials
        db_config = get_db_credentials()
        print(f"Connecting to database: {db_config['host']}/{db_config['database']}")
        
        # Connect to database
        conn = psycopg2.connect(**db_config)
        print("✓ Connected to database")
        print()
        
        # Create tables
        create_tables(conn)
        print()
        
        # Load data in correct order (respecting foreign keys)
        load_csv_to_table(conn, CSV_DIR / 'customers.csv', 'customers')
        print()
        
        load_csv_to_table(conn, CSV_DIR / 'products.csv', 'products')
        print()
        
        load_csv_to_table(conn, CSV_DIR / 'orders.csv', 'orders')
        print()
        
        load_csv_to_table(conn, CSV_DIR / 'order_items.csv', 'order_items')
        print()
        
        # Close connection
        conn.close()
        
        print("=" * 70)
        print("✓ All data loaded successfully!")
        print("=" * 70)
    
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()

