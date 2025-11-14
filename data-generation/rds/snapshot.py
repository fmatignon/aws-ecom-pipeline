"""
RDS snapshot operations: Export to Parquet and bulk reload
"""
import pandas as pd
import psycopg2
from pathlib import Path
from typing import Dict, Optional
import tempfile
import os
from datetime import datetime
import io

from .loader import get_db_connection


def export_rds_to_parquet(output_dir: Path) -> Dict[str, Path]:
    """
    Export all RDS tables to Parquet files
    
    Args:
        output_dir: Directory to save Parquet files
    
    Returns:
        Dictionary mapping table names to Parquet file paths
    """
    conn = get_db_connection()
    parquet_files = {}
    
    try:
        tables = ['customers', 'products', 'orders', 'order_items']
        
        for table in tables:
            print(f"  Exporting {table}...", end='', flush=True)
            query = f"SELECT * FROM {table}"
            df = pd.read_sql_query(query, conn)
            
            parquet_path = output_dir / f"{table}.parquet"
            df.to_parquet(parquet_path, index=False, engine='pyarrow')
            parquet_files[table] = parquet_path
            print(f" ✓ ({len(df):,} rows)")
        
        return parquet_files
        
    finally:
        conn.close()


def bulk_reload_from_parquet(parquet_files: Dict[str, Path], drop_tables: bool = True):
    """
    Bulk reload RDS tables from Parquet files
    
    Optimized approach:
    1. Drop existing tables
    2. Create tables WITHOUT constraints/indexes (faster COPY)
    3. Load data using COPY FROM STDIN
    4. Add PRIMARY KEY constraints
    5. Add FOREIGN KEY constraints
    6. Re-enable autovacuum
    
    Args:
        parquet_files: Dictionary mapping table names to Parquet file paths
        drop_tables: Whether to drop and recreate tables
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        if drop_tables:
            print("  Dropping existing tables...", end='', flush=True)
            # Drop in reverse order (respecting foreign keys)
            cursor.execute("DROP TABLE IF EXISTS order_items CASCADE")
            cursor.execute("DROP TABLE IF EXISTS orders CASCADE")
            cursor.execute("DROP TABLE IF EXISTS products CASCADE")
            cursor.execute("DROP TABLE IF EXISTS customers CASCADE")
            conn.commit()
            print(" ✓")
            
            print("  Creating tables WITHOUT constraints/indexes...", end='', flush=True)
            # Create tables WITHOUT PRIMARY KEY or FOREIGN KEY constraints
            # This makes COPY much faster (no constraint checking during load)
            cursor.execute("""
                CREATE TABLE customers (
                    customer_id INTEGER,
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
            
            cursor.execute("""
                CREATE TABLE products (
                    product_id INTEGER,
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
            
            cursor.execute("""
                CREATE TABLE orders (
                    order_id INTEGER,
                    customer_id INTEGER,
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
            
            cursor.execute("""
                CREATE TABLE order_items (
                    order_item_id INTEGER,
                    order_id INTEGER,
                    product_id INTEGER,
                    quantity INTEGER,
                    unit_price DECIMAL(10, 2),
                    line_total DECIMAL(10, 2),
                    created_at TIMESTAMP
                )
            """)
            
            conn.commit()
            print(" ✓")
        
        # Disable autovacuum for all tables during bulk load
        print("  Disabling autovacuum...", end='', flush=True)
        for table in ['customers', 'products', 'orders', 'order_items']:
            try:
                cursor.execute(f"ALTER TABLE {table} SET (autovacuum_enabled = false)")
            except Exception:
                pass
        conn.commit()
        print(" ✓")
        
        # Load data in order (no foreign key constraints yet, but logical order helps)
        load_order = ['customers', 'products', 'orders', 'order_items']
        
        for table in load_order:
            if table not in parquet_files:
                continue
                
            parquet_path = parquet_files[table]
            print(f"  Loading {table} from {parquet_path.name}...", end='', flush=True)
            
            # Read Parquet file
            df = pd.read_parquet(parquet_path)
            
            # Convert to CSV in memory for COPY FROM STDIN
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False, header=False)
            csv_buffer.seek(0)
            
            # Get columns
            columns = list(df.columns)
            columns_str = ', '.join(columns)
            
            # Use COPY FROM STDIN (fastest method - no constraints to check!)
            copy_query = f"""
                COPY {table} ({columns_str})
                FROM STDIN
                WITH (FORMAT csv, DELIMITER ',', NULL '', QUOTE '"', ESCAPE '\\')
            """
            
            cursor.copy_expert(copy_query, csv_buffer)
            conn.commit()
            
            print(f" ✓ ({len(df):,} rows)")
        
        # Now add constraints and indexes (much faster after data is loaded)
        print("  Adding PRIMARY KEY constraints...", end='', flush=True)
        cursor.execute("ALTER TABLE customers ADD PRIMARY KEY (customer_id)")
        cursor.execute("ALTER TABLE products ADD PRIMARY KEY (product_id)")
        cursor.execute("ALTER TABLE orders ADD PRIMARY KEY (order_id)")
        cursor.execute("ALTER TABLE order_items ADD PRIMARY KEY (order_item_id)")
        conn.commit()
        print(" ✓")
        
        print("  Adding FOREIGN KEY constraints...", end='', flush=True)
        cursor.execute("ALTER TABLE orders ADD CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)")
        cursor.execute("ALTER TABLE order_items ADD CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES orders(order_id)")
        cursor.execute("ALTER TABLE order_items ADD CONSTRAINT fk_order_items_product FOREIGN KEY (product_id) REFERENCES products(product_id)")
        conn.commit()
        print(" ✓")
        
        # Re-enable autovacuum
        print("  Re-enabling autovacuum...", end='', flush=True)
        for table in ['customers', 'products', 'orders', 'order_items']:
            try:
                cursor.execute(f"ALTER TABLE {table} SET (autovacuum_enabled = true)")
            except Exception:
                pass
        conn.commit()
        print(" ✓")
        
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

