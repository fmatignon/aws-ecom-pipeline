"""
RDS upsert functions for all tables
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values, execute_batch
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from typing import Optional
import time
import io
from .loader import get_db_connection


def upsert_customers(
    new_customers_df: Optional[pd.DataFrame] = None,
    updated_customers_df: Optional[pd.DataFrame] = None
):
    """
    INSERT new customers and UPDATE existing customers (segments)
    
    Args:
        new_customers_df: DataFrame with new customers to insert
        updated_customers_df: DataFrame with existing customers to update (segments)
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Skip new customer inserts - use bulk_loader instead
        # Only handle updates
        if new_customers_df is not None and len(new_customers_df) > 0:
            print(f"\r      Warning: Skipping {len(new_customers_df):,} new customers (use bulk_loader instead)", end='', flush=True)
            print(f"\r      Preparing {len(new_customers_df):,} customers for insert...", end='', flush=True)
            prep_start = time.time()
            columns = [
                'customer_id', 'first_name', 'last_name', 'email', 'phone',
                'country', 'city', 'state', 'postal_code', 'address',
                'signup_date', 'created_at', 'updated_at',
                'customer_segment', 'date_of_birth', 'gender'
            ]
            
            # Ensure all columns exist
            df = new_customers_df[[col for col in columns if col in new_customers_df.columns]].copy()
            df = df.where(pd.notna(df), None)
            
            values = [tuple(row) for row in df.values]
            prep_elapsed = time.time() - prep_start
            print(f"\r      Inserting {len(df):,} customers...", end='', flush=True)
            import sys
            sys.stdout.flush()  # Ensure output is flushed before long operation
            
            insert_start = time.time()
            insert_query = """
                INSERT INTO customers (
                    customer_id, first_name, last_name, email, phone,
                    country, city, state, postal_code, address,
                    signup_date, created_at, updated_at,
                    customer_segment, date_of_birth, gender
                )
                VALUES %s
                ON CONFLICT (customer_id) DO NOTHING
            """
            
            try:
                # Check connection before executing
                cursor.execute("SELECT 1")
                
                # Use INSERT with smaller batches and intermediate commits to avoid timeout
                # This prevents long-running transactions that trigger statement timeout
                batch_size = 50  # Small batches to avoid timeout and reduce lock time
                inserted_count = 0
                total_batches = (len(values) + batch_size - 1) // batch_size
                
                batch_insert_query = """
                    INSERT INTO customers (
                        customer_id, first_name, last_name, email, phone,
                        country, city, state, postal_code, address,
                        signup_date, created_at, updated_at,
                        customer_segment, date_of_birth, gender
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id) DO NOTHING
                """
                
                print(f"\r      Inserting in {total_batches} batches of {batch_size}...", end='', flush=True)
                sys.stdout.flush()
                
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    batch_num = (i // batch_size) + 1
                    
                    try:
                        execute_batch(cursor, batch_insert_query, batch, page_size=batch_size)
                        conn.commit()  # Commit after each batch to release locks quickly
                        inserted_count += len(batch)
                        
                        # Log progress every 10 batches or at the end
                        if batch_num % 10 == 0 or batch_num == total_batches:
                            elapsed = time.time() - insert_start
                            rate = inserted_count / elapsed if elapsed > 0 else 0
                            print(f"\r      Batch {batch_num}/{total_batches}: {inserted_count:,}/{len(values):,} customers ({rate:.0f} rec/s)...", end='', flush=True)
                            sys.stdout.flush()
                    except Exception as batch_error:
                        conn.rollback()
                        print(f"\r      Error in batch {batch_num} ({i}-{i+batch_size}): {batch_error}")
                        # Continue with next batch
                        continue
                
                insert_elapsed = time.time() - insert_start
                print(f"\r      Inserted {inserted_count:,}/{len(df):,} new customers (prep: {prep_elapsed:.1f}s, insert: {insert_elapsed:.1f}s)")
            except Exception as e:
                conn.rollback()
                print(f"\r      Error inserting customers: {e}")
                import traceback
                traceback.print_exc()
                raise
        
        # Update existing customers (segments) - use optimized batch update
        if updated_customers_df is not None and len(updated_customers_df) > 0:
            df = updated_customers_df.copy()
            df = df.where(pd.notna(df), None)
            total = len(df)
            
            print(f"\r      Preparing optimized batch update for {total:,} customer segments...", end='', flush=True)
            prep_start = time.time()
            
            # Optimization: Drop indexes temporarily
            cursor.execute("""
                SELECT indexname FROM pg_indexes 
                WHERE tablename = 'customers' 
                AND indexname NOT LIKE '%_pkey'
            """)
            indexes_to_drop = [row[0] for row in cursor.fetchall()]
            for idx in indexes_to_drop:
                try:
                    cursor.execute(f"DROP INDEX IF EXISTS {idx}")
                except Exception:
                    pass
            
            # Disable autovacuum temporarily
            try:
                cursor.execute("ALTER TABLE customers SET (autovacuum_enabled = false)")
            except Exception:
                pass
            
            conn.commit()
            
            # Create staging table WITHOUT indexes
            staging_table = f"staging_customer_updates_{int(time.time())}"
            cursor.execute(f"""
                CREATE TEMPORARY TABLE {staging_table} (
                    customer_id INTEGER PRIMARY KEY,
                    customer_segment VARCHAR(50),
                    updated_at TIMESTAMP
                )
            """)
            
            # Prepare data for staging table
            update_data = df[['customer_id', 'customer_segment', 'updated_at']].copy()
            update_data = update_data.where(pd.notna(update_data), None)
            values = [tuple(row) for row in update_data.values]
            
            # Insert into staging table using execute_values (fast batch insert)
            execute_values(
                cursor,
                f"INSERT INTO {staging_table} (customer_id, customer_segment, updated_at) VALUES %s",
                values,
                page_size=1000
            )
            prep_elapsed = time.time() - prep_start
            
            print(f"\r      Updating {total:,} customer segments (set-based)...", end='', flush=True)
            update_start = time.time()
            
            # Set-based UPDATE using UPDATE ... FROM (equivalent to MERGE)
            cursor.execute(f"""
                UPDATE customers
                SET customer_segment = staging.customer_segment,
                    updated_at = staging.updated_at
                FROM {staging_table} staging
                WHERE customers.customer_id = staging.customer_id
            """)
            
            # Drop staging table
            cursor.execute(f"DROP TABLE {staging_table}")
            
            # Re-enable autovacuum
            try:
                cursor.execute("ALTER TABLE customers SET (autovacuum_enabled = true)")
            except Exception:
                pass
            
            update_elapsed = time.time() - update_start
            total_elapsed = time.time() - prep_start
            print(f"\r      Updated {len(df):,} customer segments in {total_elapsed:.1f}s (prep: {prep_elapsed:.1f}s, update: {update_elapsed:.1f}s)")
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def upsert_products(new_products_df: Optional[pd.DataFrame] = None):
    """
    DEPRECATED: Use bulk_loader.bulk_load_products_from_s3() instead
    
    Args:
        new_products_df: DEPRECATED - use bulk_loader instead
    """
    if new_products_df is None or len(new_products_df) == 0:
        return
    
    # Skip - use bulk_loader instead
    print(f"\r      Warning: Skipping {len(new_products_df):,} new products (use bulk_loader instead)", end='', flush=True)
    return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print(f"\r      Preparing {len(new_products_df):,} products for insert...", end='', flush=True)
        prep_start = time.time()
        columns = [
            'product_id', 'product_name', 'category', 'sub_category',
            'brand', 'price', 'cost', 'created_at', 'color'
        ]
        
        df = new_products_df[[col for col in columns if col in new_products_df.columns]].copy()
        df = df.where(pd.notna(df), None)
        
        values = [tuple(row) for row in df.values]
        prep_elapsed = time.time() - prep_start
        
        print(f"\r      Inserting {len(df):,} products...", end='', flush=True)
        insert_start = time.time()
        insert_query = """
            INSERT INTO products (
                product_id, product_name, category, sub_category,
                brand, price, cost, created_at, color
            )
            VALUES %s
            ON CONFLICT (product_id) DO NOTHING
        """
        
        execute_values(cursor, insert_query, values, page_size=1000)
        conn.commit()
        insert_elapsed = time.time() - insert_start
        print(f"\r      Inserted {len(df):,} new products (prep: {prep_elapsed:.1f}s, insert: {insert_elapsed:.1f}s)")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def upsert_orders(
    new_orders_df: Optional[pd.DataFrame] = None,
    updated_orders_df: Optional[pd.DataFrame] = None
):
    """
    UPDATE existing orders (status, dates) only
    
    Note: New orders should be loaded via bulk_loader.bulk_load_orders_from_s3()
    This function only handles updates to existing records.
    
    Args:
        new_orders_df: DEPRECATED - use bulk_loader instead. Ignored if provided.
        updated_orders_df: DataFrame with existing orders to update
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Skip new order inserts - use bulk_loader instead
        # Only handle updates
        if new_orders_df is not None and len(new_orders_df) > 0:
            print(f"\r      Warning: Skipping {len(new_orders_df):,} new orders (use bulk_loader instead)", end='', flush=True)
            print(f"\r      Preparing {len(new_orders_df):,} orders for insert...", end='', flush=True)
            prep_start = time.time()
            columns = [
                'order_id', 'customer_id', 'order_date', 'payment_date',
                'shipment_date', 'delivered_date', 'created_at', 'updated_at',
                'order_status', 'payment_status', 'subtotal', 'discount_amount',
                'tax_amount', 'shipping_cost', 'total_amount', 'payment_method',
                'payment_id', 'shipping_carrier', 'tracking_number',
                'customer_segment_at_order'
            ]
            
            df = new_orders_df[[col for col in columns if col in new_orders_df.columns]].copy()
            df = df.where(pd.notna(df), None)
            
            values = [tuple(row) for row in df.values]
            prep_elapsed = time.time() - prep_start
            
            print(f"\r      Inserting {len(df):,} orders...", end='', flush=True)
            insert_start = time.time()
            insert_query = """
                INSERT INTO orders (
                    order_id, customer_id, order_date, payment_date,
                    shipment_date, delivered_date, created_at, updated_at,
                    order_status, payment_status, subtotal, discount_amount,
                    tax_amount, shipping_cost, total_amount, payment_method,
                    payment_id, shipping_carrier, tracking_number,
                    customer_segment_at_order
                )
                VALUES %s
                ON CONFLICT (order_id) DO NOTHING
            """
            
            execute_values(cursor, insert_query, values, page_size=1000)
            insert_elapsed = time.time() - insert_start
            print(f"\r      Inserted {len(df):,} new orders (prep: {prep_elapsed:.1f}s, insert: {insert_elapsed:.1f}s)")
        
        # Update existing orders - use optimized batch update
        if updated_orders_df is not None and len(updated_orders_df) > 0:
            df = updated_orders_df.copy()
            df = df.where(pd.notna(df), None)
            total = len(df)
            
            print(f"\r      Preparing optimized batch update for {total:,} orders...", end='', flush=True)
            prep_start = time.time()
            
            # Optimization: Drop indexes temporarily
            cursor.execute("""
                SELECT indexname FROM pg_indexes 
                WHERE tablename = 'orders' 
                AND indexname NOT LIKE '%_pkey'
            """)
            indexes_to_drop = [row[0] for row in cursor.fetchall()]
            for idx in indexes_to_drop:
                try:
                    cursor.execute(f"DROP INDEX IF EXISTS {idx}")
                except Exception:
                    pass
            
            # Disable autovacuum temporarily
            try:
                cursor.execute("ALTER TABLE orders SET (autovacuum_enabled = false)")
            except Exception:
                pass
            
            conn.commit()
            
            # Create staging table WITHOUT indexes
            staging_table = f"staging_order_updates_{int(time.time())}"
            cursor.execute(f"""
                CREATE TEMPORARY TABLE {staging_table} (
                    order_id INTEGER PRIMARY KEY,
                    order_status VARCHAR(50),
                    payment_status VARCHAR(50),
                    payment_date TIMESTAMP,
                    shipment_date TIMESTAMP,
                    delivered_date TIMESTAMP,
                    updated_at TIMESTAMP
                )
            """)
            
            # Prepare data for staging table
            update_columns = ['order_id', 'order_status', 'payment_status', 'payment_date', 
                            'shipment_date', 'delivered_date', 'updated_at']
            update_data = df[update_columns].copy()
            update_data = update_data.where(pd.notna(update_data), None)
            values = [tuple(row) for row in update_data.values]
            
            # Insert into staging table using execute_values (fast batch insert)
            execute_values(
                cursor,
                f"""INSERT INTO {staging_table} 
                   (order_id, order_status, payment_status, payment_date, shipment_date, delivered_date, updated_at) 
                   VALUES %s""",
                values,
                page_size=1000
            )
            prep_elapsed = time.time() - prep_start
            
            print(f"\r      Updating {total:,} orders (set-based)...", end='', flush=True)
            update_start = time.time()
            
            # Set-based UPDATE using UPDATE ... FROM (equivalent to MERGE)
            cursor.execute(f"""
                UPDATE orders
                SET order_status = staging.order_status,
                    payment_status = staging.payment_status,
                    payment_date = staging.payment_date,
                    shipment_date = staging.shipment_date,
                    delivered_date = staging.delivered_date,
                    updated_at = staging.updated_at
                FROM {staging_table} staging
                WHERE orders.order_id = staging.order_id
            """)
            
            # Drop staging table
            cursor.execute(f"DROP TABLE {staging_table}")
            
            # Re-enable autovacuum
            try:
                cursor.execute("ALTER TABLE orders SET (autovacuum_enabled = true)")
            except Exception:
                pass
            
            update_elapsed = time.time() - update_start
            total_elapsed = time.time() - prep_start
            print(f"\r      Updated {len(df):,} orders in {total_elapsed:.1f}s (prep: {prep_elapsed:.1f}s, update: {update_elapsed:.1f}s)")
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def upsert_order_items(new_order_items_df: Optional[pd.DataFrame] = None):
    """
    DEPRECATED: Use bulk_loader.bulk_load_order_items_from_s3() instead
    
    Args:
        new_order_items_df: DEPRECATED - use bulk_loader instead
    """
    if new_order_items_df is None or len(new_order_items_df) == 0:
        return
    
    # Skip - use bulk_loader instead
    print(f"\r      Warning: Skipping {len(new_order_items_df):,} new order items (use bulk_loader instead)", end='', flush=True)
    return
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        print(f"\r      Preparing {len(new_order_items_df):,} order items for insert...", end='', flush=True)
        prep_start = time.time()
        columns = [
            'order_item_id', 'order_id', 'product_id', 'quantity',
            'unit_price', 'line_total', 'created_at'
        ]
        
        df = new_order_items_df[[col for col in columns if col in new_order_items_df.columns]].copy()
        df = df.where(pd.notna(df), None)
        
        values = [tuple(row) for row in df.values]
        prep_elapsed = time.time() - prep_start
        
        print(f"\r      Inserting {len(df):,} order items...", end='', flush=True)
        insert_start = time.time()
        insert_query = """
            INSERT INTO order_items (
                order_item_id, order_id, product_id, quantity,
                unit_price, line_total, created_at
            )
            VALUES %s
            ON CONFLICT (order_item_id) DO NOTHING
        """
        
        execute_values(cursor, insert_query, values, page_size=1000)
        conn.commit()
        insert_elapsed = time.time() - insert_start
        print(f"\r      Inserted {len(df):,} new order items (prep: {prep_elapsed:.1f}s, insert: {insert_elapsed:.1f}s)")
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

