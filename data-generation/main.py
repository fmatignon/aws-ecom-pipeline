"""
Unified main entry point for data generation (handles both initial and ongoing operations)
"""
import sys
import os
import time
from pathlib import Path
from datetime import datetime, date, timedelta
import pandas as pd
import random
from dotenv import load_dotenv

# Load environment variables from .env file (for local development)
# In ECS, environment variables are set via container definition
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)

# Add data-generation directory to path
script_dir = Path(__file__).parent.absolute()
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

from state import get_last_run_date, write_run_log
from rds.loader import (
    check_data_exists,
    get_last_order_date,
)
from generators.customers import generate_customers_for_date_range
from generators.products import generate_products_for_date_range
from generators.orders import generate_orders_for_date_range, update_existing_orders
from generators.payments import generate_payments
from generators.shipments import generate_shipments
from s3.parquet_manager import update_payments_parquet, update_shipments_parquet
from config.settings import (
    VIP_SPENDING_THRESHOLD, VIP_SUBSCRIPTION_RATE, NEW_CUSTOMER_DURATION_DAYS
)


def detect_mode():
    """
    Detect if this is an initial run or ongoing operations
    
    Returns:
        tuple: (mode: str, start_date: date)
        mode: 'initial' or 'ongoing'
        start_date: date to start generation from
    """
    # Check if RDS has any data first (doesn't require AWS credentials)
    has_rds_data = check_data_exists()
    
    # Check if S3 logs exist (only if RDS has data, or handle error gracefully)
    last_run_date = None
    if has_rds_data:
        # Try to get logs, but don't fail if S3 is unavailable
        try:
            last_run_date = get_last_run_date()
        except Exception as e:
            print(f"  Warning: Could not access S3 logs: {e}")
            print("  Will use RDS order date instead.")
    
    if not has_rds_data:
        # No RDS data - check if logs exist (for initial run detection)
        if last_run_date is None:
            try:
                last_run_date = get_last_run_date()
            except Exception:
                # S3 unavailable or no logs - this is fine for initial run
                pass
        
        if last_run_date is None:
            # Initial run mode - require START_DATE env var
            start_date_str = os.getenv('START_DATE')
            if not start_date_str:
                raise ValueError(
                    "Initial run detected (no existing data). "
                    "Please provide START_DATE environment variable (YYYY-MM-DD format)"
                )
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            except ValueError:
                raise ValueError(f"Invalid START_DATE format: {start_date_str}. Expected YYYY-MM-DD")
            
            return 'initial', start_date
    
    # Ongoing operations mode (RDS has data)
    if last_run_date is None:
        # No logs but RDS has data - use last order date from RDS
        last_order_date = get_last_order_date()
        if last_order_date:
            start_date = last_order_date + timedelta(days=1)
            print(f"  Found existing RDS data. Using last order date: {last_order_date}")
        else:
            # Fallback: start from today if no orders found
            start_date = date.today()
            print(f"  Found existing RDS data but no orders. Starting from today.")
    else:
        # Continue from last run date + 1 day
        start_date = last_run_date + timedelta(days=1)
    
    return 'ongoing', start_date


def update_customer_segments(customers_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Update customer segments based on order history
    """
    if len(orders_df) == 0:
        return customers_df.copy()
    
    # Build order history per customer
    customer_order_history = {}
    for _, order in orders_df.iterrows():
        customer_id = order['customer_id']
        order_date_val = order['order_date']
        # Handle both string and datetime/timestamp objects
        if isinstance(order_date_val, str):
            order_date = datetime.strptime(order_date_val, '%Y-%m-%d %H:%M:%S')
        elif isinstance(order_date_val, datetime):
            order_date = order_date_val
        elif isinstance(order_date_val, pd.Timestamp):
            order_date = order_date_val.to_pydatetime()
        else:
            order_date = pd.to_datetime(order_date_val).to_pydatetime()
        total_amount = order['total_amount']
        
        if customer_id not in customer_order_history:
            customer_order_history[customer_id] = []
        customer_order_history[customer_id].append((order_date, total_amount))
    
    # Assign VIP subscribers
    all_customer_ids = customers_df['customer_id'].tolist()
    num_vip_subscribers = int(len(all_customer_ids) * VIP_SUBSCRIPTION_RATE)
    vip_subscribers = set(random.sample(all_customer_ids, num_vip_subscribers))
    
    # Update segments
    updated_customers = []
    for _, customer in customers_df.iterrows():
        customer_id = customer['customer_id']
        signup_date_val = customer['signup_date']
        # Handle both string and datetime/timestamp objects
        if isinstance(signup_date_val, str):
            signup_date = datetime.strptime(signup_date_val, '%Y-%m-%d %H:%M:%S')
        elif isinstance(signup_date_val, datetime):
            signup_date = signup_date_val
        elif isinstance(signup_date_val, pd.Timestamp):
            signup_date = signup_date_val.to_pydatetime()
        else:
            signup_date = pd.to_datetime(signup_date_val).to_pydatetime()
        
        # Get order history
        order_history = customer_order_history.get(customer_id, [])
        
        # Determine segment
        if customer_id in vip_subscribers:
            segment = 'VIP'
        else:
            days_since_signup = (datetime.now() - signup_date).days
            num_orders = len(order_history)
            
            if days_since_signup < NEW_CUSTOMER_DURATION_DAYS or num_orders < 2:
                segment = 'New'
            else:
                # Calculate recent spending
                six_months_ago = datetime.now() - timedelta(days=180)
                recent_spending = sum(
                    amount for order_date, amount in order_history
                    if order_date >= six_months_ago
                )
                
                if recent_spending > VIP_SPENDING_THRESHOLD:
                    segment = 'VIP'
                else:
                    segment = 'Regular'
        
        customer_dict = customer.to_dict()
        old_segment = customer_dict.get('customer_segment')
        
        # Update if segment changed
        if old_segment != segment:
            customer_dict['customer_segment'] = segment
            customer_dict['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        updated_customers.append(customer_dict)
    
    return pd.DataFrame(updated_customers)


def main():
    """Main orchestration function"""
    print("=" * 70)
    print("UNIFIED DATA GENERATION")
    print("=" * 70)
    print()
    
    try:
        # 1. Detect mode
        print("Detecting mode...")
        mode, start_date = detect_mode()
        print(f"  Mode: {mode.upper()}")
        print(f"  Start date: {start_date}")
        
        # 2. Determine date range
        end_date = date.today() - timedelta(days=1)  # Up to yesterday
        
        if start_date > end_date:
            print(f"  No new data to generate. Start: {start_date}, End: {end_date}")
            return
        
        print(f"  Date range: {start_date} to {end_date}")
        print()
        
        # 3. Export RDS to Parquet (snapshot approach)
        import tempfile
        from rds.snapshot import export_rds_to_parquet
        
        temp_dir = Path(tempfile.mkdtemp())
        print(f"Creating snapshot directory: {temp_dir}")
        
        existing_customers_df = pd.DataFrame()
        existing_products_df = pd.DataFrame()
        existing_orders_df = pd.DataFrame()
        existing_order_items_df = pd.DataFrame()
        start_customer_id = 1
        start_product_id = 1
        start_order_id = 1
        start_order_item_id = 1
        start_payment_id = 1
        start_tracking_number = 1
        
        if mode == 'ongoing':
            print("Exporting RDS database to Parquet snapshot...")
            start_time = time.time()
            
            parquet_files = export_rds_to_parquet(temp_dir)
            
            # Load from Parquet files
            print("  Loading from Parquet snapshot...", end='', flush=True)
            if 'customers' in parquet_files:
                existing_customers_df = pd.read_parquet(parquet_files['customers'])
            if 'products' in parquet_files:
                existing_products_df = pd.read_parquet(parquet_files['products'])
            if 'orders' in parquet_files:
                existing_orders_df = pd.read_parquet(parquet_files['orders'])
            if 'order_items' in parquet_files:
                existing_order_items_df = pd.read_parquet(parquet_files['order_items'])
            
            # Get max IDs
            print("  Calculating next IDs...", end='', flush=True)
            if len(existing_customers_df) > 0:
                start_customer_id = existing_customers_df['customer_id'].max() + 1
            if len(existing_products_df) > 0:
                start_product_id = existing_products_df['product_id'].max() + 1
            if len(existing_orders_df) > 0:
                start_order_id = existing_orders_df['order_id'].max() + 1
                max_payment_id = existing_orders_df['payment_id'].max()
                max_tracking_number = existing_orders_df['tracking_number'].max()
                if pd.notna(max_payment_id):
                    start_payment_id = int(max_payment_id) + 1
                if pd.notna(max_tracking_number):
                    start_tracking_number = int(max_tracking_number) + 1
            if len(existing_order_items_df) > 0:
                start_order_item_id = existing_order_items_df['order_item_id'].max() + 1
            
            print(f" ✓ (customers: {len(existing_customers_df):,}, products: {len(existing_products_df):,}, orders: {len(existing_orders_df):,}, order_items: {len(existing_order_items_df):,})")
            print(f" ✓ Next IDs: customer_id={start_customer_id}, product_id={start_product_id}, order_id={start_order_id}, order_item_id={start_order_item_id}")
            
            elapsed = time.time() - start_time
            print(f"  Snapshot export completed in {elapsed:.1f}s")
            print()
        
        # 4. Generate new data
        print("Generating new data...")
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())
        
        gen_start_time = time.time()
        
        # Generate new customers
        print("  Generating customers...", end='', flush=True)
        customer_start = time.time()
        new_customers_df = generate_customers_for_date_range(
            start_datetime, end_datetime, start_customer_id
        )
        customer_elapsed = time.time() - customer_start
        print(f" ✓ ({len(new_customers_df):,} customers in {customer_elapsed:.1f}s)")
        
        # Generate new products
        print("  Generating products...", end='', flush=True)
        product_start = time.time()
        new_products_df = generate_products_for_date_range(
            start_datetime, end_datetime, start_product_id
        )
        product_elapsed = time.time() - product_start
        print(f" ✓ ({len(new_products_df):,} products in {product_elapsed:.1f}s)")
        
        # Combine customers and products for order generation
        print("  Combining existing and new data...", end='', flush=True)
        all_customers_df = pd.concat([existing_customers_df, new_customers_df], ignore_index=True) if len(existing_customers_df) > 0 else new_customers_df
        all_products_df = pd.concat([existing_products_df, new_products_df], ignore_index=True) if len(existing_products_df) > 0 else new_products_df
        print(f" ✓ ({len(all_customers_df):,} total customers, {len(all_products_df):,} total products)")
        
        # Generate new orders
        print("  Generating orders (this may take a while)...", end='', flush=True)
        order_start = time.time()
        if mode == 'initial':
            # For initial run, use generate_orders_for_date_range with full date range
            from config.settings import START_DATE, END_DATE
            initial_start = datetime.combine(START_DATE.date(), datetime.min.time())
            initial_end = datetime.combine(END_DATE.date(), datetime.max.time())
            new_orders_df, new_order_items_df = generate_orders_for_date_range(
                initial_start, initial_end,
                all_customers_df, all_products_df,
                pd.DataFrame(),  # No existing orders
                start_order_id=1,
                start_order_item_id=1,
                start_payment_id=1,
                start_tracking_number=1
            )
        else:
            # For ongoing, use incremental order generation
            # Pass all historical orders for accurate customer segment calculation
            new_orders_df, new_order_items_df = generate_orders_for_date_range(
                start_datetime, end_datetime,
                all_customers_df, all_products_df,
                existing_orders_df,  # Use all existing orders for customer history
                start_order_id, start_order_item_id, start_payment_id, start_tracking_number
            )
        order_elapsed = time.time() - order_start
        print(f" ✓ ({len(new_orders_df):,} orders, {len(new_order_items_df):,} order items in {order_elapsed:.1f}s)")
        
        gen_elapsed = time.time() - gen_start_time
        print(f"  Data generation completed in {gen_elapsed:.1f}s")
        print()
        
        # 5. Update existing orders in Parquet (ongoing mode only)
        updated_orders_df = pd.DataFrame()
        if mode == 'ongoing' and len(existing_orders_df) > 0:
            print("Updating existing orders in Parquet...")
            # Filter orders that need updates (non-final states)
            orders_for_updates_df = existing_orders_df[
                ~existing_orders_df['order_status'].isin(['cancelled', 'refunded'])
            ].copy()
            
            print(f"  Processing {len(orders_for_updates_df):,} orders (non-final states) for status updates...", end='', flush=True)
            update_start = time.time()
            updated_orders_df = update_existing_orders(orders_for_updates_df, end_datetime)
            update_elapsed = time.time() - update_start
            
            # Update existing_orders_df with updated orders
            if len(updated_orders_df) > 0:
                # Normalize datetime columns before update to avoid dtype warnings
                datetime_cols = ['order_date', 'payment_date', 'shipment_date', 'delivered_date', 'created_at', 'updated_at']
                for col in datetime_cols:
                    if col in existing_orders_df.columns:
                        existing_orders_df[col] = pd.to_datetime(existing_orders_df[col], errors='coerce')
                    if col in updated_orders_df.columns:
                        updated_orders_df[col] = pd.to_datetime(updated_orders_df[col], errors='coerce')
                
                # Merge updates back into existing orders
                existing_orders_df = existing_orders_df.set_index('order_id')
                updated_orders_df_idx = updated_orders_df.set_index('order_id')
                existing_orders_df.update(updated_orders_df_idx)
                existing_orders_df = existing_orders_df.reset_index()
                # Reset updated_orders_df index so it still has order_id as a column for later use
                updated_orders_df = updated_orders_df_idx.reset_index()
            
            # Count how many actually changed
            if len(updated_orders_df) > 0:
                # Compare original vs updated status
                orders_for_updates_df_idx = orders_for_updates_df.set_index('order_id')
                updated_orders_df_idx = updated_orders_df.set_index('order_id')
                changed = (orders_for_updates_df_idx['order_status'] != updated_orders_df_idx['order_status']).sum()
                print(f" ✓ ({len(updated_orders_df):,} orders updated, {changed:,} changed status in {update_elapsed:.1f}s)")
            else:
                print(f" ✓ ({len(orders_for_updates_df):,} orders processed in {update_elapsed:.1f}s)")
            print()
        
        # 6. Combine all orders (existing + new + updated)
        print("Combining all orders...", end='', flush=True)
        if mode == 'ongoing':
            # existing_orders_df already has updates merged in from step 5
            all_orders_df = existing_orders_df.copy()
            # Append new orders
            if len(new_orders_df) > 0 and len(all_orders_df) > 0:
                all_orders_df = pd.concat([all_orders_df, new_orders_df], ignore_index=True)
            elif len(new_orders_df) > 0:
                all_orders_df = new_orders_df.copy()
        else:
            all_orders_df = new_orders_df.copy()
        
        # Combine all order items
        if len(existing_order_items_df) > 0 and len(new_order_items_df) > 0:
            all_order_items_df = pd.concat([existing_order_items_df, new_order_items_df], ignore_index=True)
        elif len(new_order_items_df) > 0:
            all_order_items_df = new_order_items_df.copy()
        elif len(existing_order_items_df) > 0:
            all_order_items_df = existing_order_items_df.copy()
        else:
            all_order_items_df = pd.DataFrame()
        
        print(f" ✓ ({len(all_orders_df):,} total orders, {len(all_order_items_df):,} total order items)")
        print()
        
        # 7. Generate payments and shipments
        print("Generating payments and shipments...")
        print("  Note: Payments and shipments are regenerated from orders (new + updated)")
        print("       They are merged/appended to S3 Parquet files, not updated in RDS")
        
        # Only generate for new/updated orders
        if len(new_orders_df) > 0 and len(updated_orders_df) > 0:
            orders_for_payments = pd.concat([new_orders_df, updated_orders_df], ignore_index=True)
        elif len(new_orders_df) > 0:
            orders_for_payments = new_orders_df.copy()
        elif len(updated_orders_df) > 0:
            orders_for_payments = updated_orders_df.copy()
        else:
            orders_for_payments = pd.DataFrame()
        print(f"  Processing {len(orders_for_payments):,} orders for payments/shipments...")
        
        payment_start = time.time()
        payments_df = generate_payments(orders_for_payments)
        payment_elapsed = time.time() - payment_start
        print(f"  Generated {len(payments_df):,} payments in {payment_elapsed:.1f}s")
        
        shipment_start = time.time()
        shipments_df = generate_shipments(orders_for_payments, all_customers_df)
        shipment_elapsed = time.time() - shipment_start
        print(f"  Generated {len(shipments_df):,} shipments in {shipment_elapsed:.1f}s")
        print()
        
        # 8. Update customer segments in Parquet
        print("Updating customer segments in Parquet...")
        print("  Calculating segments based on order history...", end='', flush=True)
        segment_start = time.time()
        # Use all orders for accurate segment calculation
        updated_customers_df = update_customer_segments(all_customers_df, all_orders_df)
        
        segment_elapsed = time.time() - segment_start
        print(f" ✓ ({len(updated_customers_df):,} customers processed in {segment_elapsed:.1f}s)")
        print()
        
        # 9. Save final Parquet files and bulk reload RDS
        print("Saving final Parquet snapshot and bulk reloading RDS...")
        print("  Note: Dropping all tables and recreating with complete data")
        
        rds_start = time.time()
        
        # Normalize datetime columns before saving to Parquet
        print("  Normalizing datetime columns...", end='', flush=True)
        
        # Customers: signup_date, created_at, updated_at, date_of_birth
        datetime_cols_customers = ['signup_date', 'created_at', 'updated_at', 'date_of_birth']
        for col in datetime_cols_customers:
            if col in updated_customers_df.columns:
                updated_customers_df[col] = pd.to_datetime(updated_customers_df[col], errors='coerce')
        
        # Products: created_at
        datetime_cols_products = ['created_at']
        for col in datetime_cols_products:
            if col in all_products_df.columns:
                all_products_df[col] = pd.to_datetime(all_products_df[col], errors='coerce')
        
        # Orders: order_date, payment_date, shipment_date, delivered_date, created_at, updated_at
        datetime_cols_orders = ['order_date', 'payment_date', 'shipment_date', 'delivered_date', 'created_at', 'updated_at']
        for col in datetime_cols_orders:
            if col in all_orders_df.columns:
                all_orders_df[col] = pd.to_datetime(all_orders_df[col], errors='coerce')
        
        # Order items: created_at
        datetime_cols_order_items = ['created_at']
        for col in datetime_cols_order_items:
            if col in all_order_items_df.columns:
                all_order_items_df[col] = pd.to_datetime(all_order_items_df[col], errors='coerce')
        
        print(" ✓")
        
        # Save final Parquet files
        print("  Saving final Parquet files...", end='', flush=True)
        parquet_save_start = time.time()
        
        final_parquet_files = {}
        final_parquet_files['customers'] = temp_dir / 'customers_final.parquet'
        final_parquet_files['products'] = temp_dir / 'products_final.parquet'
        final_parquet_files['orders'] = temp_dir / 'orders_final.parquet'
        final_parquet_files['order_items'] = temp_dir / 'order_items_final.parquet'
        
        updated_customers_df.to_parquet(final_parquet_files['customers'], index=False, engine='pyarrow')
        all_products_df.to_parquet(final_parquet_files['products'], index=False, engine='pyarrow')
        all_orders_df.to_parquet(final_parquet_files['orders'], index=False, engine='pyarrow')
        all_order_items_df.to_parquet(final_parquet_files['order_items'], index=False, engine='pyarrow')
        
        parquet_save_elapsed = time.time() - parquet_save_start
        print(f" ✓ ({parquet_save_elapsed:.1f}s)")
        
        # Bulk reload RDS from Parquet
        print("  Bulk reloading RDS from Parquet snapshot...")
        from rds.snapshot import bulk_reload_from_parquet
        bulk_reload_start = time.time()
        bulk_reload_from_parquet(final_parquet_files, drop_tables=True)
        bulk_reload_elapsed = time.time() - bulk_reload_start
        
        rds_elapsed = time.time() - rds_start
        print(f"  RDS bulk reload completed in {rds_elapsed:.1f}s (Parquet save: {parquet_save_elapsed:.1f}s, reload: {bulk_reload_elapsed:.1f}s)")
        print()
        
        # Cleanup temp directory
        import shutil
        print(f"  Cleaning up temp directory...", end='', flush=True)
        shutil.rmtree(temp_dir)
        print(" ✓")
        print()
        
        # 9. Update S3 Parquet files
        print("Updating S3 Parquet files...")
        print("  Note: Payments and shipments are merged/appended to date-partitioned Parquet files")
        
        s3_start = time.time()
        date_range = (start_date, end_date)
        
        print("  Updating payments Parquet files...", end='', flush=True)
        update_payments_parquet(payments_df, date_range)
        print(" ✓")
        
        print("  Updating shipments Parquet files...", end='', flush=True)
        update_shipments_parquet(shipments_df, date_range)
        print(" ✓")
        
        s3_elapsed = time.time() - s3_start
        print(f"  S3 Parquet updates completed in {s3_elapsed:.1f}s")
        print()
        
        # 10. Write completion log
        print("Writing completion log to S3...", end='', flush=True)
        records_processed = {
            'customers': len(new_customers_df),
            'products': len(new_products_df),
            'orders': len(new_orders_df),
            'order_items': len(new_order_items_df),
            'payments': len(payments_df),
            'shipments': len(shipments_df),
            'orders_updated': len(updated_orders_df)
        }
        
        try:
            log_key = write_run_log(
                end_date,
                'success',
                records_processed
            )
            print(f" ✓ ({log_key})")
        except Exception as e:
            print(f" ⚠ Warning: Could not write log to S3: {e}")
        print()
        
        total_elapsed = time.time() - gen_start_time if 'gen_start_time' in locals() else 0
        print("=" * 70)
        print("✓ Data generation completed successfully!")
        print(f"  Total time: {total_elapsed:.1f}s")
        print("=" * 70)
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        
        # Write error log
        try:
            current_date = date.today()
            write_run_log(
                current_date,
                'failed',
                None,
                str(e)
            )
        except:
            pass
        
        sys.exit(1)


if __name__ == '__main__':
    main()

