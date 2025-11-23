"""
Unified main entry point for data generation (handles both initial and ongoing operations)
"""

import sys
import time
from pathlib import Path
from datetime import datetime, date, timedelta
import pandas as pd
import random
from dotenv import load_dotenv

# Load environment variables from .env file (for local development)
# In ECS, environment variables are set via container definition
# (imports below require environment configuration)

from utils.state import get_last_run_date, write_run_log
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
    BUSINESS_PERIODS,
    CUSTOMERS_PER_DAY,
    PRODUCTS_PER_DAY,
    ORDERS_PER_DAY,
    VIP_SPENDING_THRESHOLD,
    VIP_SUBSCRIPTION_RATE,
    NEW_CUSTOMER_DURATION_DAYS,
)
from utils.logging_utils import (
    log_section_start,
    log_section_complete,
    log_progress,
    log_error,
)


# Load environment variables from .env file (for local development)
# In ECS, environment variables are set via container definition
# (kept separate so config modules can rely on the same env vars later)
env_path = Path(__file__).parent.parent.parent / ".env"
try:
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
except Exception as exc:
    log_error("Environment Loading", f"Failed to load .env: {exc}")


def select_business_period():
    """
    Randomly select a business period configuration for ongoing runs.

    Returns:
        tuple[str, float, str]: Selected period key, multiplier, and display name.
    """
    # Create weighted choices based on probabilities
    period_types = []
    weights = []
    for period_type, config in BUSINESS_PERIODS.items():
        period_types.append(period_type)
        weights.append(config["probability"])

    selected_period = random.choices(period_types, weights=weights, k=1)[0]
    period_info = BUSINESS_PERIODS[selected_period]

    return selected_period, period_info["multiplier"], period_info["name"]


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
            log_progress("Mode Detection", f"Warning: Could not access S3 logs: {e}")
            log_progress("Mode Detection", "Will use RDS order date instead.")

    if not has_rds_data:
        # No RDS data - check if logs exist (for initial run detection)
        if last_run_date is None:
            try:
                last_run_date = get_last_run_date()
            except Exception:
                # S3 unavailable or no logs - this is fine for initial run
                pass

        if last_run_date is None:
            # Initial run mode - will generate full historical dataset from settings.py
            log_progress(
                "Mode Detection",
                "Initial run detected - generating full historical dataset",
            )
            # No START_DATE needed - mode detection is based on data existence only
            return "initial", None

    # Ongoing operations mode (RDS has data)
    if last_run_date is None:
        # No logs but RDS has data - use last order date from RDS
        last_order_date = get_last_order_date()
        if last_order_date:
            start_date = last_order_date + timedelta(days=1)
            log_progress(
                "Mode Detection",
                f"Found existing RDS data. Using last order date: {last_order_date}",
            )
        else:
            # Fallback: start from END_DATE if no orders found
            from config.settings import END_DATE

            start_date = END_DATE.date()
            log_progress(
                "Mode Detection",
                f"Found existing RDS data but no orders. Starting from END_DATE ({start_date}).",
            )
    else:
        # Continue from last run date + 1 day
        start_date = last_run_date + timedelta(days=1)

    return "ongoing", start_date


def update_customer_segments(
    customers_df: pd.DataFrame, orders_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Update customer segments based on complete order history.

    Args:
        customers_df (pd.DataFrame): Customer records that require segment updates.
        orders_df (pd.DataFrame): Order history used to determine segment changes.

    Returns:
        pd.DataFrame: Customer records with refreshed segment information.
    """
    if len(orders_df) == 0:
        return customers_df.copy()

    # Build order history per customer
    customer_order_history = {}
    for _, order in orders_df.iterrows():
        customer_id = order["customer_id"]
        order_date_val = order["order_date"]
        # Handle both string and datetime/timestamp objects
        if isinstance(order_date_val, str):
            order_date = datetime.strptime(order_date_val, "%Y-%m-%d %H:%M:%S")
        elif isinstance(order_date_val, datetime):
            order_date = order_date_val
        elif isinstance(order_date_val, pd.Timestamp):
            order_date = order_date_val.to_pydatetime()
        else:
            order_date = pd.to_datetime(order_date_val).to_pydatetime()
        total_amount = order["total_amount"]

        if customer_id not in customer_order_history:
            customer_order_history[customer_id] = []
        customer_order_history[customer_id].append((order_date, total_amount))

    # Assign VIP subscribers
    all_customer_ids = customers_df["customer_id"].tolist()
    num_vip_subscribers = int(len(all_customer_ids) * VIP_SUBSCRIPTION_RATE)
    vip_subscribers = set(random.sample(all_customer_ids, num_vip_subscribers))

    # Update segments
    updated_customers = []
    for _, customer in customers_df.iterrows():
        customer_id = customer["customer_id"]
        signup_date_val = customer["signup_date"]
        # Handle both string and datetime/timestamp objects
        if isinstance(signup_date_val, str):
            signup_date = datetime.strptime(signup_date_val, "%Y-%m-%d %H:%M:%S")
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
            segment = "VIP"
        else:
            # Use current time for segment calculation (not END_DATE)
            current_time = datetime.now()
            days_since_signup = (current_time - signup_date).days
            num_orders = len(order_history)

            if days_since_signup < NEW_CUSTOMER_DURATION_DAYS or num_orders < 2:
                segment = "New"
            else:
                # Calculate recent spending (last 6 months)
                six_months_ago = current_time - timedelta(days=180)
                recent_spending = sum(
                    amount
                    for order_date, amount in order_history
                    if order_date >= six_months_ago
                )

                if recent_spending > VIP_SPENDING_THRESHOLD:
                    segment = "VIP"
                else:
                    segment = "Regular"

        customer_dict = customer.to_dict()
        old_segment = customer_dict.get("customer_segment")

        # Update if segment changed
        if old_segment != segment:
            customer_dict["customer_segment"] = segment
            # Use current time for audit timestamp (when the record was actually updated)
            customer_dict["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        updated_customers.append(customer_dict)

    return pd.DataFrame(updated_customers)


def main() -> None:
    """
    Run the unified data generation pipeline.
    """
    run_start_time = time.time()

    log_section_start("Unified Data Generation Run")

    try:
        # 1. Detect mode
        log_section_start("Mode Detection")
        mode, start_date = detect_mode()
        log_progress("Mode Detection", f"Mode={mode.upper()}")
        log_progress("Mode Detection", f"Start date={start_date}")
        log_section_complete("Mode Detection")

        # 2. Determine date range
        log_section_start("Date Range Planning")
        if mode == "initial":
            # Initial run: use full historical range from settings
            from config.settings import START_DATE, END_DATE

            start_date = START_DATE.date()
            end_date = END_DATE.date()
        else:
            # Ongoing run: generate data up to yesterday (current time)
            end_date = date.today() - timedelta(days=1)

        if start_date > end_date:
            log_progress(
                "Date Range Planning",
                f"No new data to generate. Start={start_date}, End={end_date}",
            )
            log_section_complete("Date Range Planning", "No new data required")
            log_section_complete("Unified Data Generation Run", "No new data generated")
            sys.exit(0)  # Explicit success exit code for ECS

        log_progress("Date Range Planning", f"Date range: {start_date} to {end_date}")

        # Select business period for ongoing runs (affects activity levels)
        if mode == "ongoing":
            period_type, period_multiplier, period_name = select_business_period()
            log_progress(
                "Date Range Planning",
                f"Business period: {period_name} ({period_multiplier:.1f}x activity)",
            )
        else:
            period_multiplier = 1.0  # Initial runs use baseline rates
            log_progress(
                "Date Range Planning",
                "Business period: Initial baseline (1.0x activity)",
            )
        log_section_complete(
            "Date Range Planning",
            f"Multiplier={period_multiplier:.1f}x over {start_date} to {end_date}",
        )

        # 3. Export RDS to Parquet (snapshot approach)
        log_section_start("Snapshot Preparation")
        import tempfile
        from rds.snapshot import export_rds_to_parquet

        temp_dir = Path(tempfile.mkdtemp())
        log_progress("Snapshot Preparation", f"Snapshot directory: {temp_dir}")

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

        if mode == "ongoing":
            log_section_start("RDS Snapshot Export")
            start_time = time.time()

            parquet_files = export_rds_to_parquet(temp_dir)

            # Load from Parquet files
            log_progress("RDS Snapshot Export", "Loading from Parquet snapshot...")
            if "customers" in parquet_files:
                existing_customers_df = pd.read_parquet(parquet_files["customers"])
            if "products" in parquet_files:
                existing_products_df = pd.read_parquet(parquet_files["products"])
            if "orders" in parquet_files:
                existing_orders_df = pd.read_parquet(parquet_files["orders"])
            if "order_items" in parquet_files:
                existing_order_items_df = pd.read_parquet(parquet_files["order_items"])

            # Get max IDs
            log_progress("RDS Snapshot Export", "Calculating next IDs...")
            if len(existing_customers_df) > 0:
                start_customer_id = existing_customers_df["customer_id"].max() + 1
            if len(existing_products_df) > 0:
                start_product_id = existing_products_df["product_id"].max() + 1
            if len(existing_orders_df) > 0:
                start_order_id = existing_orders_df["order_id"].max() + 1
                max_payment_id = existing_orders_df["payment_id"].max()
                max_tracking_number = existing_orders_df["tracking_number"].max()
                if pd.notna(max_payment_id):
                    start_payment_id = int(max_payment_id) + 1
                if pd.notna(max_tracking_number):
                    start_tracking_number = int(max_tracking_number) + 1
            if len(existing_order_items_df) > 0:
                start_order_item_id = existing_order_items_df["order_item_id"].max() + 1

            elapsed = time.time() - start_time
            log_section_complete(
                "RDS Snapshot Export",
                (
                    f"Loaded customers={len(existing_customers_df):,}, products={len(existing_products_df):,}, "
                    f"orders={len(existing_orders_df):,}, order_items={len(existing_order_items_df):,} | "
                    f"Next IDs: customer={start_customer_id}, product={start_product_id}, "
                    f"order={start_order_id}, order_item={start_order_item_id} | Duration={elapsed:.1f}s"
                ),
            )
        else:
            log_progress("RDS Snapshot Export", "Skipped for initial run")

        log_section_complete("Snapshot Preparation")

        # 4. Generate new data
        log_section_start("Data Generation")
        start_datetime = datetime.combine(start_date, datetime.min.time())
        end_datetime = datetime.combine(end_date, datetime.max.time())

        gen_start_time = time.time()

        # Generate new customers
        log_section_start("Customer Generation")
        customer_start = time.time()
        # Apply business period multiplier to customer acquisition rate
        adjusted_customers_per_day = int(CUSTOMERS_PER_DAY * period_multiplier)
        new_customers_df = generate_customers_for_date_range(
            start_datetime, end_datetime, start_customer_id, adjusted_customers_per_day
        )
        customer_elapsed = time.time() - customer_start
        log_section_complete(
            "Customer Generation",
            f"{len(new_customers_df):,} customers in {customer_elapsed:.1f}s",
        )

        # Generate new products
        log_section_start("Product Generation")
        product_start = time.time()
        # Apply business period multiplier to product creation rate
        adjusted_products_per_day = int(PRODUCTS_PER_DAY * period_multiplier)
        new_products_df = generate_products_for_date_range(
            start_datetime, end_datetime, start_product_id, adjusted_products_per_day
        )
        product_elapsed = time.time() - product_start
        log_section_complete(
            "Product Generation",
            f"{len(new_products_df):,} products in {product_elapsed:.1f}s",
        )

        # Combine customers and products for order generation
        log_progress(
            "Data Generation", "Combining existing and new customer/product data"
        )
        all_customers_df = (
            pd.concat([existing_customers_df, new_customers_df], ignore_index=True)
            if len(existing_customers_df) > 0
            else new_customers_df
        )
        all_products_df = (
            pd.concat([existing_products_df, new_products_df], ignore_index=True)
            if len(existing_products_df) > 0
            else new_products_df
        )
        log_progress(
            "Data Generation",
            f"Totals: {len(all_customers_df):,} customers, {len(all_products_df):,} products",
        )

        # Generate new orders
        log_section_start("Order Generation")
        order_start = time.time()
        log_progress("Order Generation", f"Date range: {start_date} to {end_date}")
        if mode == "initial":
            # Initial run: no existing orders to consider
            existing_orders_for_generation = pd.DataFrame()
        else:
            # Ongoing run: use all existing orders for customer segment calculation
            existing_orders_for_generation = existing_orders_df

        # Apply business period multiplier to order rate
        adjusted_orders_per_day = int(ORDERS_PER_DAY * period_multiplier)
        new_orders_df, new_order_items_df = generate_orders_for_date_range(
            start_datetime,
            end_datetime,
            all_customers_df,
            all_products_df,
            existing_orders_for_generation,
            start_order_id,
            start_order_item_id,
            start_payment_id,
            start_tracking_number,
            adjusted_orders_per_day,
        )
        order_elapsed = time.time() - order_start
        log_section_complete(
            "Order Generation",
            f"{len(new_orders_df):,} orders and {len(new_order_items_df):,} items in {order_elapsed:.1f}s",
        )

        gen_elapsed = time.time() - gen_start_time
        log_section_complete("Data Generation", f"Duration={gen_elapsed:.1f}s")

        # 5. Update existing orders in Parquet (ongoing mode only)
        updated_orders_df = pd.DataFrame()
        if mode == "ongoing" and len(existing_orders_df) > 0:
            log_section_start("Existing Order Updates")
            # Filter orders that need updates (non-final states)
            orders_for_updates_df = existing_orders_df[
                ~existing_orders_df["order_status"].isin(["cancelled", "refunded"])
            ].copy()

            log_progress(
                "Existing Order Updates",
                f"Processing {len(orders_for_updates_df):,} non-final orders",
            )
            update_start = time.time()
            updated_orders_df = update_existing_orders(
                orders_for_updates_df, end_datetime
            )
            update_elapsed = time.time() - update_start

            # Update existing_orders_df with updated orders
            if len(updated_orders_df) > 0:
                # Normalize datetime columns before update to avoid dtype warnings
                datetime_cols = [
                    "order_date",
                    "payment_date",
                    "shipment_date",
                    "delivered_date",
                    "created_at",
                    "updated_at",
                ]
                for col in datetime_cols:
                    if col in existing_orders_df.columns:
                        existing_orders_df[col] = pd.to_datetime(
                            existing_orders_df[col], errors="coerce"
                        )
                    if col in updated_orders_df.columns:
                        updated_orders_df[col] = pd.to_datetime(
                            updated_orders_df[col], errors="coerce"
                        )

                # Merge updates back into existing orders
                existing_orders_df = existing_orders_df.set_index("order_id")
                updated_orders_df_idx = updated_orders_df.set_index("order_id")
                existing_orders_df.update(updated_orders_df_idx)
                existing_orders_df = existing_orders_df.reset_index()
                # Reset updated_orders_df index so it still has order_id as a column for later use
                updated_orders_df = updated_orders_df_idx.reset_index()

            # Count how many actually changed
            if len(updated_orders_df) > 0:
                # Compare original vs updated status
                orders_for_updates_df_idx = orders_for_updates_df.set_index("order_id")
                updated_orders_df_idx = updated_orders_df.set_index("order_id")
                changed = (
                    orders_for_updates_df_idx["order_status"]
                    != updated_orders_df_idx["order_status"]
                ).sum()
                log_section_complete(
                    "Existing Order Updates",
                    f"{len(updated_orders_df):,} orders updated, {changed:,} status changes in {update_elapsed:.1f}s",
                )
            else:
                log_section_complete(
                    "Existing Order Updates",
                    f"{len(orders_for_updates_df):,} orders processed with no changes in {update_elapsed:.1f}s",
                )
        else:
            log_progress(
                "Existing Order Updates", "No existing orders required updates"
            )

        # 6. Combine all orders (existing + new + updated)
        log_section_start("Order Consolidation")
        if mode == "ongoing":
            # existing_orders_df already has updates merged in from step 5
            all_orders_df = existing_orders_df.copy()
            # Append new orders
            if len(new_orders_df) > 0 and len(all_orders_df) > 0:
                all_orders_df = pd.concat(
                    [all_orders_df, new_orders_df], ignore_index=True
                )
            elif len(new_orders_df) > 0:
                all_orders_df = new_orders_df.copy()
        else:
            all_orders_df = new_orders_df.copy()

        # Combine all order items
        if len(existing_order_items_df) > 0 and len(new_order_items_df) > 0:
            all_order_items_df = pd.concat(
                [existing_order_items_df, new_order_items_df], ignore_index=True
            )
        elif len(new_order_items_df) > 0:
            all_order_items_df = new_order_items_df.copy()
        elif len(existing_order_items_df) > 0:
            all_order_items_df = existing_order_items_df.copy()
        else:
            all_order_items_df = pd.DataFrame()

        log_section_complete(
            "Order Consolidation",
            f"{len(all_orders_df):,} total orders and {len(all_order_items_df):,} items ready",
        )

        # 7. Generate payments and shipments
        log_section_start("Payments and Shipments Generation")
        log_progress(
            "Payments and Shipments Generation",
            "Regenerating records from new and updated orders; S3 Parquet files will be merged/appended",
        )

        # Only generate for new/updated orders
        if len(new_orders_df) > 0 and len(updated_orders_df) > 0:
            orders_for_payments = pd.concat(
                [new_orders_df, updated_orders_df], ignore_index=True
            )
        elif len(new_orders_df) > 0:
            orders_for_payments = new_orders_df.copy()
        elif len(updated_orders_df) > 0:
            orders_for_payments = updated_orders_df.copy()
        else:
            orders_for_payments = pd.DataFrame()
        log_progress(
            "Payments and Shipments Generation",
            f"Processing {len(orders_for_payments):,} orders",
        )

        payment_start = time.time()
        payments_df = generate_payments(orders_for_payments)
        payment_elapsed = time.time() - payment_start
        log_progress(
            "Payments and Shipments Generation",
            f"Generated {len(payments_df):,} payments in {payment_elapsed:.1f}s",
        )

        shipment_start = time.time()
        shipments_df = generate_shipments(orders_for_payments, all_customers_df)
        shipment_elapsed = time.time() - shipment_start
        log_section_complete(
            "Payments and Shipments Generation",
            (
                f"Generated {len(shipments_df):,} shipments in {shipment_elapsed:.1f}s "
                f"after creating {len(payments_df):,} payments"
            ),
        )

        # 8. Update customer segments in Parquet
        log_section_start("Customer Segment Update")
        log_progress(
            "Customer Segment Update", "Calculating segments based on order history"
        )
        segment_start = time.time()
        # Use all orders for accurate segment calculation
        updated_customers_df = update_customer_segments(all_customers_df, all_orders_df)

        segment_elapsed = time.time() - segment_start
        log_section_complete(
            "Customer Segment Update",
            f"{len(updated_customers_df):,} customers processed in {segment_elapsed:.1f}s",
        )

        # 9. Save final Parquet files and bulk reload RDS
        log_section_start("Parquet Snapshot and RDS Reload")
        log_progress(
            "Parquet Snapshot and RDS Reload",
            "Dropping and recreating RDS tables from fresh snapshot",
        )

        rds_start = time.time()

        # Normalize datetime columns before saving to Parquet
        log_section_start("Parquet Normalization")

        # Customers: signup_date, created_at, updated_at, date_of_birth
        datetime_cols_customers = [
            "signup_date",
            "created_at",
            "updated_at",
            "date_of_birth",
        ]
        for col in datetime_cols_customers:
            if col in updated_customers_df.columns:
                updated_customers_df[col] = pd.to_datetime(
                    updated_customers_df[col], errors="coerce"
                )

        # Products: created_at
        datetime_cols_products = ["created_at"]
        for col in datetime_cols_products:
            if col in all_products_df.columns:
                all_products_df[col] = pd.to_datetime(
                    all_products_df[col], errors="coerce"
                )

        # Orders: order_date, payment_date, shipment_date, delivered_date, created_at, updated_at
        datetime_cols_orders = [
            "order_date",
            "payment_date",
            "shipment_date",
            "delivered_date",
            "created_at",
            "updated_at",
        ]
        for col in datetime_cols_orders:
            if col in all_orders_df.columns:
                all_orders_df[col] = pd.to_datetime(all_orders_df[col], errors="coerce")

        # Order items: created_at
        datetime_cols_order_items = ["created_at"]
        for col in datetime_cols_order_items:
            if col in all_order_items_df.columns:
                all_order_items_df[col] = pd.to_datetime(
                    all_order_items_df[col], errors="coerce"
                )
        log_section_complete("Parquet Normalization")

        # Save final Parquet files
        log_section_start("Parquet Snapshot Save")
        parquet_save_start = time.time()

        final_parquet_files = {}
        final_parquet_files["customers"] = temp_dir / "customers_final.parquet"
        final_parquet_files["products"] = temp_dir / "products_final.parquet"
        final_parquet_files["orders"] = temp_dir / "orders_final.parquet"
        final_parquet_files["order_items"] = temp_dir / "order_items_final.parquet"

        updated_customers_df.to_parquet(
            final_parquet_files["customers"], index=False, engine="pyarrow"
        )
        all_products_df.to_parquet(
            final_parquet_files["products"], index=False, engine="pyarrow"
        )
        all_orders_df.to_parquet(
            final_parquet_files["orders"], index=False, engine="pyarrow"
        )
        all_order_items_df.to_parquet(
            final_parquet_files["order_items"], index=False, engine="pyarrow"
        )

        parquet_save_elapsed = time.time() - parquet_save_start
        log_section_complete(
            "Parquet Snapshot Save", f"Duration={parquet_save_elapsed:.1f}s"
        )

        # Bulk reload RDS from Parquet
        log_section_start("RDS Bulk Reload")
        from rds.snapshot import bulk_reload_from_parquet

        bulk_reload_start = time.time()
        bulk_reload_from_parquet(final_parquet_files, drop_tables=True)
        bulk_reload_elapsed = time.time() - bulk_reload_start

        rds_elapsed = time.time() - rds_start
        log_section_complete(
            "RDS Bulk Reload",
            (
                f"Duration={rds_elapsed:.1f}s (Parquet save: {parquet_save_elapsed:.1f}s, "
                f"reload: {bulk_reload_elapsed:.1f}s)"
            ),
        )

        # Cleanup temp directory
        import shutil

        log_section_start("Temporary Directory Cleanup")
        shutil.rmtree(temp_dir)
        log_section_complete("Temporary Directory Cleanup", f"Removed {temp_dir}")
        log_section_complete(
            "Parquet Snapshot and RDS Reload", f"Total duration={rds_elapsed:.1f}s"
        )

        # 9. Update S3 Parquet files
        log_section_start("S3 Parquet Updates")
        log_progress(
            "S3 Parquet Updates",
            "Merging payments and shipments into date-partitioned Parquet files",
        )

        s3_start = time.time()
        date_range = (start_date, end_date)

        log_progress("S3 Parquet Updates", "Updating payments Parquet files")
        update_payments_parquet(payments_df, date_range)

        log_progress("S3 Parquet Updates", "Updating shipments Parquet files")
        update_shipments_parquet(shipments_df, date_range)

        s3_elapsed = time.time() - s3_start
        log_section_complete("S3 Parquet Updates", f"Duration={s3_elapsed:.1f}s")

        # 10. Write completion log
        log_section_start("Completion Log")
        records_processed = {
            "customers": len(new_customers_df),
            "products": len(new_products_df),
            "orders": len(new_orders_df),
            "order_items": len(new_order_items_df),
            "payments": len(payments_df),
            "shipments": len(shipments_df),
            "orders_updated": len(updated_orders_df),
        }

        try:
            log_key = write_run_log(end_date, "success", records_processed)
            log_section_complete("Completion Log", f"Written to {log_key}")
        except Exception as e:
            log_error("Completion Log", f"Could not write log to S3: {e}")

        total_elapsed = time.time() - run_start_time
        log_section_complete(
            "Unified Data Generation Run", f"Total time={total_elapsed:.1f}s"
        )
        sys.exit(0)  # Explicit success exit code for ECS
    except Exception as e:
        log_error("Unified Data Generation Run", e)
        import traceback

        log_progress("Unified Data Generation Run", traceback.format_exc())

        # Write error log
        try:
            current_date = date.today()
            write_run_log(current_date, "failed", None, str(e))
        except Exception:
            pass

        sys.exit(1)


if __name__ == "__main__":
    main()
