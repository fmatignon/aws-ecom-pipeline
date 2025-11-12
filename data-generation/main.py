"""
Master data generator - orchestrates all generator calls in proper sequence
"""
import sys
import os
from pathlib import Path

# Add the data-generation directory to Python path when running as script
# This allows running from parent directory: py data-generation/main.py
script_dir = Path(__file__).parent.absolute()
if str(script_dir) not in sys.path:
    sys.path.insert(0, str(script_dir))

from generators.customers import generate_customers
from generators.products import generate_products
from generators.orders import generate_orders
from generators.payments import generate_payments
from generators.shipments import generate_shipments
from config.settings import (
    NUM_CUSTOMERS, NUM_PRODUCTS, NUM_ORDERS
)
from datetime import datetime


def generate_all_data(
    num_customers=NUM_CUSTOMERS,
    num_products=NUM_PRODUCTS,
    num_orders=NUM_ORDERS,
    output_dir='output',
    output_format='csv',
    save_data=True
):
    """
    Generate complete e-commerce dataset
    
    Args:
        num_customers: Number of customers to generate
        num_products: Number of products to generate
        num_orders: Number of orders to generate
        output_dir: Directory to save output files
        output_format: 'csv', 'json', or 'parquet'
        save_data: Whether to save data to files
        
    Returns:
        dict: Dictionary containing all generated DataFrames
    """
    print("=" * 70)
    print("E-COMMERCE DATA GENERATOR")
    print("=" * 70)
    print(f"Generating: {num_customers:,} customers | {num_products:,} products | {num_orders:,} orders")
    print("=" * 70)
    
    start_time = datetime.now()
    
    # Generate Customers
    step_start = datetime.now()
    customers_df = generate_customers(num_customers)
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"[1/5] Customers:   {len(customers_df):,} records ({step_time:.1f}s)")
    
    # Generate Products
    step_start = datetime.now()
    products_df = generate_products(num_products)
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"[2/5] Products:    {len(products_df):,} records ({step_time:.1f}s)")
    
    # Generate Orders
    step_start = datetime.now()
    orders_df, order_items_df, customers_df = generate_orders(
        num_orders, customers_df, products_df
    )
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"[3/5] Orders:      {len(orders_df):,} records ({step_time:.1f}s)")
    print(f"      Order Items: {len(order_items_df):,} records")
    
    # Generate Payments
    step_start = datetime.now()
    payments_df = generate_payments(orders_df)
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"[4/5] Payments:    {len(payments_df):,} records ({step_time:.1f}s)")
    
    # Generate Shipments
    step_start = datetime.now()
    shipments_df = generate_shipments(orders_df, order_items_df, customers_df)
    step_time = (datetime.now() - step_start).total_seconds()
    print(f"[5/5] Shipments:   {len(shipments_df):,} records ({step_time:.1f}s)")
    print()
    
    # ============================================================
    # Collect all data
    # ============================================================
    data = {
        'customers': customers_df,
        'products': products_df,
        'orders': orders_df,
        'order_items': order_items_df,
        'payments': payments_df,
        'shipments': shipments_df
    }
    
    # Save to disk
    if save_data:
        print("=" * 70)
        os.makedirs(output_dir, exist_ok=True)
        
        total_size = 0
        for table_name, df in data.items():
            file_path = os.path.join(output_dir, f"{table_name}.{output_format}")
            
            if output_format == 'csv':
                df.to_csv(file_path, index=False)
            elif output_format == 'json':
                df.to_json(file_path, orient='records', lines=True)
            elif output_format == 'parquet':
                df.to_parquet(file_path, index=False)
            else:
                raise ValueError(f"Unsupported output format: {output_format}")
            
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
            total_size += file_size
            print(f"Exported: {table_name:12} → {file_size:6.1f} MB")
        
        print(f"{'':12}Total      → {total_size:6.1f} MB")
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("=" * 70)
    print(f"✓ Completed in {duration:.1f}s ({duration/60:.1f}m)")
    print("=" * 70)
    
    return data


def generate_sample_data(output_dir='output_sample'):
    """
    Generate a small sample dataset for testing (1000 customers with proportional products/orders)
    """
    print("Generating SAMPLE dataset for testing...")
    print()
    num_customers = 1000
    return generate_all_data(
        num_customers=num_customers,
        num_products=int(num_customers / 20),  # Same proportion as config
        num_orders=int(num_customers * 3.75),  # Same proportion as config
        output_dir=output_dir,
        output_format='csv',
        save_data=True
    )


def generate_full_data(output_dir='output'):
    """
    Generate the full production dataset
    """
    print("Generating FULL production dataset...")
    print()
    return generate_all_data(
        output_dir=output_dir,
        output_format='csv',
        save_data=True
    )


if __name__ == '__main__':
    import sys
    
    # Check command line arguments
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        
        if mode == 'sample':
            # Generate sample data
            data = generate_sample_data()
        elif mode == 'full':
            # Generate full production data
            data = generate_full_data()
        elif mode == 'test':
            # Quick test with minimal data (no saving)
            num_customers = 100
            num_products = int(num_customers / 20)  # Same proportion as config
            num_orders = int(num_customers * 3.75)  # Same proportion as config
            print(f"Running QUICK TEST ({num_customers} customers, {num_products} products, {num_orders} orders)...")
            print()
            data = generate_all_data(
                num_customers=num_customers,
                num_products=num_products,
                num_orders=num_orders,
                output_dir='output_test',
                save_data=False
            )
            
            # Show some samples
            print("\nSample data preview:")
            print("\nCustomers (first 5):")
            print(data['customers'].head())
            print("\nProducts (first 5):")
            print(data['products'].head())
            print("\nOrders (first 5):")
            print(data['orders'].head())
        else:
            print(f"Unknown mode: {mode}")
            print("Usage: python main.py [sample|full|test]")
            sys.exit(1)
    else:
        # Default: Generate sample data
        print("No mode specified. Use: python main.py [sample|full|test]")
        print("Defaulting to 'sample' mode...")
        print()
        data = generate_sample_data()