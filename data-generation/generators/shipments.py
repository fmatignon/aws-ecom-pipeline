"""
Generates realistic shipment data from orders
"""

from seed_data.config.settings import CARRIERS, AVG_DELIVERY_TIME
from datetime import datetime, timedelta
import pandas as pd
import random
import hashlib


# Warehouse locations by region (for origin country only)
WAREHOUSE_LOCATIONS = {
    'United States': 'United States',
    'Canada': 'Canada',
    'United Kingdom': 'United Kingdom',
    'Germany': 'Germany',
    'France': 'France',
    'Italy': 'Italy',
    'Spain': 'Spain',
    'Japan': 'Japan',
    'Netherlands': 'Netherlands',
    'Australia': 'Australia'
}


def _generate_tracking_number(carrier, tracking_number_id):
    """
    Generate a realistic tracking number based on carrier format
    """
    # Create a hash-based tracking number
    key = f"{carrier}|{tracking_number_id}"
    hash_value = hashlib.md5(key.encode()).hexdigest()[:12].upper()
    
    # Format based on carrier
    if carrier == 'UPS':
        return f"1Z{hash_value[:6]}{hash_value[6:]}"
    elif carrier == 'FedEx':
        return f"{hash_value[:4]} {hash_value[4:8]} {hash_value[8:]}"
    elif carrier == 'USPS':
        return f"{hash_value[:4]} {hash_value[4:8]} {hash_value[8:]}"
    elif carrier == 'DHL':
        return f"{hash_value[:4]}{hash_value[4:8]}{hash_value[8:]}"
    else:
        return hash_value


def _calculate_shipment_status(shipment_date, delivered_date, current_date):
    """
    Determine shipment status based on dates
    Exception status represents delivery issues (lost packages, wrong address, damaged goods, etc.)
    """
    if delivered_date:
        # Delivered shipments are always 'delivered' (even if late)
        return 'delivered'
    elif shipment_date:
        # For in-transit shipments, check for exceptions
        estimated_delivery = shipment_date + timedelta(days=AVG_DELIVERY_TIME)
        days_overdue = (current_date - estimated_delivery).days
        
        # Base exception rate for in-transit shipments (represents various issues)
        # This includes: lost packages, wrong address, damaged goods, carrier issues, etc.
        base_exception_rate = 0.03  # 3% base rate for any in-transit shipment
        
        if days_overdue > 2:
            # Overdue shipments have higher exception rates
            if days_overdue > 7:
                # Very overdue (>7 days) - 50% chance of exception
                exception_rate = 0.50
            elif days_overdue > 4:
                # Moderately overdue (4-7 days) - 30% chance of exception
                exception_rate = 0.30
            else:
                # Slightly overdue (2-4 days) - 15% chance of exception
                exception_rate = 0.15
        else:
            # Not yet overdue, but still some chance of exception (lost package, wrong address, etc.)
            exception_rate = base_exception_rate
        
        if random.random() < exception_rate:
            return 'exception'
        
        return 'in_transit'
    else:
        return 'pending'


def generate_shipments(orders_df, order_items_df=None, customers_df=None):
    """
    Generate shipment records from orders data
    
    Args:
        orders_df: DataFrame with order data (must have tracking_number, shipping_carrier,
                   shipment_date, delivered_date, shipping_cost, order_id, customer_id)
        order_items_df: Optional DataFrame with order items (not currently used)
        customers_df: Optional DataFrame with customer data (for destination postal code)
    
    Returns:
        shipments_df: DataFrame with shipment records
    """
    if orders_df is None or len(orders_df) == 0:
        raise ValueError("orders_df is required and must not be empty")
    
    # Filter orders that have tracking_number (shipped, delivered, or refunded)
    orders_with_shipment = orders_df[orders_df['tracking_number'].notna()].copy()
    
    if len(orders_with_shipment) == 0:
        return pd.DataFrame()
    
    # Create lookup for customer postal codes if provided
    customer_data = {}
    if customers_df is not None:
        for _, customer in customers_df.iterrows():
            customer_data[customer['customer_id']] = {
                'postal_code': customer.get('postal_code', ''),
                'country': customer.get('country', '')
            }
    
    shipments = []
    current_date = datetime.now()
    
    for idx, (_, order) in enumerate(orders_with_shipment.iterrows(), 1):
        tracking_number_id = int(order['tracking_number'])
        order_id = order['order_id']
        customer_id = order['customer_id']
        shipping_carrier = order['shipping_carrier']
        shipping_cost = order['shipping_cost']
        shipment_date_str = order.get('shipment_date')
        delivered_date_str = order.get('delivered_date')
        
        # Parse dates
        shipment_date = None
        delivered_date = None
        if shipment_date_str:
            shipment_date = datetime.strptime(shipment_date_str, '%Y-%m-%d %H:%M:%S')
        if delivered_date_str:
            delivered_date = datetime.strptime(delivered_date_str, '%Y-%m-%d %H:%M:%S')
        
        # Generate tracking number
        tracking_number = _generate_tracking_number(shipping_carrier, tracking_number_id)
        
        # Get origin country (warehouse) based on customer country
        customer_country = None
        destination_postal_code = None
        if customer_id in customer_data:
            customer_country = customer_data[customer_id]['country']
            destination_postal_code = customer_data[customer_id]['postal_code']
        else:
            # Fallback: use US warehouse
            customer_country = 'United States'
            destination_postal_code = '00000'
        
        origin_country = WAREHOUSE_LOCATIONS.get(customer_country, 'United States')
        destination_country = customer_country or 'United States'
        
        # Calculate estimated delivery date
        estimated_delivery_date = None
        if shipment_date:
            estimated_delivery_date = shipment_date + timedelta(
                days=AVG_DELIVERY_TIME + random.randint(-1, 2)
            )
        
        # Determine shipment status
        shipment_status = _calculate_shipment_status(shipment_date, delivered_date, current_date)
        
        shipment = {
            'tracking_number': tracking_number_id,
            'tracking_code': tracking_number,  # Human-readable tracking number
            'order_id': order_id,
            'shipping_carrier': shipping_carrier,
            'origin_country': origin_country,
            'destination_country': destination_country,
            'destination_postal_code': destination_postal_code,
            'status': shipment_status,
            'shipping_cost': shipping_cost,
            'shipment_date': shipment_date_str,
            'estimated_delivery_date': estimated_delivery_date.strftime('%Y-%m-%d %H:%M:%S') if estimated_delivery_date else None,
            'actual_delivery_date': delivered_date_str,
            'created_at': shipment_date_str if shipment_date_str else None,
        }
        
        shipments.append(shipment)
        
        # Update progress every 1%
        total = len(orders_with_shipment)
        pct = int((idx / total) * 100)
        prev_pct = int(((idx - 1) / total) * 100) if idx > 1 else -1
        
        if pct != prev_pct or idx == total:
            bar_width = 50
            filled = int(bar_width * pct / 100)
            bar = '|' * filled + ' ' * (bar_width - filled)
            print(f"\r      Shipments: {pct:3d}% |{bar}|", end='', flush=True)
    
    # Clear progress line using ANSI escape code
    print("\r\033[K", end='', flush=True)
    return pd.DataFrame(shipments)
