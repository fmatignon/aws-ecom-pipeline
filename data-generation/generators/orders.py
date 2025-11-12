"""
Generates realistic order data with business logic from settings.py
"""

from config.settings import (
    NUM_ORDERS, END_DATE, ORDER_SUCCESS_RATE,
    REFUND_RATE, CARRIERS, PAYMENT_METHODS, COUNTRIES, CUSTOMER_SEGMENTS,
    PAYMENT_STATUSES, DISCOUNT_USAGE_RATE,
    CATEGORY_PREFERENCE_RATE, VIP_SPENDING_THRESHOLD, VIP_SUBSCRIPTION_RATE,
    NEW_CUSTOMER_DURATION_DAYS, ITEMS_PER_ORDER_DISTRIBUTION,
    QUANTITY_PER_PRODUCT_MIN, QUANTITY_PER_PRODUCT_MAX,
    ORDER_DATE_DISTRIBUTION_PEAK, ORDER_STATUS_TRANSITION_WEIGHTS,
    AVG_PROCESSING_TIME, AVG_SHIPPING_TIME, AVG_DELIVERY_TIME
)
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import random
from collections import defaultdict


def _determine_segment_at_order_time(customer_id, order_date, signup_date, customer_order_history, vip_subscribers):
    """
    Determine customer segment at the time of a specific order
    
    Args:
        customer_id: Customer ID
        order_date: Date of current order
        signup_date: Customer signup date
        customer_order_history: List of previous orders for this customer [(date, total_amount), ...]
        vip_subscribers: Set of customer IDs with voluntary VIP subscription
    
    Returns:
        segment: 'New', 'Regular', or 'VIP'
    """
    # Check VIP subscription (permanent status)
    if customer_id in vip_subscribers:
        return 'VIP'
    
    # Calculate days since signup
    days_since_signup = (order_date - signup_date).days
    
    # Count previous orders (not including current one)
    num_previous_orders = len(customer_order_history)
    
    # New customer: < 60 days since signup OR < 2 orders
    if days_since_signup < NEW_CUSTOMER_DURATION_DAYS or num_previous_orders < 2:
        return 'New'
    
    # Calculate spending in last 6 months (from previous orders only)
    six_months_ago = order_date - timedelta(days=180)
    recent_spending = sum(
        amount for date, amount in customer_order_history
        if date >= six_months_ago
    )
    
    # VIP: last 6 months spending > threshold
    if recent_spending > VIP_SPENDING_THRESHOLD:
        return 'VIP'
    
    # Default: Regular
    return 'Regular'


def _calculate_shipping_cost(subtotal, segment, order_number, country):
    """
    Calculate shipping cost based on segment and order number
    """
    # Determine free shipping threshold based on segment
    if segment == 'New':
        if order_number == 1:
            threshold = CUSTOMER_SEGMENTS['New']['free_shipping_threshold']['first_order']
        elif order_number == 2:
            threshold = CUSTOMER_SEGMENTS['New']['free_shipping_threshold']['second_order']
        else:
            # Shouldn't happen for New customers, but fallback to Regular
            threshold = CUSTOMER_SEGMENTS['Regular']['free_shipping_threshold']
    elif segment == 'VIP':
        threshold = CUSTOMER_SEGMENTS['VIP']['free_shipping_threshold']
    else:  # Regular
        threshold = CUSTOMER_SEGMENTS['Regular']['free_shipping_threshold']
    
    # Free shipping if subtotal meets threshold
    if subtotal >= threshold:
        return 0.0
    
    # Base shipping cost varies by country
    cost_range = COUNTRIES[country]['shipping_cost_range']
    base_cost = random.uniform(cost_range[0], cost_range[1])
    
    return round(base_cost, 2)


def _calculate_discount(subtotal, segment, discount_seed):
    """
    Calculate discount amount based on customer segment
    Uses discount_seed for consistent randomness
    """
    discount_rate = CUSTOMER_SEGMENTS[segment]['discount_rate']
    
    # Use discount_seed to determine if discount is used (reproducible)
    random.seed(discount_seed)
    use_discount = random.random() < DISCOUNT_USAGE_RATE
    random.seed()  # Reset seed
    
    if discount_rate > 0 and use_discount:
        return round(subtotal * discount_rate, 2)
    return 0.0


def _calculate_tax(subtotal, discount_amount, country):
    """
    Calculate tax on subtotal after discount
    """
    tax_rate = COUNTRIES[country]['tax_rate']
    taxable_amount = subtotal - discount_amount
    return round(taxable_amount * tax_rate, 2)


def _determine_order_status(order_date, payment_status):
    """
    Determine order status based on payment status and timing
    """
    # If payment failed, order is cancelled
    if payment_status == 'failed':
        return 'cancelled'
    
    # If payment pending, order is pending
    if payment_status == 'pending':
        return 'pending'
    
    # For completed payments, determine status based on time since order
    days_since_order = (END_DATE - order_date).days
    
    # Adjust based on timing - older orders more likely to be delivered
    if days_since_order > AVG_PROCESSING_TIME + AVG_SHIPPING_TIME + AVG_DELIVERY_TIME:
        # Very old orders should be delivered (with small chance of refund)
        if random.random() < ORDER_SUCCESS_RATE:
            return 'delivered'
        else:
            return 'refunded'
    elif days_since_order > AVG_PROCESSING_TIME + AVG_SHIPPING_TIME:
        # Medium old orders likely shipped or delivered
        weights = ORDER_STATUS_TRANSITION_WEIGHTS['shipped_delivered']
        return random.choices(['shipped', 'delivered'], weights=weights)[0]
    elif days_since_order > AVG_PROCESSING_TIME:
        # Recent orders likely processing or shipped
        weights = ORDER_STATUS_TRANSITION_WEIGHTS['processing_shipped']
        return random.choices(['processing', 'shipped'], weights=weights)[0]
    else:
        # Very recent orders likely processing
        return 'processing'


def _determine_payment_status():
    """
    Determine payment status based on PAYMENT_STATUSES
    """
    statuses = list(PAYMENT_STATUSES.keys())
    weights = list(PAYMENT_STATUSES.values())
    return random.choices(statuses, weights=weights)[0]


def _calculate_timestamps(order_date, order_status, payment_status):
    """
    Calculate payment_date, shipment_date, delivered_date based on order status
    """
    payment_date = None
    shipment_date = None
    delivered_date = None
    
    # Payment date - usually same day or next day if payment completed
    if payment_status == 'completed':
        payment_date = order_date + timedelta(
            hours=random.randint(0, 24),
            minutes=random.randint(0, 59)
        )
    elif payment_status == 'failed':
        payment_date = order_date + timedelta(
            hours=random.randint(1, 6),
            minutes=random.randint(0, 59)
        )
    
    # Shipment date - only if order is shipped, delivered, or refunded
    if order_status in ['shipped', 'delivered', 'refunded']:
        shipment_date = order_date + timedelta(
            days=random.randint(AVG_PROCESSING_TIME, AVG_PROCESSING_TIME + AVG_SHIPPING_TIME),
            hours=random.randint(0, 23)
        )
    
    # Delivered date - only if order is delivered or refunded
    if order_status in ['delivered', 'refunded']:
        if shipment_date:
            delivered_date = shipment_date + timedelta(
                days=random.randint(AVG_DELIVERY_TIME - 2, AVG_DELIVERY_TIME + 3),
                hours=random.randint(0, 23)
            )
        else:
            # Fallback if shipment_date wasn't set
            delivered_date = order_date + timedelta(
                days=random.randint(AVG_PROCESSING_TIME + AVG_SHIPPING_TIME + AVG_DELIVERY_TIME - 2,
                                   AVG_PROCESSING_TIME + AVG_SHIPPING_TIME + AVG_DELIVERY_TIME + 3),
                hours=random.randint(0, 23)
            )
    
    return payment_date, shipment_date, delivered_date


def _apply_refund_logic(order_status, payment_status, order_date):
    """
    Apply refund logic based on REFUND_RATE
    Some delivered orders should be refunded
    """
    if order_status == 'delivered' and payment_status == 'completed':
        if random.random() < REFUND_RATE:
            return 'refunded', 'refunded'
    return order_status, payment_status


def _select_products_with_preferences(products_df, customer_category_prefs, customer_id, num_items, order_date):
    """
    Select products based on customer's category preferences
    Ensures products existed at order time (created_at <= order_date)
    """
    # Filter products to only those that existed at order time
    # Convert created_at strings to datetime for comparison
    products_df_copy = products_df.copy()
    products_df_copy['created_at_dt'] = pd.to_datetime(products_df_copy['created_at'])
    available_products_df = products_df_copy[products_df_copy['created_at_dt'] <= order_date].copy()
    available_products_df = available_products_df.drop(columns=['created_at_dt'])
    
    # If no products available, fall back to all products (shouldn't happen in normal operation)
    if len(available_products_df) == 0:
        available_products_df = products_df.copy()
    
    # Get customer's preferred categories
    preferred_categories = customer_category_prefs.get(customer_id, {})
    
    selected_products = []
    
    for _ in range(num_items):
        # CATEGORY_PREFERENCE_RATE chance to select from preferred categories (if any exist)
        if preferred_categories and random.random() < CATEGORY_PREFERENCE_RATE:
            # Select from preferred categories
            # Weight by purchase count
            categories = list(preferred_categories.keys())
            weights = [preferred_categories[cat] for cat in categories]
            preferred_category = random.choices(categories, weights=weights)[0]
            
            # Get products from this category
            category_products = available_products_df[available_products_df['category'] == preferred_category]
            if len(category_products) > 0:
                product_id = random.choice(category_products['product_id'].values)
                selected_products.append(product_id)
            else:
                # Fallback: select from all available products
                product_id = random.choice(available_products_df['product_id'].values)
                selected_products.append(product_id)
        else:
            # Select from any available category
            product_id = random.choice(available_products_df['product_id'].values)
            selected_products.append(product_id)
    
    # Ensure uniqueness (no duplicate products in same order)
    selected_products = list(set(selected_products))
    
    # If we removed duplicates, we may have fewer items, so add more
    while len(selected_products) < num_items:
        product_id = random.choice(available_products_df['product_id'].values)
        if product_id not in selected_products:
            selected_products.append(product_id)
    
    return selected_products[:num_items]


def _update_category_preferences(customer_category_prefs, customer_id, selected_products, products_dict):
    """
    Update customer's category preferences based on purchased products
    """
    if customer_id not in customer_category_prefs:
        customer_category_prefs[customer_id] = defaultdict(int)
    
    for product_id in selected_products:
        category = products_dict[product_id]['category']
        customer_category_prefs[customer_id][category] += 1


def generate_orders(num_orders=NUM_ORDERS, customers_df=None, products_df=None):
    """
    Generate orders with realistic business logic
    
    Args:
        num_orders: Number of orders to generate
        customers_df: DataFrame with customer data
        products_df: DataFrame with product data
        
    Returns:
        tuple: (orders_df, order_items_df, customers_df)
    """
    num_orders = int(num_orders)  # Ensure integer
    if customers_df is None or products_df is None:
        raise ValueError("customers_df and products_df are required")
    
    # Assign VIP subscribers (voluntary VIP status)
    all_customer_ids = customers_df['customer_id'].tolist()
    num_vip_subscribers = int(len(all_customer_ids) * VIP_SUBSCRIPTION_RATE)
    vip_subscribers = set(random.sample(all_customer_ids, num_vip_subscribers))
    
    # Create lookup dictionaries for faster access
    customers_dict = customers_df.set_index('customer_id').to_dict('index')
    products_dict = products_df.set_index('product_id').to_dict('index')
    
    # Cache parsed signup dates to avoid parsing twice
    signup_dates_cache = {}
    for customer_id, customer in customers_dict.items():
        signup_dates_cache[customer_id] = datetime.strptime(customer['signup_date'], '%Y-%m-%d %H:%M:%S')
    
    # Track customer category preferences and order history
    customer_category_prefs = {}
    customer_order_history = defaultdict(list)
    
    # Track customer segments to detect changes and update updated_at
    customer_segments = {}  # customer_id -> current_segment
    for customer_id in customers_dict.keys():
        customer_segments[customer_id] = None  # Initialize to None
    
    orders = []
    order_items = []
    
    # Track payment_id and tracking_number for FK relationships
    payment_id_counter = 1
    tracking_number_counter = 1
    
    # Pre-calculate customer weights based on tenure (how long they've been customers)
    # Customers who signed up earlier should get more orders
    customer_weights = []
    customer_ids = []
    
    for customer_id in customers_dict.keys():
        signup_date = signup_dates_cache[customer_id]
        # Calculate days since signup (tenure)
        tenure_days = (END_DATE - signup_date).days
        # Weight is proportional to tenure (min weight of 0.1 to ensure new customers still get some orders)
        weight = max(0.1, tenure_days / 365.0)  # Normalize by year
        customer_weights.append(weight)
        customer_ids.append(customer_id)
    
    # Convert to numpy arrays and normalize weights to probabilities for faster sampling
    customer_ids_array = np.array(customer_ids)
    customer_weights_array = np.array(customer_weights)
    customer_probs = customer_weights_array / customer_weights_array.sum()
    
    # Generate order-customer assignments with per-customer date distributions
    order_assignments = []
    for assignment_idx in range(1, num_orders + 1):
        # Select customer weighted by tenure (using numpy for much faster sampling)
        customer_id = np.random.choice(customer_ids_array, p=customer_probs)
        signup_date = signup_dates_cache[customer_id]
        
        # Generate order date using triangular distribution from signup to END_DATE
        tenure_days = (END_DATE - signup_date).days
        
        if tenure_days > 0:
            peak_days = tenure_days * ORDER_DATE_DISTRIBUTION_PEAK
            days_since_signup = random.triangular(0, tenure_days, peak_days)
            order_date = signup_date + timedelta(days=days_since_signup)
        else:
            # If tenure_days == 0, just use signup_date and random time
            order_date = signup_date.replace(
                hour=random.randint(0, 23),
                minute=random.randint(0, 59),
                second=random.randint(0, 59)
            )
        
        order_assignments.append((customer_id, order_date))
        
        # Progress logging for assignment phase
        pct = int((assignment_idx / num_orders) * 100)
        prev_pct = int(((assignment_idx - 1) / num_orders) * 100) if assignment_idx > 1 else -1
        
        if pct != prev_pct or assignment_idx == num_orders:
            bar_width = 50
            filled = int(bar_width * pct / 100)
            bar = '|' * filled + ' ' * (bar_width - filled)
            print(f"\r      Assigning orders: {pct:3d}% |{bar}|", end='', flush=True)
    
    # Clear assignment progress line using ANSI escape code
    print("\r\033[K", end='', flush=True)
    order_assignments.sort(key=lambda x: x[1])
    
    # Process orders chronologically
    for order_id in range(1, num_orders + 1):
        customer_id, order_date = order_assignments[order_id - 1]
        customer = customers_dict[customer_id]
        country = customer['country']
        signup_date = signup_dates_cache[customer_id]  # Use cached date
        
        # Determine payment method
        payment_method = random.choices(
            list(PAYMENT_METHODS.keys()),
            weights=list(PAYMENT_METHODS.values())
        )[0]
        
        # Determine payment status
        payment_status = _determine_payment_status()
        
        # Generate payment_id if payment completed or failed
        payment_id = None
        if payment_status in ['completed', 'failed']:
            payment_id = payment_id_counter
            payment_id_counter += 1
        
        # Select products based on category preferences
        items = list(ITEMS_PER_ORDER_DISTRIBUTION.keys())
        weights = list(ITEMS_PER_ORDER_DISTRIBUTION.values())
        num_items = random.choices(items, weights=weights)[0]
        
        selected_products = _select_products_with_preferences(
            products_df, customer_category_prefs, customer_id, num_items, order_date
        )
        
        # Update category preferences based on purchase
        _update_category_preferences(customer_category_prefs, customer_id, selected_products, products_dict)
        
        # Calculate subtotal from order items
        subtotal = 0.0
        for product_id in selected_products:
            product = products_dict[product_id]
            quantity = random.randint(QUANTITY_PER_PRODUCT_MIN, QUANTITY_PER_PRODUCT_MAX)
            unit_price = product['price']
            line_total = round(unit_price * quantity, 2)
            subtotal += line_total
            
            # Add to order_items
            order_items.append({
                'order_item_id': len(order_items) + 1,
                'order_id': order_id,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price,
                'line_total': line_total,
                'created_at': order_date.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        subtotal = round(subtotal, 2)
        
        # Determine customer segment at this order time
        segment = _determine_segment_at_order_time(
            customer_id, order_date, signup_date, 
            customer_order_history[customer_id], vip_subscribers
        )
        
        # Update customer updated_at if segment changed
        previous_segment = customer_segments.get(customer_id)
        if previous_segment != segment:
            # Segment changed, update customer's updated_at
            customers_dict[customer_id]['updated_at'] = order_date.strftime('%Y-%m-%d %H:%M:%S')
            customer_segments[customer_id] = segment
        
        # Calculate discount based on actual segment at order time
        # Use order_id as seed for consistent discount application
        discount_amount = _calculate_discount(subtotal, segment, order_id)
        
        # Calculate shipping cost based on segment
        order_number = len(customer_order_history[customer_id]) + 1
        shipping_cost = _calculate_shipping_cost(subtotal, segment, order_number, country)
        
        # Calculate tax (on subtotal after discount)
        tax_amount = _calculate_tax(subtotal, discount_amount, country)
        
        # Calculate total
        total_amount = round(subtotal - discount_amount + tax_amount + shipping_cost, 2)
        
        # Determine order status
        order_status = _determine_order_status(order_date, payment_status)
        
        # Apply refund logic
        order_status, payment_status = _apply_refund_logic(order_status, payment_status, order_date)
        
        # Calculate timestamps
        payment_date, shipment_date, delivered_date = _calculate_timestamps(
            order_date, order_status, payment_status
        )
        
        # Determine shipping carrier
        shipping_carrier = random.choices(
            list(CARRIERS.keys()),
            weights=list(CARRIERS.values())
        )[0]
        
        # Generate tracking_number if order is shipped/delivered/refunded
        tracking_number = None
        if order_status in ['shipped', 'delivered', 'refunded']:
            tracking_number = tracking_number_counter
            tracking_number_counter += 1
        
        # Create order record
        order_date_str = order_date.strftime('%Y-%m-%d %H:%M:%S')
        payment_date_str = payment_date.strftime('%Y-%m-%d %H:%M:%S') if payment_date else None
        shipment_date_str = shipment_date.strftime('%Y-%m-%d %H:%M:%S') if shipment_date else None
        delivered_date_str = delivered_date.strftime('%Y-%m-%d %H:%M:%S') if delivered_date else None
        
        # Calculate updated_at as latest of all event dates
        event_dates = [order_date]
        if payment_date:
            event_dates.append(payment_date)
        if shipment_date:
            event_dates.append(shipment_date)
        if delivered_date:
            event_dates.append(delivered_date)
        updated_at = max(event_dates).strftime('%Y-%m-%d %H:%M:%S')
        
        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'order_date': order_date_str,
            'payment_date': payment_date_str,
            'shipment_date': shipment_date_str,
            'delivered_date': delivered_date_str,
            'created_at': order_date_str,
            'updated_at': updated_at,
            'order_status': order_status,
            'payment_status': payment_status,
            'subtotal': subtotal,
            'discount_amount': discount_amount,
            'tax_amount': tax_amount,
            'shipping_cost': shipping_cost,
            'total_amount': total_amount,
            'payment_method': payment_method,
            'payment_id': payment_id,
            'shipping_carrier': shipping_carrier,
            'tracking_number': tracking_number,
            'customer_segment_at_order': segment  # Track segment at order time
        }
        
        orders.append(order)
        
        # Add to customer order history (for future orders' segment calculation)
        customer_order_history[customer_id].append((order_date, total_amount))
        
        # Update progress every 1%
        pct = int((order_id / num_orders) * 100)
        prev_pct = int(((order_id - 1) / num_orders) * 100) if order_id > 1 else -1
        
        if pct != prev_pct or order_id == num_orders:
            bar_width = 50
            filled = int(bar_width * pct / 100)
            bar = '|' * filled + ' ' * (bar_width - filled)
            print(f"\r      Orders: {pct:3d}% |{bar}|", end='', flush=True)
    
    # Clear progress line using ANSI escape code
    print("\r\033[K", end='', flush=True)
    orders_df = pd.DataFrame(orders)
    order_items_df = pd.DataFrame(order_items)
    
    # Update customers_df with final segments and updated_at values
    customers_df = customers_df.copy()
    for customer_id, customer_data in customers_dict.items():
        idx = customers_df[customers_df['customer_id'] == customer_id].index
        if len(idx) > 0:
            customers_df.at[idx[0], 'customer_segment'] = customer_segments.get(customer_id)
            customers_df.at[idx[0], 'updated_at'] = customer_data.get('updated_at', customer_data['signup_date'])
    
    return orders_df, order_items_df, customers_df
