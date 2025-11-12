"""
Generates realistic payment data from orders
"""

from config.settings import PAYMENT_METHODS, CARD_BRANDS, PAYMENT_STATUSES, REFUND_RATE
from datetime import datetime, timedelta
import pandas as pd
import random


def _generate_card_last_4():
    """
    Generate last 4 digits of card number
    """
    return str(random.randint(1000, 9999))


def generate_payments(orders_df):
    """
    Generate payment records from orders data
    
    Args:
        orders_df: DataFrame with order data (must have payment_id, payment_method, 
                    payment_status, total_amount, payment_date, customer_id, order_id)
    
    Returns:
        payments_df: DataFrame with payment records
    """
    if orders_df is None or len(orders_df) == 0:
        raise ValueError("orders_df is required and must not be empty")
    
    # Filter orders that have payment_id (completed or failed payments)
    orders_with_payment = orders_df[orders_df['payment_id'].notna()].copy()
    
    if len(orders_with_payment) == 0:
        return pd.DataFrame()
    
    payments = []
    
    for idx, (_, order) in enumerate(orders_with_payment.iterrows(), 1):
        payment_id = int(order['payment_id'])
        payment_method = order['payment_method']
        payment_status = order['payment_status']
        amount = order['total_amount']
        payment_date_str = order['payment_date']
        customer_id = order['customer_id']
        order_id = order['order_id']
        delivered_date_str = order.get('delivered_date')
        
        # Determine card_brand if payment_method is credit_card
        card_brand = None
        card_last_4 = None
        if payment_method == 'credit_card':
            card_brand = random.choices(
                list(CARD_BRANDS.keys()),
                weights=list(CARD_BRANDS.values())
            )[0]
            card_last_4 = _generate_card_last_4()
        
        # Check if payment should be refunded (based on REFUND_RATE)
        # This applies to completed payments that are part of refunded orders
        final_payment_status = payment_status
        if payment_status == 'completed' and order.get('order_status') == 'refunded':
            if random.random() < REFUND_RATE:
                final_payment_status = 'refunded'
        
        # Calculate created-at and updated-at timestamps
        created_at = payment_date_str
        
        # Calculate updated-at based on payment status
        if final_payment_status == 'refunded' and delivered_date_str:
            # Refund processed 5-30 days after delivery
            delivered_date = datetime.strptime(delivered_date_str, '%Y-%m-%d %H:%M:%S')
            refund_days = random.randint(5, 30)
            updated_at = (delivered_date + timedelta(days=refund_days)).strftime('%Y-%m-%d %H:%M:%S')
        else:
            # For completed or failed, updated_at same as payment_date
            updated_at = payment_date_str
        
        payment = {
            'payment-id': payment_id,
            'order-id': order_id,
            'customer-id': customer_id,
            'payment-method': payment_method,
            'card-brand': card_brand,
            'card-last-4': card_last_4,
            'amount': amount,
            'payment-status': final_payment_status,
            'payment-date': payment_date_str,
            'created-at': created_at,
            'updated-at': updated_at,
        }
        
        payments.append(payment)
        
        # Update progress every 1%
        total = len(orders_with_payment)
        pct = int((idx / total) * 100)
        prev_pct = int(((idx - 1) / total) * 100) if idx > 1 else -1
        
        if pct != prev_pct or idx == total:
            bar_width = 50
            filled = int(bar_width * pct / 100)
            bar = '|' * filled + ' ' * (bar_width - filled)
            print(f"\r      Payments: {pct:3d}% |{bar}|", end='', flush=True)
    
    # Clear progress line using ANSI escape code
    print("\r\033[K", end='', flush=True)
    return pd.DataFrame(payments)
