"""
Configuration settings for data generation
"""
from datetime import datetime, timedelta

# Generation volumes
NUM_CUSTOMERS = 120000  # Increased to reduce orders per customer (more realistic distribution)
NUM_PRODUCTS = int(NUM_CUSTOMERS / 20)
NUM_ORDERS = int(NUM_CUSTOMERS * 3.75)

# Date ranges
START_DATE = datetime.now() - timedelta(days=540)  # 18 months ago
END_DATE = datetime.now()

# Business logic constants
ORDER_SUCCESS_RATE = 0.95  # 95% of orders complete successfully
REFUND_RATE = 0.06

# Shipping carriers
CARRIERS = {
    'UPS': 0.40,
    'FedEx': 0.35,
    'USPS': 0.20,
    'DHL': 0.05
}

# Payment methods
PAYMENT_METHODS = {
    'credit_card': 0.65,
    'paypal': 0.20,
    'apple_pay': 0.10,
    'google_pay': 0.05
}

# Card brands
CARD_BRANDS = {
    'visa': 0.50,
    'mastercard': 0.30,
    'amex': 0.15,
    'discover': 0.05
}

# Parameters for each country
COUNTRIES = {
    'United States': {
        'weight': 0.60,
        'tax_rate': 0.08,
        'shipping_cost_range': (5.0, 12.0)
    },
    'Canada': {
        'weight': 0.13,
        'tax_rate': 0.13,
        'shipping_cost_range': (8.0, 18.0)
    },
    'United Kingdom': {
        'weight': 0.10,
        'tax_rate': 0.20,
        'shipping_cost_range': (10.0, 25.0)
    },
    'Germany': {
        'weight': 0.05,
        'tax_rate': 0.19,
        'shipping_cost_range': (10.0, 25.0)
    },
    'France': {
        'weight': 0.04,
        'tax_rate': 0.20,
        'shipping_cost_range': (10.0, 25.0)
    },
    'Italy': {
        'weight': 0.02,
        'tax_rate': 0.22,
        'shipping_cost_range': (10.0, 25.0)
    },
    'Spain': {
        'weight': 0.02,
        'tax_rate': 0.21,
        'shipping_cost_range': (10.0, 25.0)
    },
    'Japan': {
        'weight': 0.02,
        'tax_rate': 0.10,
        'shipping_cost_range': (12.0, 30.0)
    },
    'Netherlands': {
        'weight': 0.01,
        'tax_rate': 0.21,
        'shipping_cost_range': (10.0, 25.0)
    },
    'Australia': {
        'weight': 0.01,
        'tax_rate': 0.10,
        'shipping_cost_range': (12.0, 30.0)
    }
}

# Customer segments parameters
CUSTOMER_SEGMENTS = {
    'New': {
        'discount_rate': 0.10,
        'free_shipping_threshold': {
            'first_order': 0.0,  # Free shipping on first order
            'second_order': 40.0  # $40 threshold on 2nd order
        }
    },
    'Regular': {
        'discount_rate': 0.05,
        'free_shipping_threshold': 50.0
    },
    'VIP': {
        'discount_rate': 0.10,
        'free_shipping_threshold': 20.0
    }
}

# Segment calculation rules
VIP_SPENDING_THRESHOLD = 6000.0  # Last 6 months spending threshold for VIP
VIP_SUBSCRIPTION_RATE = 0.10  # 10% voluntary VIP subscription
NEW_CUSTOMER_DURATION_DAYS = 60  # Or 2 orders, whichever comes first

# Discount logic
DISCOUNT_USAGE_RATE = 0.60  # 60% of customers use available discounts

# Category preference
CATEGORY_PREFERENCE_RATE = 0.60  # 60% from preferred categories, 40% from any

# Payment status values (larger pending % represents in-flight payments)
PAYMENT_STATUSES = {
    'completed': 0.92,  # 92% completed
    'failed': 0.03,  # 3% failed
    'pending': 0.05,  # 5% pending (in-flight transactions)
    'refunded': 0.00  # Refunded handled separately based on REFUND_RATE
}

# Order composition
ITEMS_PER_ORDER_DISTRIBUTION = {
    1: 0.30,  # 30% of orders have 1 item
    2: 0.30,  # 30% have 2 items
    3: 0.20,  # 20% have 3 items
    4: 0.15,  # 15% have 4 items
    5: 0.05   # 5% have 5 items
}
QUANTITY_PER_PRODUCT_MIN = 1  # Minimum quantity per product in order
QUANTITY_PER_PRODUCT_MAX = 3  # Maximum quantity per product in order

# Order date distribution
ORDER_DATE_DISTRIBUTION_PEAK = 0.3  # Peak of triangular distribution (30% of time range)

# Order status transition probabilities
ORDER_STATUS_TRANSITION_WEIGHTS = {
    'shipped_delivered': [0.3, 0.7],  # [shipped, delivered] weights for medium-old orders
    'processing_shipped': [0.4, 0.6]  # [processing, shipped] weights for recent orders
}

# Order fulfillment timing (in days)
AVG_PROCESSING_TIME = 1  # Average days to process order
AVG_SHIPPING_TIME = 3  # Average days to ship after processing
AVG_DELIVERY_TIME = 5  # Average days to deliver after shipping
