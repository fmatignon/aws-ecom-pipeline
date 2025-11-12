"""
E-commerce data generation package
"""

from .main import (
    generate_all_data,
    generate_sample_data,
    generate_full_data,
)

# Re-export generators for convenience
from .generators import (
    generate_customers,
    generate_products,
    generate_orders,
    generate_payments,
    generate_shipments,
)

# Re-export config items
from .config import PRODUCT_TEMPLATES

__all__ = [
    # Main functions
    'generate_all_data',
    'generate_sample_data',
    'generate_full_data',
    # Generator functions
    'generate_customers',
    'generate_products',
    'generate_orders',
    'generate_payments',
    'generate_shipments',
    # Config
    'PRODUCT_TEMPLATES',
]

