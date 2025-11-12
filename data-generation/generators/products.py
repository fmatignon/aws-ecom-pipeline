"""
Generates realistic product data from the parameters in settings.py and product_templates.py
"""

from config.product_templates import PRODUCT_TEMPLATES
from config.settings import NUM_PRODUCTS, START_DATE, END_DATE
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
import random
import hashlib


def _generate_consistent_price(brand, adjective, product_name, price_range):
    """
    Generate a consistent price for the same brand+adjective+product combination
    using a hash-based approach
    """
    # Create a unique key for this combination
    key = f"{brand}|{adjective}|{product_name}"
    # Use hash to get a deterministic but random-seeming value
    hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
    # Map to price range
    min_price, max_price = price_range
    price = min_price + (hash_value % 10000) / 10000.0 * (max_price - min_price)
    return round(price, 2)


def _generate_consistent_margin(brand, adjective, product_name, margin_range):
    """
    Generate a consistent margin for the same brand+adjective+product combination
    """
    key = f"{brand}|{adjective}|{product_name}|margin"
    hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
    min_margin, max_margin = margin_range
    margin = min_margin + (hash_value % 10000) / 10000.0 * (max_margin - min_margin)
    return margin


def generate_products(num_products=NUM_PRODUCTS):
    """
    Generate product catalog with realistic names, prices, costs, and colors.
    Ensures same brand+product+adjective has same price/cost across colors.
    """
    num_products = int(num_products)  # Ensure integer
    # Collect all category/subcategory/product combinations
    all_combinations = []
    for category, subcategories in PRODUCT_TEMPLATES.items():
        for subcategory, data in subcategories.items():
            brands = data['brands']
            adjectives = data['adjectives']
            products = data['products']
            margin_range = data['margin_range']
            
            for product_template in products:
                product_name = product_template['name']
                price_range = product_template['price_range']
                colors = product_template.get('colors', [None])  # None if no colors
                
                # Generate all brand+adjective combinations for this product
                for brand in brands:
                    for adjective in adjectives:
                        # Generate base price and margin (consistent across colors)
                        base_price = _generate_consistent_price(
                            brand, adjective, product_name, price_range
                        )
                        margin = _generate_consistent_margin(
                            brand, adjective, product_name, margin_range
                        )
                        base_cost = round(base_price * (1 - margin), 2)
                        
                        # Create product for each color (or one if no colors)
                        for color in colors:
                            all_combinations.append({
                                'category': category,
                                'subcategory': subcategory,
                                'brand': brand,
                                'adjective': adjective,
                                'product_name': product_name,
                                'color': color,
                                'base_price': base_price,
                                'base_cost': base_cost,
                                'margin': margin
                            })
    
    # If we have more combinations than needed, sample randomly
    # Otherwise, we'll use all combinations and may need to duplicate some
    if len(all_combinations) <= num_products:
        # Use all combinations, then randomly duplicate to reach target
        selected = all_combinations.copy()
        while len(selected) < num_products:
            selected.append(random.choice(all_combinations))
        selected = random.sample(selected, num_products)
    else:
        # Randomly sample to get exactly num_products
        selected = random.sample(all_combinations, num_products)
    
    # Build final product list
    products = []
    product_id = 1
    
    # Calculate peak date for triangular distribution (~30% of the way from START_DATE to END_DATE)
    date_range_days = (END_DATE - START_DATE).days
    peak_days = int(date_range_days * 0.3)
    peak_date = START_DATE + timedelta(days=peak_days)
    
    for combo in selected:
        # Build product name: Brand + Adjective + Product
        product_name_full = f"{combo['brand']} {combo['adjective']} {combo['product_name']}"
        
        # Generate created_at with triangular distribution (more recent products more common)
        days_offset = random.triangular(0, date_range_days, peak_days)
        created_at = START_DATE + timedelta(days=int(days_offset))
        
        product = {
            'product_id': product_id,
            'product_name': product_name_full,
            'category': combo['category'],
            'sub_category': combo['subcategory'],
            'brand': combo['brand'],
            'price': combo['base_price'],
            'cost': combo['base_cost'],
            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Add color if available
        if combo['color'] is not None:
            product['color'] = combo['color']
        
        products.append(product)
        
        # Update progress every 1%
        pct = int((product_id / num_products) * 100)
        prev_pct = int(((product_id - 1) / num_products) * 100) if product_id > 1 else -1
        
        if pct != prev_pct or product_id == num_products:
            bar_width = 50
            filled = int(bar_width * pct / 100)
            bar = '|' * filled + ' ' * (bar_width - filled)
            print(f"\r      Products: {pct:3d}% |{bar}|", end='', flush=True)
        
        product_id += 1
    
    # Clear progress line using ANSI escape code
    print("\r\033[K", end='', flush=True)
    return pd.DataFrame(products)
