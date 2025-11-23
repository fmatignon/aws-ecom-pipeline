"""
Generates realistic product data from the parameters in settings.py and product_templates.py
"""

from config.product_templates import PRODUCT_TEMPLATES
from config.settings import NUM_PRODUCTS, START_DATE, END_DATE, PRODUCTS_PER_DAY
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
import random
import hashlib
from utils.logging_utils import (
    log_section_start,
    log_section_complete,
    log_progress,
    clear_progress_line,
)


def _generate_consistent_price(brand, adjective, product_name, price_range):
    """
    Generate a deterministic price for a brand/adjective/product combination.

    Args:
        brand (str): Product brand name.
        adjective (str): Descriptive term used in the product title.
        product_name (str): Base product name.
        price_range (tuple[float, float]): Minimum and maximum allowable price.

    Returns:
        float: Stable price for the specified combination.
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
    Generate a deterministic margin for a brand/adjective/product combination.

    Args:
        brand (str): Product brand name.
        adjective (str): Descriptive term used in the product title.
        product_name (str): Base product name.
        margin_range (tuple[float, float]): Minimum and maximum margin values.

    Returns:
        float: Stable margin for the specified combination.
    """
    key = f"{brand}|{adjective}|{product_name}|margin"
    hash_value = int(hashlib.md5(key.encode()).hexdigest(), 16)
    min_margin, max_margin = margin_range
    margin = min_margin + (hash_value % 10000) / 10000.0 * (max_margin - min_margin)
    return margin


def generate_products(num_products=NUM_PRODUCTS):
    """
    Generate a product catalog with consistent pricing, costs, and colors.
    Ensures identical brand/product/adjective combinations reuse values.

    Args:
        num_products (int): Total number of products to produce.

    Returns:
        pd.DataFrame: Generated product catalog.
    """
    section = "Legacy Product Generation"
    log_section_start(section)
    num_products = int(num_products)  # Ensure integer
    # Collect all category/subcategory/product combinations
    all_combinations = []
    for category, subcategories in PRODUCT_TEMPLATES.items():
        for subcategory, data in subcategories.items():
            brands = data["brands"]
            adjectives = data["adjectives"]
            products = data["products"]
            margin_range = data["margin_range"]

            for product_template in products:
                product_name = product_template["name"]
                price_range = product_template["price_range"]
                colors = product_template.get("colors", [None])  # None if no colors

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
                            all_combinations.append(
                                {
                                    "category": category,
                                    "subcategory": subcategory,
                                    "brand": brand,
                                    "adjective": adjective,
                                    "product_name": product_name,
                                    "color": color,
                                    "base_price": base_price,
                                    "base_cost": base_cost,
                                    "margin": margin,
                                }
                            )

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

    prev_pct = -1
    for combo in selected:
        # Build product name: Brand + Adjective + Product
        product_name_full = (
            f"{combo['brand']} {combo['adjective']} {combo['product_name']}"
        )

        # Generate created_at with triangular distribution (more recent products more common)
        days_offset = random.triangular(0, date_range_days, peak_days)
        created_at = START_DATE + timedelta(days=int(days_offset))

        product = {
            "product_id": product_id,
            "product_name": product_name_full,
            "category": combo["category"],
            "sub_category": combo["subcategory"],
            "brand": combo["brand"],
            "price": combo["base_price"],
            "cost": combo["base_cost"],
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Add color if available
        if combo["color"] is not None:
            product["color"] = combo["color"]

        products.append(product)

        # Update progress every 1%
        current_index = product_id
        pct = int((current_index / num_products) * 100)
        if pct != prev_pct or current_index == num_products:
            log_progress(section, f"{pct}% complete", end="\r", flush=True)
            prev_pct = pct
        product_id += 1

    clear_progress_line(section)
    log_section_complete(section, f"{len(products):,} products generated")
    return pd.DataFrame(products)


def generate_products_for_date_range(
    start_date: datetime,
    end_date: datetime,
    start_product_id: int = 1,
    products_per_day: int = None,
) -> pd.DataFrame:
    """
    Generate products for a specific date range (for unified system)

    Args:
        start_date (datetime): Inclusive start timestamp for product creation.
        end_date (datetime): Inclusive end timestamp for product creation.
        start_product_id (int): Initial identifier for generated products.
        products_per_day (int | None): Optional daily generation rate override.

    Returns:
        pd.DataFrame: Product records created for the requested range.
    """
    # Calculate number of days and products to generate
    days = (end_date - start_date).days + 1
    # Use provided rate or default from settings
    rate = products_per_day if products_per_day is not None else PRODUCTS_PER_DAY
    num_products = int(rate * days)

    section = "Product Date-Range Generation"
    log_section_start(section)
    if num_products == 0:
        log_section_complete(section, "No products required for provided date range")
        return pd.DataFrame()

    # Collect all category/subcategory/product combinations
    all_combinations = []
    for category, subcategories in PRODUCT_TEMPLATES.items():
        for subcategory, data in subcategories.items():
            brands = data["brands"]
            adjectives = data["adjectives"]
            products = data["products"]
            margin_range = data["margin_range"]

            for product_template in products:
                product_name = product_template["name"]
                price_range = product_template["price_range"]
                colors = product_template.get("colors", [None])

                for brand in brands:
                    for adjective in adjectives:
                        base_price = _generate_consistent_price(
                            brand, adjective, product_name, price_range
                        )
                        margin = _generate_consistent_margin(
                            brand, adjective, product_name, margin_range
                        )
                        base_cost = round(base_price * (1 - margin), 2)

                        for color in colors:
                            all_combinations.append(
                                {
                                    "category": category,
                                    "subcategory": subcategory,
                                    "brand": brand,
                                    "adjective": adjective,
                                    "product_name": product_name,
                                    "color": color,
                                    "base_price": base_price,
                                    "base_cost": base_cost,
                                    "margin": margin,
                                }
                            )

    # Sample combinations if needed
    if len(all_combinations) <= num_products:
        selected = all_combinations.copy()
        while len(selected) < num_products:
            selected.append(random.choice(all_combinations))
        selected = random.sample(selected, num_products)
    else:
        selected = random.sample(all_combinations, num_products)

    # Build final product list
    products = []
    product_id = start_product_id
    date_range_days = (end_date - start_date).days

    prev_pct = -1
    for index, combo in enumerate(selected, 1):
        product_name_full = (
            f"{combo['brand']} {combo['adjective']} {combo['product_name']}"
        )

        # Generate created_at within date range
        days_offset = random.uniform(0, date_range_days)
        created_at = start_date + timedelta(
            days=int(days_offset), hours=random.randint(0, 23)
        )

        product = {
            "product_id": product_id,
            "product_name": product_name_full,
            "category": combo["category"],
            "sub_category": combo["subcategory"],
            "brand": combo["brand"],
            "price": combo["base_price"],
            "cost": combo["base_cost"],
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        }

        if combo["color"] is not None:
            product["color"] = combo["color"]

        products.append(product)
        product_id += 1

        pct = int((index / num_products) * 100)
        if pct != prev_pct or index == num_products:
            log_progress(section, f"{pct}% complete", end="\r", flush=True)
            prev_pct = pct

    clear_progress_line(section)
    log_section_complete(section, f"{len(products):,} products generated")
    return pd.DataFrame(products)
