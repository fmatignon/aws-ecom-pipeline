"""
Generates realistic order data with business logic from settings.py
"""

from config.settings import (
    NUM_ORDERS,
    END_DATE,
    ORDER_SUCCESS_RATE,
    REFUND_RATE,
    CARRIERS,
    PAYMENT_METHODS,
    COUNTRIES,
    CUSTOMER_SEGMENTS,
    PAYMENT_STATUSES,
    DISCOUNT_USAGE_RATE,
    CATEGORY_PREFERENCE_RATE,
    VIP_SPENDING_THRESHOLD,
    VIP_SUBSCRIPTION_RATE,
    NEW_CUSTOMER_DURATION_DAYS,
    ITEMS_PER_ORDER_DISTRIBUTION,
    QUANTITY_PER_PRODUCT_MIN,
    QUANTITY_PER_PRODUCT_MAX,
    ORDER_STATUS_TRANSITION_WEIGHTS,
    AVG_PROCESSING_TIME,
    AVG_SHIPPING_TIME,
    AVG_DELIVERY_TIME,
    ORDERS_PER_DAY,
)
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import random
from collections import defaultdict
from utils.logging_utils import (
    log_section_start,
    log_section_complete,
    log_progress,
    clear_progress_line,
)


def _determine_segment_at_order_time(
    customer_id, order_date, signup_date, customer_order_history, vip_subscribers
):
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
        return "VIP"

    # Calculate days since signup
    days_since_signup = (order_date - signup_date).days

    # Count previous orders (not including current one)
    num_previous_orders = len(customer_order_history)

    # New customer: < 60 days since signup OR < 2 orders
    if days_since_signup < NEW_CUSTOMER_DURATION_DAYS or num_previous_orders < 2:
        return "New"

    # Calculate spending in last 6 months (from previous orders only)
    six_months_ago = order_date - timedelta(days=180)
    recent_spending = sum(
        amount for date, amount in customer_order_history if date >= six_months_ago
    )

    # VIP: last 6 months spending > threshold
    if recent_spending > VIP_SPENDING_THRESHOLD:
        return "VIP"

    # Default: Regular
    return "Regular"


def _calculate_shipping_cost(subtotal, segment, order_number, country):
    """
    Calculate shipping cost based on customer segment and geography.

    Args:
        subtotal (float): Order subtotal used to evaluate free shipping thresholds.
        segment (str): Customer segment classification.
        order_number (int): Sequence number of the customer's order.
        country (str): Destination country impacting shipping cost ranges.

    Returns:
        float: Calculated shipping amount.
    """
    # Determine free shipping threshold based on segment
    if segment == "New":
        if order_number == 1:
            threshold = CUSTOMER_SEGMENTS["New"]["free_shipping_threshold"][
                "first_order"
            ]
        elif order_number == 2:
            threshold = CUSTOMER_SEGMENTS["New"]["free_shipping_threshold"][
                "second_order"
            ]
        else:
            # Shouldn't happen for New customers, but fallback to Regular
            threshold = CUSTOMER_SEGMENTS["Regular"]["free_shipping_threshold"]
    elif segment == "VIP":
        threshold = CUSTOMER_SEGMENTS["VIP"]["free_shipping_threshold"]
    else:  # Regular
        threshold = CUSTOMER_SEGMENTS["Regular"]["free_shipping_threshold"]

    # Free shipping if subtotal meets threshold
    if subtotal >= threshold:
        return 0.0

    # Base shipping cost varies by country
    cost_range = COUNTRIES[country]["shipping_cost_range"]
    base_cost = random.uniform(cost_range[0], cost_range[1])

    return round(base_cost, 2)


def _calculate_discount(subtotal, segment, discount_seed):
    """
    Calculate a segment-based discount amount.
    Uses a deterministic seed to produce reproducible randomness.

    Args:
        subtotal (float): Pre-discount order subtotal.
        segment (str): Customer segment dictating discount eligibility.
        discount_seed (int): Seed value that stabilizes randomness per order.

    Returns:
        float: Discount amount to subtract from the subtotal.
    """
    discount_rate = CUSTOMER_SEGMENTS[segment]["discount_rate"]

    # Use discount_seed to determine if discount is used (reproducible)
    # Convert to int if it's a numpy/pandas type
    seed_value = int(discount_seed) if discount_seed is not None else None
    random.seed(seed_value)
    use_discount = random.random() < DISCOUNT_USAGE_RATE
    random.seed()  # Reset seed

    if discount_rate > 0 and use_discount:
        return round(subtotal * discount_rate, 2)
    return 0.0


def _calculate_tax(subtotal, discount_amount, country):
    """
    Calculate tax on the post-discount subtotal.

    Args:
        subtotal (float): Original order subtotal.
        discount_amount (float): Discount deducted from the subtotal.
        country (str): Country key used to determine tax rate.

    Returns:
        float: Calculated tax amount.
    """
    tax_rate = COUNTRIES[country]["tax_rate"]
    taxable_amount = subtotal - discount_amount
    return round(taxable_amount * tax_rate, 2)


def _determine_order_status(order_date, payment_status, current_date=None):
    """
    Determine order status based on payment results and elapsed time.

    Args:
        order_date (datetime): Timestamp when the order was placed.
        payment_status (str): Payment outcome guiding possible statuses.
        current_date (datetime | None): Reference date for status progression.

    Returns:
        str: Derived order status value.
    """
    # If payment failed, order is cancelled
    if payment_status == "failed":
        return "cancelled"

    # If payment pending, order is pending
    if payment_status == "pending":
        return "pending"

    # Use current_date if provided, otherwise use END_DATE
    reference_date = current_date if current_date else END_DATE

    # For completed payments, determine status based on time since order
    days_since_order = (reference_date - order_date).days

    # Adjust based on timing - older orders more likely to be delivered
    if days_since_order > AVG_PROCESSING_TIME + AVG_SHIPPING_TIME + AVG_DELIVERY_TIME:
        # Very old orders should be delivered (with small chance of refund)
        if random.random() < ORDER_SUCCESS_RATE:
            return "delivered"
        else:
            return "refunded"
    elif days_since_order > AVG_PROCESSING_TIME + AVG_SHIPPING_TIME:
        # Medium old orders likely shipped or delivered
        weights = ORDER_STATUS_TRANSITION_WEIGHTS["shipped_delivered"]
        return random.choices(["shipped", "delivered"], weights=weights)[0]
    elif days_since_order > AVG_PROCESSING_TIME:
        # Recent orders likely processing or shipped
        weights = ORDER_STATUS_TRANSITION_WEIGHTS["processing_shipped"]
        return random.choices(["processing", "shipped"], weights=weights)[0]
    else:
        # Very recent orders likely processing
        return "processing"


def _determine_payment_status():
    """
    Determine payment status using configured probability weights.

    Returns:
        str: Selected payment status value.
    """
    statuses = list(PAYMENT_STATUSES.keys())
    weights = list(PAYMENT_STATUSES.values())
    return random.choices(statuses, weights=weights)[0]


def _calculate_timestamps(order_date, order_status, payment_status):
    """
    Calculate payment, shipment, and delivery timestamps based on status.

    Args:
        order_date (datetime): Time when the order was placed.
        order_status (str): Final order status, influences shipment/delivery times.
        payment_status (str): Payment outcome determining payment timestamp logic.

    Returns:
        tuple[datetime | None, datetime | None, datetime | None]: Payment, shipment,
            and delivery timestamps respectively.
    """
    payment_date = None
    shipment_date = None
    delivered_date = None

    # Payment date - usually same day or next day if payment completed
    if payment_status == "completed":
        payment_date = order_date + timedelta(
            hours=random.randint(0, 24), minutes=random.randint(0, 59)
        )
    elif payment_status == "failed":
        payment_date = order_date + timedelta(
            hours=random.randint(1, 6), minutes=random.randint(0, 59)
        )

    # Shipment date - only if order is shipped, delivered, or refunded
    if order_status in ["shipped", "delivered", "refunded"]:
        shipment_date = order_date + timedelta(
            days=random.randint(
                AVG_PROCESSING_TIME, AVG_PROCESSING_TIME + AVG_SHIPPING_TIME
            ),
            hours=random.randint(0, 23),
        )

    # Delivered date - only if order is delivered or refunded
    if order_status in ["delivered", "refunded"]:
        if shipment_date:
            delivered_date = shipment_date + timedelta(
                days=random.randint(AVG_DELIVERY_TIME - 2, AVG_DELIVERY_TIME + 3),
                hours=random.randint(0, 23),
            )
        else:
            # Fallback if shipment_date wasn't set
            delivered_date = order_date + timedelta(
                days=random.randint(
                    AVG_PROCESSING_TIME + AVG_SHIPPING_TIME + AVG_DELIVERY_TIME - 2,
                    AVG_PROCESSING_TIME + AVG_SHIPPING_TIME + AVG_DELIVERY_TIME + 3,
                ),
                hours=random.randint(0, 23),
            )

    return payment_date, shipment_date, delivered_date


def _apply_refund_logic(order_status, payment_status, order_date):
    """
    Apply refund logic to delivered orders using the configured refund rate.

    Args:
        order_status (str): Current order status prior to refund evaluation.
        payment_status (str): Current payment status.
        order_date (datetime): Original order timestamp, used for randomization context.

    Returns:
        tuple[str, str]: Possibly updated order and payment statuses.
    """
    if order_status == "delivered" and payment_status == "completed":
        if random.random() < REFUND_RATE:
            return "refunded", "refunded"
    return order_status, payment_status


def _select_products_with_preferences(
    products_df, customer_category_prefs, customer_id, num_items, order_date
):
    """
    Select products based on customer category preferences and availability.

    Args:
        products_df (pd.DataFrame): Product catalog with availability timestamps.
        customer_category_prefs (dict): Purchase counts per category per customer.
        customer_id (int): Identifier for the customer placing the order.
        num_items (int): Number of unique products to include in the order.
        order_date (datetime): Order timestamp used to filter available products.

    Returns:
        list[int]: Product identifiers selected for the order.
    """
    if "created_at_dt" not in products_df.columns:
        raise ValueError(
            "products_df must include 'created_at_dt' column for availability filtering"
        )

    available_products_df = products_df[products_df["created_at_dt"] <= order_date]
    if available_products_df.empty:
        available_products_df = products_df

    available_product_ids = available_products_df["product_id"].tolist()
    if not available_product_ids:
        raise ValueError("No available products to select from for order generation")

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
            category_products = available_products_df[
                available_products_df["category"] == preferred_category
            ]
            if len(category_products) > 0:
                product_id = random.choice(category_products["product_id"].values)
                selected_products.append(product_id)
            else:
                # Fallback: select from all available products
                product_id = random.choice(available_product_ids)
                selected_products.append(product_id)
        else:
            # Select from any available category
            product_id = random.choice(available_product_ids)
            selected_products.append(product_id)

    # Try to deduplicate without losing original order
    unique_selected = []
    seen_ids = set()
    for product_id in selected_products:
        if product_id not in seen_ids:
            unique_selected.append(product_id)
            seen_ids.add(product_id)

    selected_products = unique_selected
    unique_available_ids = list(dict.fromkeys(available_product_ids))
    max_unique = len(unique_available_ids)

    # Fill remaining slots, preferring new products until we exhaust unique options
    while len(selected_products) < num_items:
        if max_unique == 0:
            break

        candidate = random.choice(unique_available_ids)
        if len(set(selected_products)) < max_unique:
            if candidate not in selected_products:
                selected_products.append(candidate)
        else:
            selected_products.append(candidate)

    # If duplicates are needed because available catalog is smaller than num_items
    while len(selected_products) < num_items:
        selected_products.append(random.choice(unique_available_ids))

    return selected_products[:num_items]


def _update_category_preferences(
    customer_category_prefs, customer_id, selected_products, products_dict
):
    """
    Update the customer's category preferences based on purchased products.

    Args:
        customer_category_prefs (dict): Running category counts per customer.
        customer_id (int): Identifier of the customer being updated.
        selected_products (list[int]): Product identifiers purchased in the order.
        products_dict (dict): Product metadata keyed by product identifier.
    """
    if customer_id not in customer_category_prefs:
        customer_category_prefs[customer_id] = defaultdict(int)

    for product_id in selected_products:
        category = products_dict[product_id]["category"]
        customer_category_prefs[customer_id][category] += 1


def generate_orders(num_orders=NUM_ORDERS, customers_df=None, products_df=None):
    """
    Generate orders with realistic business logic (legacy function for backward compatibility)

    This function is a wrapper around generate_orders_for_date_range() for initial seed generation.
    It generates orders across START_DATE to END_DATE.

    Note: Customer segments are NOT updated here - that should be done separately by the caller.

    Args:
        num_orders (int): Requested order count (ignored; date range dictates volume).
        customers_df (pd.DataFrame): Customer dataset required for generation.
        products_df (pd.DataFrame): Product dataset required for generation.

    Returns:
        tuple: (orders_df, order_items_df, customers_df)
        Note: customers_df is returned unchanged - caller should update segments separately
    """
    from config.settings import START_DATE, END_DATE

    section = "Legacy Order Generation"
    log_section_start(section)

    if customers_df is None or products_df is None:
        raise ValueError("customers_df and products_df are required")

    # Use generate_orders_for_date_range for the full date range
    start_datetime = datetime.combine(START_DATE.date(), datetime.min.time())
    end_datetime = datetime.combine(END_DATE.date(), datetime.max.time())

    # Generate orders
    orders_df, order_items_df = generate_orders_for_date_range(
        start_datetime,
        end_datetime,
        customers_df,
        products_df,
        pd.DataFrame(),  # No existing orders for initial seed
        start_order_id=1,
        start_payment_id=1,
        start_tracking_number=1,
    )

    # Return customers_df unchanged - caller should update segments separately
    log_section_complete(
        section,
        f"{len(orders_df):,} orders and {len(order_items_df):,} items generated",
    )
    return orders_df, order_items_df, customers_df


def generate_orders_for_date_range(
    start_date: datetime,
    end_date: datetime,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    existing_orders_df: pd.DataFrame,
    start_order_id: int = 1,
    start_order_item_id: int = 1,
    start_payment_id: int = 1,
    start_tracking_number: int = 1,
    orders_per_day: int = None,
) -> tuple:
    """
    Generate new orders for date range (for unified system)

    Args:
        start_date (datetime): Inclusive start timestamp for order generation.
        end_date (datetime): Inclusive end timestamp for order generation.
        customers_df (pd.DataFrame): All customers to sample from.
        products_df (pd.DataFrame): Full product catalog.
        existing_orders_df (pd.DataFrame): Historical orders for context.
        start_order_id (int): Initial order identifier.
        start_payment_id (int): Initial payment identifier.
        start_tracking_number (int): Initial tracking number identifier.
        orders_per_day (int | None): Optional override for order rate.

    Returns:
        tuple: (orders_df, order_items_df)
    """
    days = (end_date - start_date).days + 1
    # Use provided rate or default from settings
    rate = orders_per_day if orders_per_day is not None else ORDERS_PER_DAY
    num_orders = int(rate * days)

    section = "Order Generation (Date Range)"
    log_section_start(section)

    if num_orders == 0:
        log_section_complete(section, "No orders required for provided date range")
        return pd.DataFrame(), pd.DataFrame()

    products_df = products_df.copy()
    if "created_at_dt" not in products_df.columns:
        products_df["created_at_dt"] = pd.to_datetime(
            products_df["created_at"], errors="coerce"
        )
    else:
        products_df["created_at_dt"] = pd.to_datetime(
            products_df["created_at_dt"], errors="coerce"
        )

    # Assign VIP subscribers
    all_customer_ids = customers_df["customer_id"].tolist()
    num_vip_subscribers = int(len(all_customer_ids) * VIP_SUBSCRIPTION_RATE)
    vip_subscribers = set(random.sample(all_customer_ids, num_vip_subscribers))

    customers_dict = customers_df.set_index("customer_id").to_dict("index")
    products_dict = products_df.set_index("product_id").to_dict("index")

    signup_dates_cache = {}
    for customer_id, customer in customers_dict.items():
        signup_date_val = customer["signup_date"]
        # Handle both string and datetime/timestamp objects
        if isinstance(signup_date_val, str):
            signup_dates_cache[customer_id] = datetime.strptime(
                signup_date_val, "%Y-%m-%d %H:%M:%S"
            )
        elif isinstance(signup_date_val, datetime):
            signup_dates_cache[customer_id] = signup_date_val
        elif isinstance(signup_date_val, pd.Timestamp):
            signup_dates_cache[customer_id] = signup_date_val.to_pydatetime()
        else:
            # Try to parse as string
            signup_dates_cache[customer_id] = pd.to_datetime(
                signup_date_val
            ).to_pydatetime()

    customer_category_prefs = {}
    customer_order_history = defaultdict(list)

    # Build order history from existing orders
    if len(existing_orders_df) > 0:
        for _, order in existing_orders_df.iterrows():
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
            customer_order_history[customer_id].append((order_date, total_amount))

    orders = []
    order_items = []
    order_id = start_order_id
    order_item_id = start_order_item_id
    payment_id_counter = start_payment_id
    tracking_number_counter = start_tracking_number

    # Generate customer weights
    customer_weights = []
    customer_ids = []
    for customer_id in customers_dict.keys():
        signup_date = signup_dates_cache[customer_id]
        tenure_days = (end_date - signup_date).days
        weight = max(0.1, tenure_days / 365.0)
        customer_weights.append(weight)
        customer_ids.append(customer_id)

    customer_ids_array = np.array(customer_ids)
    customer_weights_array = np.array(customer_weights)
    customer_probs = customer_weights_array / customer_weights_array.sum()

    prev_pct = -1
    # Generate orders
    for i in range(num_orders):
        customer_id = np.random.choice(customer_ids_array, p=customer_probs)
        signup_date = signup_dates_cache[customer_id]
        customer = customers_dict[customer_id]
        country = customer["country"]

        # Generate order date within range
        days_offset = random.uniform(0, days - 1)
        order_date = start_date + timedelta(
            days=int(days_offset), hours=random.randint(0, 23)
        )

        payment_method = random.choices(
            list(PAYMENT_METHODS.keys()), weights=list(PAYMENT_METHODS.values())
        )[0]
        payment_status = _determine_payment_status()

        payment_id = None
        if payment_status in ["completed", "failed"]:
            payment_id = payment_id_counter
            payment_id_counter += 1

        # Select products
        items = list(ITEMS_PER_ORDER_DISTRIBUTION.keys())
        weights = list(ITEMS_PER_ORDER_DISTRIBUTION.values())
        num_items = random.choices(items, weights=weights)[0]

        selected_products = _select_products_with_preferences(
            products_df, customer_category_prefs, customer_id, num_items, order_date
        )

        # Update preferences
        for product_id in selected_products:
            category = products_dict[product_id]["category"]
            if customer_id not in customer_category_prefs:
                customer_category_prefs[customer_id] = defaultdict(int)
            customer_category_prefs[customer_id][category] += 1

        # Calculate subtotal
        subtotal = 0.0
        for product_id in selected_products:
            product = products_dict[product_id]
            quantity = random.randint(
                QUANTITY_PER_PRODUCT_MIN, QUANTITY_PER_PRODUCT_MAX
            )
            unit_price = product["price"]
            line_total = round(unit_price * quantity, 2)
            subtotal += line_total

            order_items.append(
                {
                    "order_item_id": order_item_id,
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_total": line_total,
                    "created_at": order_date.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            order_item_id += 1

        subtotal = round(subtotal, 2)

        # Determine segment
        segment = _determine_segment_at_order_time(
            customer_id,
            order_date,
            signup_date,
            customer_order_history.get(customer_id, []),
            vip_subscribers,
        )

        # Calculate costs
        discount_amount = _calculate_discount(subtotal, segment, order_id)
        order_number = len(customer_order_history.get(customer_id, [])) + 1
        shipping_cost = _calculate_shipping_cost(
            subtotal, segment, order_number, country
        )
        tax_amount = _calculate_tax(subtotal, discount_amount, country)
        total_amount = round(subtotal - discount_amount + tax_amount + shipping_cost, 2)

        # Determine order status
        order_status = _determine_order_status(order_date, payment_status, end_date)
        order_status, payment_status = _apply_refund_logic(
            order_status, payment_status, order_date
        )

        # Calculate timestamps
        payment_date, shipment_date, delivered_date = _calculate_timestamps(
            order_date, order_status, payment_status
        )

        # Generate tracking number if shipped
        tracking_number = None
        shipping_carrier = None
        if order_status in ["shipped", "delivered", "refunded"]:
            tracking_number = tracking_number_counter
            tracking_number_counter += 1
            shipping_carrier = random.choices(
                list(CARRIERS.keys()), weights=list(CARRIERS.values())
            )[0]

        order_date_str = order_date.strftime("%Y-%m-%d %H:%M:%S")
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date_str,
                "payment_date": payment_date.strftime("%Y-%m-%d %H:%M:%S")
                if payment_date
                else None,
                "shipment_date": shipment_date.strftime("%Y-%m-%d %H:%M:%S")
                if shipment_date
                else None,
                "delivered_date": delivered_date.strftime("%Y-%m-%d %H:%M:%S")
                if delivered_date
                else None,
                "created_at": order_date_str,
                "updated_at": order_date_str,
                "order_status": order_status,
                "payment_status": payment_status,
                "subtotal": subtotal,
                "discount_amount": discount_amount,
                "tax_amount": tax_amount,
                "shipping_cost": shipping_cost,
                "total_amount": total_amount,
                "payment_method": payment_method,
                "payment_id": payment_id,
                "shipping_carrier": shipping_carrier,
                "tracking_number": tracking_number,
                "customer_segment_at_order": segment,
            }
        )

        customer_order_history[customer_id].append((order_date, total_amount))
        order_id += 1

        pct = int(((i + 1) / num_orders) * 100)
        if pct != prev_pct or i == num_orders - 1:
            log_progress(section, f"{pct}% complete", end="\r", flush=True)
            prev_pct = pct

    clear_progress_line(section)
    log_section_complete(
        section,
        f"{len(orders):,} orders and {len(order_items):,} order items generated",
    )
    return pd.DataFrame(orders), pd.DataFrame(order_items)


def update_existing_orders(
    existing_orders_df: pd.DataFrame, current_date: datetime
) -> pd.DataFrame:
    """
    Update existing orders with status transitions based on time elapsed

    Args:
        existing_orders_df: DataFrame with existing orders
        current_date: Current date for status calculations

    Returns:
        DataFrame with updated orders
    """
    section = "Existing Order Status Updates"
    log_section_start(section)

    if len(existing_orders_df) == 0:
        log_section_complete(section, "No orders were supplied for updates")
        return pd.DataFrame()

    updated_orders = []
    total = len(existing_orders_df)
    prev_pct = -1

    for idx, (_, order) in enumerate(existing_orders_df.iterrows(), 1):
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
        current_status = order["order_status"]
        current_payment_status = order["payment_status"]

        # Skip if already in final state
        if current_status in ["cancelled", "refunded"]:
            updated_orders.append(order.to_dict())
            continue

        days_since_order = (current_date - order_date).days

        # Status transitions
        new_status = current_status
        new_payment_status = current_payment_status
        shipment_date_added = False
        delivered_date_added = False

        if current_status == "processing":
            if days_since_order >= AVG_PROCESSING_TIME:
                new_status = "shipped"
                if pd.isna(order.get("shipment_date")):
                    shipment_date = order_date + timedelta(
                        days=AVG_PROCESSING_TIME, hours=random.randint(0, 23)
                    )
                    order["shipment_date"] = shipment_date.strftime("%Y-%m-%d %H:%M:%S")
                    shipment_date_added = True

        elif current_status == "shipped":
            if (
                days_since_order
                >= AVG_PROCESSING_TIME + AVG_SHIPPING_TIME + AVG_DELIVERY_TIME
            ):
                new_status = "delivered"
                if pd.isna(order.get("delivered_date")):
                    delivered_date = order_date + timedelta(
                        days=AVG_PROCESSING_TIME
                        + AVG_SHIPPING_TIME
                        + AVG_DELIVERY_TIME,
                        hours=random.randint(0, 23),
                    )
                    order["delivered_date"] = delivered_date.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    delivered_date_added = True

        elif current_status == "delivered":
            # Check for refunds
            if current_payment_status == "completed":
                delivered_date_val = order.get("delivered_date")
                if delivered_date_val and not pd.isna(delivered_date_val):
                    # Handle both string and datetime/timestamp objects
                    if isinstance(delivered_date_val, str):
                        delivered_date = datetime.strptime(
                            delivered_date_val, "%Y-%m-%d %H:%M:%S"
                        )
                    elif isinstance(delivered_date_val, datetime):
                        delivered_date = delivered_date_val
                    elif isinstance(delivered_date_val, pd.Timestamp):
                        delivered_date = delivered_date_val.to_pydatetime()
                    else:
                        delivered_date = pd.to_datetime(
                            delivered_date_val
                        ).to_pydatetime()
                    days_since_delivery = (current_date - delivered_date).days
                    # Refund can happen 5-30 days after delivery
                    if days_since_delivery >= 5 and random.random() < REFUND_RATE:
                        new_status = "refunded"
                        new_payment_status = "refunded"

        # Check if anything actually changed
        status_changed = new_status != current_status
        payment_status_changed = new_payment_status != current_payment_status
        anything_changed = (
            status_changed
            or payment_status_changed
            or shipment_date_added
            or delivered_date_added
        )

        # Update order
        order_dict = order.to_dict()
        order_dict["order_status"] = new_status
        order_dict["payment_status"] = new_payment_status

        # Only update updated_at if something actually changed
        if anything_changed:
            order_dict["updated_at"] = current_date.strftime("%Y-%m-%d %H:%M:%S")
        # Otherwise keep the original updated_at (don't modify it)

        updated_orders.append(order_dict)

        # Log progress every 10% for large datasets
        if total > 100 and idx % max(1, total // 10) == 0:
            pct = int((idx / total) * 100)
            log_progress(
                section,
                f"{pct}% ({idx:,}/{total:,}) complete",
                end="\r",
                flush=True,
            )
            prev_pct = pct

    if total > 100 and prev_pct >= 0:
        clear_progress_line(section)

    log_section_complete(section, f"{len(updated_orders):,} orders evaluated")
    return pd.DataFrame(updated_orders)
