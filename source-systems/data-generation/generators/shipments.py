"""
Generates realistic shipment data from orders
"""

from config.settings import AVG_DELIVERY_TIME
from datetime import datetime, timedelta
from typing import Any, Optional
import pandas as pd
import random
import hashlib
from utils.logging_utils import (
    log_section_start,
    log_section_complete,
    log_progress,
    clear_progress_line,
)


# Warehouse locations by region (for origin country only)
WAREHOUSE_LOCATIONS = {
    "United States": "United States",
    "Canada": "Canada",
    "United Kingdom": "United Kingdom",
    "Germany": "Germany",
    "France": "France",
    "Italy": "Italy",
    "Spain": "Spain",
    "Japan": "Japan",
    "Netherlands": "Netherlands",
    "Australia": "Australia",
}


def _generate_tracking_number(carrier, tracking_number_id):
    """
    Generate a realistic tracking number string based on carrier format.

    Args:
        carrier (str): Shipping carrier name.
        tracking_number_id (int): Internal numeric identifier for the shipment.

    Returns:
        str: Display-friendly tracking identifier.
    """
    # Create a hash-based tracking number
    key = f"{carrier}|{tracking_number_id}"
    hash_value = hashlib.md5(key.encode()).hexdigest()[:12].upper()

    # Format based on carrier
    if carrier == "UPS":
        return f"1Z{hash_value[:6]}{hash_value[6:]}"
    elif carrier == "FedEx":
        return f"{hash_value[:4]} {hash_value[4:8]} {hash_value[8:]}"
    elif carrier == "USPS":
        return f"{hash_value[:4]} {hash_value[4:8]} {hash_value[8:]}"
    elif carrier == "DHL":
        return f"{hash_value[:4]}{hash_value[4:8]}{hash_value[8:]}"
    else:
        return hash_value


def _calculate_shipment_status(shipment_date, delivered_date, current_date):
    """
    Determine shipment status based on shipment, delivery, and current dates.
    Exception status represents delivery issues (lost packages, wrong address, damaged goods, etc.)

    Args:
        shipment_date (datetime | None): Date the shipment left the warehouse.
        delivered_date (datetime | None): Date the order was delivered.
        current_date (datetime): Current timestamp used for transition logic.

    Returns:
        str: Shipment status value.
    """
    if delivered_date:
        # Delivered shipments are always 'delivered' (even if late)
        return "delivered"
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
            return "exception"

        return "in_transit"
    else:
        return "pending"


def _parse_datetime(value: Any) -> Optional[datetime]:
    """
    Parse various datetime representations into a datetime object or None.

    Args:
        value: Candidate datetime value (str, datetime, Timestamp, or other).

    Returns:
        Optional[datetime]: Parsed datetime or None if input is missing.
    """
    if pd.notna(value):
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        if isinstance(value, datetime):
            return value
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        return pd.to_datetime(value).to_pydatetime()
    return None


def generate_shipments(
    orders_df: pd.DataFrame,
    order_items_df: pd.DataFrame | None = None,
    customers_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Generate shipment records from orders data

    Args:
        orders_df (pd.DataFrame): Order data with shipment-related fields
            (tracking_number, carrier, shipment/delivery dates, shipping_cost, order_id, customer_id).
        order_items_df (pd.DataFrame | None): Optional order item details (unused placeholder).
        customers_df (pd.DataFrame | None): Optional customer dataset for postal code lookups.

    Returns:
        pd.DataFrame: Shipment records derived from the input orders.
    """
    if orders_df is None or len(orders_df) == 0:
        raise ValueError("orders_df is required and must not be empty")

    section = "Shipment Generation"
    log_section_start(section)
    # Filter orders that have tracking_number (shipped, delivered, or refunded)
    orders_with_shipment = orders_df[orders_df["tracking_number"].notna()].copy()

    if len(orders_with_shipment) == 0:
        log_section_complete(section, "No shipments required for supplied orders")
        return pd.DataFrame()

    # Create lookup for customer postal codes if provided
    customer_data = {}
    if customers_df is not None:
        for _, customer in customers_df.iterrows():
            customer_data[customer["customer_id"]] = {
                "postal_code": customer.get("postal_code", ""),
                "country": customer.get("country", ""),
            }

    shipments = []
    # Use current time for shipment status calculations (not END_DATE)
    current_date = datetime.now()

    total = len(orders_with_shipment)
    prev_pct = -1
    for idx, (_, order) in enumerate(orders_with_shipment.iterrows(), 1):
        tracking_number_id = int(order["tracking_number"])
        order_id = order["order_id"]
        customer_id = order["customer_id"]
        shipping_carrier = order["shipping_carrier"]
        shipping_cost = order["shipping_cost"]
        shipment_date_str = order.get("shipment_date")
        delivered_date_str = order.get("delivered_date")

        # Parse dates (handle string, datetime, or Timestamp)
        shipment_date = _parse_datetime(shipment_date_str)
        delivered_date = _parse_datetime(delivered_date_str)

        # Generate tracking number
        tracking_number = _generate_tracking_number(
            shipping_carrier, tracking_number_id
        )

        # Get origin country (warehouse) based on customer country
        customer_country = None
        destination_postal_code = None
        if customer_id in customer_data:
            customer_country = customer_data[customer_id]["country"]
            destination_postal_code = customer_data[customer_id]["postal_code"]
        else:
            # Fallback: use US warehouse
            customer_country = "United States"
            destination_postal_code = "00000"

        origin_country = WAREHOUSE_LOCATIONS.get(customer_country, "United States")
        destination_country = customer_country or "United States"

        # Calculate estimated delivery date
        estimated_delivery_date = None
        if shipment_date:
            estimated_delivery_date = shipment_date + timedelta(
                days=AVG_DELIVERY_TIME + random.randint(-1, 2)
            )

        # Determine shipment status
        shipment_status = _calculate_shipment_status(
            shipment_date, delivered_date, current_date
        )

        # Calculate updated-at based on shipment status
        if shipment_status == "delivered" and delivered_date:
            # Delivered: use actual_delivery_date
            updated_at = (
                delivered_date.strftime("%Y-%m-%d %H:%M:%S") if delivered_date else None
            )
        elif shipment_status == "exception" and estimated_delivery_date:
            # Exception: detected after estimated delivery date
            days_overdue = max(1, (current_date - estimated_delivery_date).days)
            exception_detection_days = random.randint(1, min(7, days_overdue))
            updated_at = (
                estimated_delivery_date + timedelta(days=exception_detection_days)
            ).strftime("%Y-%m-%d %H:%M:%S")
        elif shipment_date:
            # In transit or pending: use shipment_date
            updated_at = shipment_date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            # No shipment date yet
            updated_at = None

        shipment = {
            "tracking_number": tracking_number_id,
            "tracking_code": tracking_number,  # Human-readable tracking number
            "order_id": order_id,
            "shipping_carrier": shipping_carrier,
            "origin_country": origin_country,
            "destination_country": destination_country,
            "destination_postal_code": destination_postal_code,
            "status": shipment_status,
            "shipping_cost": shipping_cost,
            "shipment-date": shipment_date.strftime("%Y-%m-%d %H:%M:%S")
            if shipment_date
            else None,
            "estimated-delivery-date": estimated_delivery_date.strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            if estimated_delivery_date
            else None,
            "actual-delivery-date": delivered_date.strftime("%Y-%m-%d %H:%M:%S")
            if delivered_date
            else None,
            "created-at": shipment_date.strftime("%Y-%m-%d %H:%M:%S")
            if shipment_date
            else None,
            "updated-at": updated_at,
        }

        shipments.append(shipment)

        # Update progress every 1%
        pct = int((idx / total) * 100)
        if pct != prev_pct or idx == total:
            log_progress(section, f"{pct}% complete", end="\r", flush=True)
            prev_pct = pct

    if total > 0:
        clear_progress_line(section)
    log_section_complete(section, f"{len(shipments):,} shipments generated")
    return pd.DataFrame(shipments)
