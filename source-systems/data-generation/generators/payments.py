"""
Generates realistic payment data from orders
"""

from config.settings import PAYMENT_METHODS, CARD_BRANDS, PAYMENT_STATUSES, REFUND_RATE
from datetime import datetime, timedelta
from typing import Any, Optional
import pandas as pd
import random
from utils.logging_utils import (
    log_section_start,
    log_section_complete,
    log_progress,
    clear_progress_line,
)


def _generate_card_last_4() -> str:
    """
    Generate the last four digits of a card number.

    Returns:
        str: Randomly generated four-digit string.
    """
    return str(random.randint(1000, 9999))


def _normalize_timestamp(value: Any, fallback: Optional[Any]) -> datetime:
    """
    Normalize string/timestamp inputs to a datetime, falling back to order_date if needed.

    Args:
        value: Candidate timestamp (str, datetime, Timestamp, or None).
        fallback: Fallback value (order_date) when primary value is missing.

    Returns:
        datetime: Normalized timestamp.
    """
    if pd.notna(value):
        if isinstance(value, str):
            return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        if isinstance(value, datetime):
            return value
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        return pd.to_datetime(value).to_pydatetime()
    if pd.notna(fallback):
        return _normalize_timestamp(fallback, None)
    raise ValueError("Unable to determine timestamp for payment")


def generate_payments(orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Generate payment records from orders data

    Args:
        orders_df (pd.DataFrame): Order data (requires payment_id, payment_method,
            payment_status, total_amount, payment_date, customer_id, order_id).

    Returns:
        pd.DataFrame: Payment records inferred from orders.
    """
    if orders_df is None or len(orders_df) == 0:
        raise ValueError("orders_df is required and must not be empty")

    section = "Payment Generation"
    log_section_start(section)
    # Filter orders that have payment_id (completed or failed payments)
    orders_with_payment = orders_df[orders_df["payment_id"].notna()].copy()

    if len(orders_with_payment) == 0:
        log_section_complete(section, "No orders required payment generation")
        return pd.DataFrame()

    payments = []

    total = len(orders_with_payment)
    prev_pct = -1
    for idx, (_, order) in enumerate(orders_with_payment.iterrows(), 1):
        payment_id = int(order["payment_id"])
        payment_method = order["payment_method"]
        payment_status = order["payment_status"]
        amount = order["total_amount"]
        payment_date_val = order["payment_date"]
        customer_id = order["customer_id"]
        order_id = order["order_id"]
        delivered_date_val = order.get("delivered_date")
        order_date_val = order.get("order_date")

        payment_date = _normalize_timestamp(payment_date_val, order_date_val)
        payment_date_str = payment_date.strftime("%Y-%m-%d %H:%M:%S")

        # Determine card_brand if payment_method is credit_card
        card_brand = None
        card_last_4 = None
        if payment_method == "credit_card":
            card_brand = random.choices(
                list(CARD_BRANDS.keys()), weights=list(CARD_BRANDS.values())
            )[0]
            card_last_4 = _generate_card_last_4()

        # Check if payment should be refunded (based on REFUND_RATE)
        # This applies to completed payments that are part of refunded orders
        final_payment_status = payment_status
        if payment_status == "completed" and order.get("order_status") == "refunded":
            if random.random() < REFUND_RATE:
                final_payment_status = "refunded"

        # Calculate created-at and updated-at timestamps
        created_at = payment_date_str

        # Calculate updated-at based on payment status
        if (
            final_payment_status == "refunded"
            and delivered_date_val
            and pd.notna(delivered_date_val)
        ):
            # Refund processed 5-30 days after delivery
            # Handle delivered_date (can be string, datetime, or Timestamp)
            if isinstance(delivered_date_val, str):
                delivered_date = datetime.strptime(
                    delivered_date_val, "%Y-%m-%d %H:%M:%S"
                )
            elif isinstance(delivered_date_val, datetime):
                delivered_date = delivered_date_val
            elif isinstance(delivered_date_val, pd.Timestamp):
                delivered_date = delivered_date_val.to_pydatetime()
            else:
                delivered_date = pd.to_datetime(delivered_date_val).to_pydatetime()

            refund_days = random.randint(5, 30)
            updated_at = (delivered_date + timedelta(days=refund_days)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        else:
            # For completed or failed, updated_at same as payment_date
            updated_at = payment_date_str

        payment = {
            "payment-id": payment_id,
            "order-id": order_id,
            "customer-id": customer_id,
            "payment-method": payment_method,
            "card-brand": card_brand,
            "card-last-4": card_last_4,
            "amount": amount,
            "payment-status": final_payment_status,
            "payment-date": payment_date_str,
            "created-at": created_at,
            "updated-at": updated_at,
        }

        payments.append(payment)

        # Update progress every 1%
        pct = int((idx / total) * 100)
        if pct != prev_pct or idx == total:
            log_progress(section, f"{pct}% complete", end="\r", flush=True)
            prev_pct = pct

    if total > 0:
        clear_progress_line(section)
    log_section_complete(section, f"{len(payments):,} payments generated")
    return pd.DataFrame(payments)
