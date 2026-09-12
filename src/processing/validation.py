"""Business rules an order must satisfy before it can be aggregated."""

from __future__ import annotations

import math

from src.common.errors import PermanentError
from src.domain.order import Order

MAX_REASONABLE_PRICE = 1_000_000.0


def validate(order: Order) -> None:
    """Raise PermanentError if the order can never be processed.

    These are content faults: no amount of retrying turns a negative price
    into a valid one, so every failure here is a one-way trip to the DLQ.
    """
    if not order.orderId or not order.orderId.strip():
        raise PermanentError("orderId is empty")

    if not order.product or not order.product.strip():
        raise PermanentError(f"order {order.orderId} has an empty product name")

    price = order.price
    if price is None or math.isnan(price) or math.isinf(price):
        raise PermanentError(f"order {order.orderId} has a non-numeric price ({price})")

    if price <= 0:
        raise PermanentError(f"order {order.orderId} has a non-positive price ({price:.2f})")

    if price > MAX_REASONABLE_PRICE:
        raise PermanentError(
            f"order {order.orderId} price {price:.2f} exceeds the {MAX_REASONABLE_PRICE:,.0f} ceiling"
        )
