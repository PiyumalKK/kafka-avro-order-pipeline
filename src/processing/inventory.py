"""A stand-in for the downstream service an order consumer would really call.

Real pipelines fail at the edges -- an inventory API times out, a payment
gateway rate-limits, a database deadlocks. None of that happens on a laptop,
so this module fakes it deterministically enough to demo but randomly enough
to look real. It is the *only* source of transient faults in the system.
"""

from __future__ import annotations

import logging
import random

from src.common.errors import TransientError
from src.domain.order import Order

log = logging.getLogger(__name__)

# Orders for these products always hit a flaky code path, so a live demo can
# reliably show the retry machinery instead of waiting for luck.
FLAKY_PRODUCTS = {"Item7", "Item8"}


class InventoryService:
    """Simulated reservation call with a configurable failure rate."""

    def __init__(self, failure_rate: float = 0.15, flaky_failure_rate: float = 0.6) -> None:
        self.failure_rate = failure_rate
        self.flaky_failure_rate = flaky_failure_rate
        self.calls = 0
        self.failures = 0

    def reserve(self, order: Order) -> str:
        """Reserve stock for an order, or raise TransientError trying."""
        self.calls += 1
        rate = self.flaky_failure_rate if order.product in FLAKY_PRODUCTS else self.failure_rate

        if random.random() < rate:
            self.failures += 1
            raise TransientError(
                f"inventory service unavailable while reserving {order.product} "
                f"for order {order.orderId}"
            )

        return f"RES-{order.orderId}"
