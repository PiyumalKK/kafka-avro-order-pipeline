"""Real-time aggregation: a running average of order prices.

Uses Welford's online algorithm rather than keeping a running sum. Both give
the same mean, but Welford stays numerically stable over a long-lived stream
and gives us variance for free -- which matters when a consumer is meant to
run for days without restarting.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

from src.domain.order import Order


@dataclass
class RunningStats:
    """Online mean/variance for one group of prices."""

    count: int = 0
    mean: float = 0.0
    _m2: float = 0.0
    minimum: float = math.inf
    maximum: float = -math.inf

    def update(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / self.count
        self._m2 += delta * (value - self.mean)
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)

    @property
    def total(self) -> float:
        return self.mean * self.count

    @property
    def variance(self) -> float:
        return self._m2 / (self.count - 1) if self.count > 1 else 0.0

    @property
    def std_dev(self) -> float:
        return math.sqrt(self.variance)


@dataclass
class OrderAggregator:
    """Maintains the global running average plus a per-product breakdown."""

    overall: RunningStats = field(default_factory=RunningStats)
    by_product: dict[str, RunningStats] = field(default_factory=dict)

    def add(self, order: Order) -> float:
        """Fold one order into the aggregate and return the new running average."""
        self.overall.update(order.price)
        self.by_product.setdefault(order.product, RunningStats()).update(order.price)
        return self.overall.mean

    @property
    def running_average(self) -> float:
        return self.overall.mean

    @property
    def processed_count(self) -> int:
        return self.overall.count

    def snapshot(self) -> dict[str, object]:
        """A plain dict for logging, dashboards, or an HTTP endpoint."""
        return {
            "count": self.overall.count,
            "running_average": round(self.overall.mean, 2),
            "total_value": round(self.overall.total, 2),
            "min": round(self.overall.minimum, 2) if self.overall.count else None,
            "max": round(self.overall.maximum, 2) if self.overall.count else None,
            "std_dev": round(self.overall.std_dev, 2),
            "by_product": {
                product: {"count": s.count, "avg": round(s.mean, 2)}
                for product, s in sorted(self.by_product.items())
            },
        }
