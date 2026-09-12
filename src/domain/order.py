"""The Order domain object and its mapping to/from the Avro record."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Order:
    """Mirrors schemas/order.avsc exactly."""

    orderId: str
    product: str
    price: float

    def to_dict(self) -> dict[str, Any]:
        return {"orderId": self.orderId, "product": self.product, "price": self.price}

    @staticmethod
    def from_dict(payload: dict[str, Any], _ctx: Any = None) -> "Order":
        return Order(
            orderId=payload["orderId"],
            product=payload["product"],
            price=payload["price"],
        )

    def __str__(self) -> str:
        return f"Order({self.orderId}, {self.product}, {self.price:.2f})"
