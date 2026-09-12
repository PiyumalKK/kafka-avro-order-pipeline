import math

import pytest

from src.common.errors import PermanentError
from src.domain.order import Order
from src.processing.validation import validate


def test_valid_order_passes():
    validate(Order("1001", "Item1", 250.0))


@pytest.mark.parametrize(
    "order",
    [
        Order("", "Item1", 100.0),
        Order("   ", "Item1", 100.0),
        Order("1001", "", 100.0),
        Order("1001", "   ", 100.0),
        Order("1001", "Item1", 0.0),
        Order("1001", "Item1", -50.0),
        Order("1001", "Item1", 9_999_999.0),
        Order("1001", "Item1", float("nan")),
        Order("1001", "Item1", float("inf")),
    ],
)
def test_invalid_orders_raise_permanent_error(order):
    with pytest.raises(PermanentError):
        validate(order)


def test_error_message_names_the_order():
    with pytest.raises(PermanentError, match="1001"):
        validate(Order("1001", "Item1", -1.0))


def test_boundary_price_is_accepted():
    validate(Order("1001", "Item1", 1_000_000.0))
    assert not math.isnan(1_000_000.0)
