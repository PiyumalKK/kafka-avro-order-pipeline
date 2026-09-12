import math

from src.domain.order import Order
from src.processing.aggregator import OrderAggregator, RunningStats


def test_running_average_matches_arithmetic_mean():
    stats = RunningStats()
    values = [10.0, 20.0, 30.0, 40.0]
    for v in values:
        stats.update(v)
    assert stats.count == 4
    assert math.isclose(stats.mean, sum(values) / len(values))
    assert math.isclose(stats.total, sum(values))


def test_running_average_updates_incrementally():
    agg = OrderAggregator()
    assert agg.running_average == 0.0
    assert math.isclose(agg.add(Order("1", "Item1", 100.0)), 100.0)
    assert math.isclose(agg.add(Order("2", "Item1", 200.0)), 150.0)
    assert math.isclose(agg.add(Order("3", "Item2", 300.0)), 200.0)
    assert agg.processed_count == 3


def test_per_product_breakdown_is_independent():
    agg = OrderAggregator()
    agg.add(Order("1", "Item1", 100.0))
    agg.add(Order("2", "Item2", 500.0))
    agg.add(Order("3", "Item1", 200.0))

    assert math.isclose(agg.by_product["Item1"].mean, 150.0)
    assert math.isclose(agg.by_product["Item2"].mean, 500.0)
    assert agg.by_product["Item1"].count == 2


def test_min_max_and_std_dev():
    agg = OrderAggregator()
    for price in (10.0, 20.0, 30.0):
        agg.add(Order("x", "Item1", price))
    assert agg.overall.minimum == 10.0
    assert agg.overall.maximum == 30.0
    assert math.isclose(agg.overall.std_dev, 10.0)


def test_welford_is_stable_over_a_long_stream():
    """A naive running sum drifts with large offsets; Welford should not."""
    agg = OrderAggregator()
    base = 1_000_000.0
    for i in range(10_000):
        agg.add(Order(str(i), "Item1", base + (i % 2)))
    assert math.isclose(agg.running_average, base + 0.5, rel_tol=1e-9)


def test_snapshot_is_json_friendly():
    agg = OrderAggregator()
    agg.add(Order("1", "Item1", 99.5))
    snap = agg.snapshot()
    assert snap["count"] == 1
    assert snap["running_average"] == 99.5
    assert snap["by_product"]["Item1"]["count"] == 1
