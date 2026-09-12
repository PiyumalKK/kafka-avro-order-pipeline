import pytest

from src.common.errors import PermanentError, RetriesExhausted, TransientError
from src.processing.retry import RetryPolicy

FAST = RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0, jitter=False)


def test_succeeds_without_retrying():
    calls = []

    def op():
        calls.append(1)
        return "ok"

    assert FAST.run(op) == "ok"
    assert len(calls) == 1


def test_recovers_after_transient_failures():
    calls = []

    def op():
        calls.append(1)
        if len(calls) < 3:
            raise TransientError("downstream busy")
        return "ok"

    assert FAST.run(op) == "ok"
    assert len(calls) == 3


def test_gives_up_after_max_attempts():
    calls = []

    def op():
        calls.append(1)
        raise TransientError("still down")

    with pytest.raises(RetriesExhausted) as exc_info:
        FAST.run(op)

    assert len(calls) == 3
    assert exc_info.value.attempts == 3
    assert isinstance(exc_info.value.last_error, TransientError)


def test_permanent_error_is_not_retried():
    """Retrying invalid data just wastes the budget -- it must fail fast."""
    calls = []

    def op():
        calls.append(1)
        raise PermanentError("price is negative")

    with pytest.raises(PermanentError):
        FAST.run(op)

    assert len(calls) == 1


def test_backoff_grows_exponentially_and_is_capped():
    policy = RetryPolicy(max_attempts=6, base_delay=1.0, max_delay=4.0, jitter=False)
    assert [policy.delay_for(n) for n in range(1, 6)] == [1.0, 2.0, 4.0, 4.0, 4.0]


def test_jitter_stays_within_half_the_nominal_delay():
    policy = RetryPolicy(max_attempts=3, base_delay=2.0, max_delay=10.0, jitter=True)
    delays = [policy.delay_for(1) for _ in range(200)]
    assert all(1.0 <= d <= 2.0 for d in delays)
    assert len(set(delays)) > 1  # actually randomised
