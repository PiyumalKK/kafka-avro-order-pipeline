"""Retry policy for transient failures: exponential backoff with jitter."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Callable, TypeVar

from src.common.errors import PermanentError, RetriesExhausted, TransientError

T = TypeVar("T")
log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded exponential backoff.

    Delay before attempt n is base * 2**(n-1), capped at max_delay, then
    multiplied by a random factor in [0.5, 1.0). The jitter stops a batch of
    consumers that failed together from retrying in lockstep and hammering an
    already-struggling downstream service.
    """

    max_attempts: int = 3
    base_delay: float = 0.4
    max_delay: float = 6.0
    jitter: bool = True

    def delay_for(self, attempt: int) -> float:
        """Seconds to wait *after* a failed attempt (1-indexed)."""
        raw = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
        return raw * random.uniform(0.5, 1.0) if self.jitter else raw

    def run(self, operation: Callable[[], T], *, description: str = "operation") -> T:
        """Call operation, retrying only on TransientError.

        PermanentError passes straight through untouched -- retrying it would
        just burn the budget on a message that can never succeed.
        """
        last_error: Exception | None = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                return operation()
            except PermanentError:
                raise
            except TransientError as exc:
                last_error = exc
                if attempt == self.max_attempts:
                    break
                pause = self.delay_for(attempt)
                log.warning(
                    "%s failed (attempt %d/%d): %s -- retrying in %.2fs",
                    description, attempt, self.max_attempts, exc, pause,
                )
                time.sleep(pause)

        assert last_error is not None
        raise RetriesExhausted(self.max_attempts, last_error)
