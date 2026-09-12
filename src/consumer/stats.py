"""Counters describing what the consumer has done to the stream so far."""

from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class PipelineStats:
    consumed: int = 0
    processed: int = 0
    retried: int = 0
    recovered: int = 0
    dlq_invalid: int = 0
    dlq_exhausted: int = 0
    dlq_undeserializable: int = 0
    started_at: float = 0.0

    def __post_init__(self) -> None:
        self.started_at = time.monotonic()

    @property
    def dlq_total(self) -> int:
        return self.dlq_invalid + self.dlq_exhausted + self.dlq_undeserializable

    @property
    def uptime_seconds(self) -> float:
        return time.monotonic() - self.started_at

    @property
    def throughput(self) -> float:
        elapsed = self.uptime_seconds
        return self.consumed / elapsed if elapsed > 0 else 0.0

    @property
    def success_rate(self) -> float:
        return (self.processed / self.consumed * 100) if self.consumed else 0.0
