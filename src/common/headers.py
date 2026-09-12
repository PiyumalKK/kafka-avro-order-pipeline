"""Kafka record headers used to carry failure context to the DLQ.

A dead-lettered message is useless without provenance: whoever inspects the
DLQ needs to know where the message came from, what broke, and how hard we
tried before giving up.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Sequence

KafkaHeaders = Sequence[tuple[str, bytes]]


def build_dlq_headers(
    *,
    source_topic: str,
    partition: int,
    offset: int,
    error_type: str,
    error_message: str,
    attempts: int,
    consumer_group: str,
) -> list[tuple[str, bytes]]:
    return [
        ("x-original-topic", source_topic.encode()),
        ("x-original-partition", str(partition).encode()),
        ("x-original-offset", str(offset).encode()),
        ("x-error-type", error_type.encode()),
        ("x-error-message", error_message[:900].encode("utf-8", errors="replace")),
        ("x-retry-attempts", str(attempts).encode()),
        ("x-consumer-group", consumer_group.encode()),
        ("x-failed-at", datetime.now(timezone.utc).isoformat().encode()),
    ]


def decode(headers: Iterable[tuple[str, bytes]] | None) -> dict[str, str]:
    """Turn raw Kafka headers into a readable dict (for logs and the DLQ viewer)."""
    if not headers:
        return {}
    return {
        key: (value.decode("utf-8", errors="replace") if value is not None else "")
        for key, value in headers
    }
