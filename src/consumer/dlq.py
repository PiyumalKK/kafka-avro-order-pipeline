"""Dead Letter Queue publisher.

Two rules make a DLQ genuinely useful rather than a bin of mystery bytes:

1. Forward the *original* payload verbatim. If the value failed to
   deserialise, re-encoding it is impossible -- and re-encoding a message
   that did deserialise would quietly hide the bytes that caused the problem.
2. Attach the provenance and the error as headers, so the message can be
   diagnosed and replayed without cross-referencing consumer logs.
"""

from __future__ import annotations

import logging

from confluent_kafka import Message, Producer

from src.common.headers import build_dlq_headers
from src.config import Settings

log = logging.getLogger("dlq")


class DeadLetterPublisher:
    def __init__(self, producer: Producer, settings: Settings) -> None:
        self._producer = producer
        self._settings = settings
        self.published = 0

    def publish(self, message: Message, error: Exception, *, attempts: int) -> None:
        headers = build_dlq_headers(
            source_topic=message.topic() or self._settings.topic_orders,
            partition=message.partition() or 0,
            offset=message.offset() or -1,
            error_type=type(error).__name__,
            error_message=str(error),
            attempts=attempts,
            consumer_group=self._settings.consumer_group,
        )

        self._producer.produce(
            topic=self._settings.topic_dlq,
            key=message.key(),
            value=message.value(),   # untouched original bytes
            headers=headers,
        )
        self._producer.poll(0)
        self.published += 1

        log.error(
            "DLQ <- offset %s after %d attempt(s): %s: %s",
            message.offset(), attempts, type(error).__name__, error,
        )

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)
