"""Tests for the dead letter publisher."""

from __future__ import annotations

from src.common.errors import PermanentError
from src.common.headers import decode
from src.config import settings
from src.consumer.dlq import DeadLetterPublisher


class FakeMessage:
    """Minimal stand-in for confluent_kafka.Message."""

    def __init__(self, *, topic="orders", partition=0, offset=0, key=b"1001", value=b"\x00raw"):
        self._topic, self._partition = topic, partition
        self._offset, self._key, self._value = offset, key, value

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._value


class FakeProducer:
    def __init__(self):
        self.produced = []

    def produce(self, **kwargs):
        self.produced.append(kwargs)

    def poll(self, _timeout):
        pass

    def flush(self, _timeout=10.0):
        return 0


def _publish(message, error=None, attempts=0):
    producer = FakeProducer()
    publisher = DeadLetterPublisher(producer, settings)
    publisher.publish(message, error or PermanentError("bad price"), attempts=attempts)
    return producer.produced[0]


def test_offset_zero_is_preserved_not_coerced_to_minus_one():
    """Offset 0 is falsy -- `or -1` here would make the first message of every
    partition unreplayable. This is a regression guard, not a style check."""
    record = _publish(FakeMessage(partition=0, offset=0))
    headers = decode(record["headers"])

    assert headers["x-original-offset"] == "0"
    assert headers["x-original-partition"] == "0"


def test_original_bytes_are_forwarded_untouched():
    raw = b"\x00\x00\x00\x00\x01some-avro-bytes"
    record = _publish(FakeMessage(value=raw))
    assert record["value"] == raw
    assert record["topic"] == settings.topic_dlq


def test_key_is_preserved_so_ordering_survives_replay():
    record = _publish(FakeMessage(key=b"1042"))
    assert record["key"] == b"1042"


def test_error_context_is_recorded():
    record = _publish(
        FakeMessage(partition=2, offset=57),
        error=PermanentError("order 1001 has a non-positive price (-12.00)"),
        attempts=3,
    )
    headers = decode(record["headers"])

    assert headers["x-error-type"] == "PermanentError"
    assert "non-positive price" in headers["x-error-message"]
    assert headers["x-retry-attempts"] == "3"
    assert headers["x-original-partition"] == "2"
    assert headers["x-original-offset"] == "57"
    assert headers["x-consumer-group"] == settings.consumer_group


def test_publisher_counts_what_it_dead_letters():
    producer = FakeProducer()
    publisher = DeadLetterPublisher(producer, settings)
    for i in range(4):
        publisher.publish(FakeMessage(offset=i), PermanentError("nope"), attempts=0)
    assert publisher.published == 4
