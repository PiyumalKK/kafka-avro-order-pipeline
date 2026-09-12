from src.common.headers import build_dlq_headers, decode


def test_dlq_headers_capture_full_provenance():
    headers = build_dlq_headers(
        source_topic="orders",
        partition=2,
        offset=57,
        error_type="PermanentError",
        error_message="order 1001 has a non-positive price (-12.00)",
        attempts=3,
        consumer_group="order-processor",
    )
    decoded = decode(headers)

    assert decoded["x-original-topic"] == "orders"
    assert decoded["x-original-partition"] == "2"
    assert decoded["x-original-offset"] == "57"
    assert decoded["x-error-type"] == "PermanentError"
    assert decoded["x-retry-attempts"] == "3"
    assert decoded["x-consumer-group"] == "order-processor"
    assert "non-positive price" in decoded["x-error-message"]
    assert decoded["x-failed-at"]


def test_long_error_messages_are_truncated():
    headers = build_dlq_headers(
        source_topic="orders", partition=0, offset=1,
        error_type="TransientError", error_message="x" * 5000,
        attempts=1, consumer_group="g",
    )
    assert len(decode(headers)["x-error-message"]) <= 900


def test_decode_handles_missing_headers():
    assert decode(None) == {}
    assert decode([]) == {}
