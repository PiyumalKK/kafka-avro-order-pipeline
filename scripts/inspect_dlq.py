"""Read the Dead Letter Queue and explain why each message landed there.

Run this during the demo right after the pipeline has been left to fail for a
while -- it turns the DLQ from an opaque topic into an audit trail.

    python scripts/inspect_dlq.py
    python scripts/inspect_dlq.py --follow
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka import Consumer, KafkaError  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402
from rich.console import Console  # noqa: E402
from rich.table import Table  # noqa: E402

from src.common.headers import decode  # noqa: E402
from src.common.kafka_clients import build_order_deserializer  # noqa: E402
from src.config import settings  # noqa: E402

console = Console()


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect the DLQ topic.")
    parser.add_argument("--follow", action="store_true", help="keep tailing instead of exiting at the end")
    parser.add_argument("--timeout", type=float, default=5.0, help="seconds of silence before giving up")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": "dlq-inspector",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([settings.topic_dlq])
    deserialize = build_order_deserializer(settings)
    ctx = SerializationContext(settings.topic_orders, MessageField.VALUE)

    table = Table(title=f"Dead Letter Queue: {settings.topic_dlq}", expand=True)
    table.add_column("src offset", justify="right")
    table.add_column("order")
    table.add_column("payload")
    table.add_column("error type", style="red")
    table.add_column("tries", justify="right")
    table.add_column("reason", overflow="fold")

    idle = 0.0
    count = 0

    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                idle += 1.0
                if not args.follow and idle >= args.timeout:
                    break
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                console.print(f"[red]error:[/red] {message.error()}")
                continue

            idle = 0.0
            headers = decode(message.headers())

            # The original bytes are preserved, so most dead letters still decode.
            try:
                order = deserialize(message.value(), ctx)
                payload = f"{order.product or '<empty>'} @ {order.price:,.2f}"
                order_id = order.orderId
            except Exception:  # noqa: BLE001
                payload = "<undecodable>"
                order_id = (message.key() or b"?").decode(errors="replace")

            table.add_row(
                headers.get("x-original-offset", "?"),
                order_id,
                payload,
                headers.get("x-error-type", "?"),
                headers.get("x-retry-attempts", "0"),
                headers.get("x-error-message", ""),
            )
            count += 1

            if args.follow:
                console.clear()
                console.print(table)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    if not args.follow:
        console.print(table)
    console.print(f"\n[bold]{count}[/bold] message(s) in the DLQ.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
