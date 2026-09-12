"""Order producer: generates purchase transactions and publishes them as Avro.

A deliberate slice of the traffic is malformed. Without it a live demo shows
a happy path and nothing else -- the retry and DLQ machinery would never
fire. See --permanent-fault-rate below.
"""

from __future__ import annotations

import argparse
import itertools
import logging
import random
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402

from src.common.kafka_clients import build_order_serializer, build_producer  # noqa: E402
from src.common.logging_setup import configure, console  # noqa: E402
from src.config import settings  # noqa: E402
from src.domain.order import Order  # noqa: E402

log = logging.getLogger("producer")

PRODUCTS = [f"Item{i}" for i in range(1, 11)]
_running = True


def _stop(*_args: object) -> None:
    global _running
    _running = False
    console.print("\n[yellow]Stopping producer...[/yellow]")


def make_valid_order(order_id: int) -> Order:
    return Order(
        orderId=str(order_id),
        product=random.choice(PRODUCTS),
        price=round(random.uniform(50.0, 5000.0), 2),
    )


def make_poisoned_order(order_id: int) -> tuple[Order, str]:
    """Produce an order that validation will reject outright -> DLQ."""
    kind = random.choice(["negative_price", "zero_price", "empty_product", "absurd_price"])
    product = random.choice(PRODUCTS)

    if kind == "negative_price":
        return Order(str(order_id), product, round(random.uniform(-2000, -1), 2)), kind
    if kind == "zero_price":
        return Order(str(order_id), product, 0.0), kind
    if kind == "empty_product":
        return Order(str(order_id), "", round(random.uniform(50, 500), 2)), kind
    return Order(str(order_id), product, 9_999_999.0), kind


def delivery_report(err: object, msg: object) -> None:
    if err is not None:
        log.error("delivery failed: %s", err)


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish Avro-encoded order messages to Kafka.")
    parser.add_argument("--count", type=int, default=0, help="messages to send (0 = run until Ctrl+C)")
    parser.add_argument("--interval", type=float, default=settings.produce_interval_seconds,
                        help="seconds between messages")
    parser.add_argument("--start-id", type=int, default=1001, help="first orderId")
    parser.add_argument("--permanent-fault-rate", type=float, default=settings.fault_rate_permanent,
                        help="fraction of orders deliberately made invalid (0-1)")
    parser.add_argument("--clean", action="store_true", help="send only valid orders")
    args = parser.parse_args()

    configure()
    signal.signal(signal.SIGINT, _stop)

    fault_rate = 0.0 if args.clean else args.permanent_fault_rate

    producer = build_producer(settings, client_id="order-producer")
    serializer = build_order_serializer(settings)
    ctx = SerializationContext(settings.topic_orders, MessageField.VALUE)

    console.rule("[bold cyan]Order Producer[/bold cyan]")
    console.print(f"broker      : {settings.bootstrap_servers}")
    console.print(f"registry    : {settings.schema_registry_url}")
    console.print(f"topic       : {settings.topic_orders}")
    console.print(f"fault rate  : {fault_rate:.0%} invalid orders")
    console.print(f"volume      : {'unbounded' if args.count == 0 else args.count} messages\n")

    sent = poisoned = 0
    counter = itertools.count(args.start_id)

    try:
        while _running:
            if args.count and sent >= args.count:
                break

            order_id = next(counter)
            if random.random() < fault_rate:
                order, kind = make_poisoned_order(order_id)
                poisoned += 1
                log.warning("injecting invalid order %s (%s) price=%.2f product=%r",
                            order.orderId, kind, order.price, order.product)
            else:
                order = make_valid_order(order_id)
                log.info("sent %s  %-7s %10.2f", order.orderId, order.product, order.price)

            producer.produce(
                topic=settings.topic_orders,
                key=order.orderId.encode(),
                value=serializer(order, ctx),
                on_delivery=delivery_report,
            )
            producer.poll(0)
            sent += 1

            if args.interval > 0:
                time.sleep(args.interval)

    finally:
        remaining = producer.flush(10)
        console.rule("[bold cyan]Summary[/bold cyan]")
        console.print(f"produced         : {sent}")
        console.print(f"  valid          : {sent - poisoned}")
        console.print(f"  invalid (bait) : {poisoned}")
        if remaining:
            log.error("%d message(s) still undelivered after flush", remaining)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
