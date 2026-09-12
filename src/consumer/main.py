"""Order consumer: deserialise, validate, retry, aggregate, dead-letter.

Delivery semantics are at-least-once. Offsets are committed only after a
message reaches a terminal state -- either successfully aggregated or safely
parked in the DLQ -- so a crash mid-processing replays the message rather
than losing it.
"""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from confluent_kafka import KafkaError, Message  # noqa: E402
from confluent_kafka.serialization import MessageField, SerializationContext  # noqa: E402
from rich.live import Live  # noqa: E402

from src.common.errors import PermanentError, RetriesExhausted  # noqa: E402
from src.common.kafka_clients import (  # noqa: E402
    build_consumer,
    build_order_deserializer,
    build_producer,
)
from src.common.logging_setup import configure, console  # noqa: E402
from src.config import settings  # noqa: E402
from src.consumer.dashboard import render  # noqa: E402
from src.consumer.dlq import DeadLetterPublisher  # noqa: E402
from src.consumer.stats import PipelineStats  # noqa: E402
from src.processing.aggregator import OrderAggregator  # noqa: E402
from src.processing.inventory import InventoryService  # noqa: E402
from src.processing.retry import RetryPolicy  # noqa: E402
from src.processing.validation import validate  # noqa: E402

log = logging.getLogger("consumer")
_running = True


def _stop(*_args: object) -> None:
    global _running
    _running = False
    console.print("\n[yellow]Draining and shutting down...[/yellow]")


class OrderProcessor:
    """Owns the per-message decision: aggregate it, retry it, or bin it."""

    def __init__(self, dlq: DeadLetterPublisher, inventory: InventoryService) -> None:
        self.aggregator = OrderAggregator()
        self.stats = PipelineStats()
        self.dlq = dlq
        self.inventory = inventory
        self.policy = RetryPolicy(
            max_attempts=settings.max_retry_attempts,
            base_delay=settings.retry_base_delay_seconds,
            max_delay=settings.retry_max_delay_seconds,
        )
        self._deserialize = build_order_deserializer(settings)
        self._ctx = SerializationContext(settings.topic_orders, MessageField.VALUE)

    def handle(self, message: Message) -> None:
        self.stats.consumed += 1

        # --- Stage 1: decode -------------------------------------------------
        try:
            order = self._deserialize(message.value(), self._ctx)
        except Exception as exc:  # noqa: BLE001 -- any decode failure is terminal
            self.stats.dlq_undeserializable += 1
            self.dlq.publish(message, PermanentError(f"Avro decode failed: {exc}"), attempts=0)
            return

        if order is None:
            self.stats.dlq_undeserializable += 1
            self.dlq.publish(message, PermanentError("null message body"), attempts=0)
            return

        # --- Stage 2: validate (permanent faults) ----------------------------
        try:
            validate(order)
        except PermanentError as exc:
            self.stats.dlq_invalid += 1
            self.dlq.publish(message, exc, attempts=0)
            return

        # --- Stage 3: downstream call, with bounded retry (transient faults) --
        failures_before = self.inventory.failures
        try:
            self.policy.run(
                lambda: self.inventory.reserve(order),
                description=f"reserve stock for order {order.orderId}",
            )
        except RetriesExhausted as exc:
            self.stats.retried += exc.attempts - 1
            self.stats.dlq_exhausted += 1
            self.dlq.publish(message, exc, attempts=exc.attempts)
            return

        failed_tries = self.inventory.failures - failures_before
        if failed_tries:
            self.stats.retried += failed_tries
            self.stats.recovered += 1
            log.info("order %s recovered after %d retry(ies)", order.orderId, failed_tries)

        # --- Stage 4: real-time aggregation ----------------------------------
        running_avg = self.aggregator.add(order)
        self.stats.processed += 1
        log.info(
            "ok %s %-7s %9.2f  ->  running avg %.2f over %d orders",
            order.orderId, order.product, order.price, running_avg,
            self.aggregator.processed_count,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Consume Avro orders, aggregate, retry, dead-letter."
    )
    parser.add_argument("--plain", action="store_true", help="log lines only, no live dashboard")
    parser.add_argument("--transient-fault-rate", type=float,
                        default=settings.fault_rate_transient,
                        help="simulated downstream failure rate (0-1)")
    parser.add_argument("--max-messages", type=int, default=0,
                        help="stop after N messages (0 = forever)")
    args = parser.parse_args()

    configure()
    signal.signal(signal.SIGINT, _stop)

    consumer = build_consumer(settings, client_id="order-consumer")
    dlq = DeadLetterPublisher(build_producer(settings, client_id="order-dlq-producer"), settings)
    processor = OrderProcessor(dlq, InventoryService(failure_rate=args.transient_fault_rate))

    consumer.subscribe([settings.topic_orders])

    console.rule("[bold cyan]Order Consumer[/bold cyan]")
    console.print(f"broker        : {settings.bootstrap_servers}")
    console.print(f"group         : {settings.consumer_group}")
    console.print(f"topics        : {settings.topic_orders} -> DLQ {settings.topic_dlq}")
    console.print(f"retry policy  : {settings.max_retry_attempts} attempts, "
                  f"{settings.retry_base_delay_seconds}s base, exponential + jitter")
    console.print(f"transient rate: {args.transient_fault_rate:.0%}\n")

    live = None if args.plain else Live(
        render(processor.aggregator, processor.stats),
        console=console, refresh_per_second=4, vertical_overflow="visible",
    )

    try:
        if live:
            live.start()

        while _running:
            message = consumer.poll(1.0)

            if message is None:
                if live:
                    live.update(render(processor.aggregator, processor.stats))
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("consumer error: %s", message.error())
                continue

            processor.handle(message)
            # Terminal state reached -> it is now safe to advance the offset.
            consumer.commit(message, asynchronous=False)

            if live:
                live.update(render(processor.aggregator, processor.stats))

            if args.max_messages and processor.stats.consumed >= args.max_messages:
                break

    finally:
        if live:
            live.stop()
        dlq.flush()
        consumer.close()

        s = processor.stats
        console.print(render(processor.aggregator, s))
        console.rule("[bold cyan]Final report[/bold cyan]")
        console.print(f"consumed        : {s.consumed}")
        console.print(f"aggregated      : {s.processed}")
        console.print(f"running average : {processor.aggregator.running_average:,.2f}")
        console.print(f"retry attempts  : {s.retried} (recovered {s.recovered})")
        console.print(f"dead-lettered   : {s.dlq_total} "
                      f"(invalid {s.dlq_invalid}, exhausted {s.dlq_exhausted}, "
                      f"undecodable {s.dlq_undeserializable})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
