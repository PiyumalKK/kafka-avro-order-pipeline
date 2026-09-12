"""Factories for the Avro-aware Kafka clients used by both apps.

Serializers are built explicitly (rather than via the deprecated
SerializingProducer) so the DLQ path can reuse the same Producer instance to
forward *raw, undeserialised* bytes.
"""

from __future__ import annotations

from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

from src.config import SCHEMA_PATH, Settings
from src.domain.order import Order


def load_schema_str() -> str:
    """Read order.avsc from disk -- the single source of truth for the contract."""
    return SCHEMA_PATH.read_text(encoding="utf-8")


def schema_registry(settings: Settings) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": settings.schema_registry_url})


def build_order_serializer(settings: Settings) -> AvroSerializer:
    """Registers order.avsc under the 'orders-value' subject on first use."""
    return AvroSerializer(
        schema_registry(settings),
        load_schema_str(),
        lambda order, _ctx: order.to_dict(),
    )


def build_order_deserializer(settings: Settings) -> AvroDeserializer:
    return AvroDeserializer(
        schema_registry(settings),
        load_schema_str(),
        Order.from_dict,
    )


def build_producer(settings: Settings, *, client_id: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": client_id,
            # Durability over raw throughput: wait for all in-sync replicas.
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "linger.ms": 10,
            "compression.type": "snappy",
        }
    )


def build_consumer(settings: Settings, *, client_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": settings.consumer_group,
            "client.id": client_id,
            "auto.offset.reset": "earliest",
            # Offsets are committed by hand only after a message is fully
            # handled (processed or dead-lettered), giving at-least-once
            # delivery with no silent data loss on a crash.
            "enable.auto.commit": False,
            "session.timeout.ms": 45000,
            "max.poll.interval.ms": 300000,
        }
    )
