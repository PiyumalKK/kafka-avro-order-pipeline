"""Central, environment-driven configuration for every component.

Nothing in this project reads os.environ directly; they all go through
Settings so the whole pipeline can be reconfigured from a single .env file.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = PROJECT_ROOT / "schemas" / "order.avsc"

load_dotenv(PROJECT_ROOT / ".env")


def _env(key: str, default: str) -> str:
    return os.getenv(key, default)


def _env_float(key: str, default: float) -> float:
    return float(os.getenv(key, str(default)))


def _env_int(key: str, default: int) -> int:
    return int(os.getenv(key, str(default)))


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    schema_registry_url: str = field(default_factory=lambda: _env("SCHEMA_REGISTRY_URL", "http://localhost:8081"))

    topic_orders: str = field(default_factory=lambda: _env("TOPIC_ORDERS", "orders"))
    topic_retry: str = field(default_factory=lambda: _env("TOPIC_RETRY", "orders.retry"))
    topic_dlq: str = field(default_factory=lambda: _env("TOPIC_DLQ", "orders.dlq"))

    consumer_group: str = field(default_factory=lambda: _env("CONSUMER_GROUP", "order-processor"))

    max_retry_attempts: int = field(default_factory=lambda: _env_int("MAX_RETRY_ATTEMPTS", 3))
    retry_base_delay_seconds: float = field(default_factory=lambda: _env_float("RETRY_BASE_DELAY_SECONDS", 0.4))
    retry_max_delay_seconds: float = field(default_factory=lambda: _env_float("RETRY_MAX_DELAY_SECONDS", 6.0))

    produce_interval_seconds: float = field(default_factory=lambda: _env_float("PRODUCE_INTERVAL_SECONDS", 0.4))
    fault_rate_permanent: float = field(default_factory=lambda: _env_float("FAULT_RATE_PERMANENT", 0.08))
    fault_rate_transient: float = field(default_factory=lambda: _env_float("FAULT_RATE_TRANSIENT", 0.15))

    @property
    def all_topics(self) -> list[str]:
        return [self.topic_orders, self.topic_retry, self.topic_dlq]


settings = Settings()
