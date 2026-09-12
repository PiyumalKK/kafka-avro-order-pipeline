"""Create the three pipeline topics. Safe to re-run."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from confluent_kafka.admin import AdminClient, NewTopic  # noqa: E402

from src.config import settings  # noqa: E402

PARTITIONS = 3
REPLICATION = 1


def main() -> int:
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics)

    wanted = [t for t in settings.all_topics if t not in existing]
    for topic in settings.all_topics:
        if topic in existing:
            print(f"  = {topic} already exists")

    if not wanted:
        print("All topics present.")
        return 0

    futures = admin.create_topics(
        [NewTopic(t, num_partitions=PARTITIONS, replication_factor=REPLICATION) for t in wanted]
    )
    failed = False
    for topic, future in futures.items():
        try:
            future.result()
            print(f"  + created {topic} ({PARTITIONS} partitions)")
        except Exception as exc:  # noqa: BLE001
            print(f"  ! failed to create {topic}: {exc}")
            failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
