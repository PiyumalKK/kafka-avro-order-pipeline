"""Console logging that stays readable next to a live dashboard."""

from __future__ import annotations

import logging

from rich.console import Console
from rich.logging import RichHandler

console = Console()


def configure(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="%H:%M:%S",
        handlers=[RichHandler(console=console, rich_tracebacks=True, show_path=False)],
    )
    # The librdkafka wrapper is chatty about partition assignments at INFO.
    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
