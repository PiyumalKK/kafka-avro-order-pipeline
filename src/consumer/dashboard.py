"""Live terminal dashboard -- the thing you point at during the demo."""

from __future__ import annotations

from rich.console import Group
from rich.panel import Panel
from rich.table import Table

from src.consumer.stats import PipelineStats
from src.processing.aggregator import OrderAggregator


def render(aggregator: OrderAggregator, stats: PipelineStats) -> Panel:
    headline = Table.grid(padding=(0, 3))
    headline.add_column(justify="right", style="dim")
    headline.add_column(justify="left")

    avg = aggregator.running_average
    headline.add_row("RUNNING AVERAGE", f"[bold green]{avg:,.2f}[/bold green]")
    headline.add_row("orders aggregated", f"{aggregator.processed_count:,}")
    headline.add_row("total value", f"{aggregator.overall.total:,.2f}")
    if aggregator.processed_count:
        headline.add_row(
            "min / max",
            f"{aggregator.overall.minimum:,.2f} / {aggregator.overall.maximum:,.2f}",
        )

    health = Table(title="Pipeline health", title_style="bold", expand=True)
    health.add_column("metric")
    health.add_column("value", justify="right")
    health.add_row("consumed", f"{stats.consumed:,}")
    health.add_row("processed OK", f"[green]{stats.processed:,}[/green]")
    health.add_row("retry attempts", f"[yellow]{stats.retried:,}[/yellow]")
    health.add_row("recovered by retry", f"[cyan]{stats.recovered:,}[/cyan]")
    health.add_row("DLQ - invalid data", f"[red]{stats.dlq_invalid:,}[/red]")
    health.add_row("DLQ - retries exhausted", f"[red]{stats.dlq_exhausted:,}[/red]")
    health.add_row("DLQ - undeserializable", f"[red]{stats.dlq_undeserializable:,}[/red]")
    health.add_row("success rate", f"{stats.success_rate:.1f}%")
    health.add_row("throughput", f"{stats.throughput:.1f} msg/s")

    products = Table(title="Running average by product", title_style="bold", expand=True)
    products.add_column("product")
    products.add_column("n", justify="right")
    products.add_column("avg price", justify="right")
    for product, s in sorted(aggregator.by_product.items()):
        products.add_row(product, str(s.count), f"{s.mean:,.2f}")

    side_by_side = Table.grid(expand=True)
    side_by_side.add_column(ratio=1)
    side_by_side.add_column(ratio=1)
    side_by_side.add_row(health, products)

    return Panel(
        Group(headline, "", side_by_side),
        title="[bold cyan]Real-time Order Aggregation[/bold cyan]",
        border_style="cyan",
    )
