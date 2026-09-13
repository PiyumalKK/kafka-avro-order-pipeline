"""Render the architecture diagram to PNG.

    python scripts/make_diagram.py

Writes docs/images/architecture.png (and .jpg) at 200 DPI, sized for a report
page or a slide.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import matplotlib.patches as patches  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402

OUT_DIR = Path(__file__).resolve().parent.parent / "docs" / "images"

# palette
INK = "#1A202C"
MUTED = "#4A5568"
FAINT = "#A0AEC0"
TEAL_F, TEAL_E = "#B2F5EA", "#234E52"
BLUE_F, BLUE_E = "#BEE3F8", "#2A4365"
AMBER_F, AMBER_E = "#FEEBC8", "#7B341E"
GREY_F, GREY_E = "#EDF2F7", "#4A5568"
GREEN_F, GREEN_E = "#C6F6D5", "#22543D"
RED_F, RED_E = "#FED7D7", "#822727"


def box(ax, x0, y0, x1, y1, face, edge, lw=1.8, radius=1.2, z=2, alpha=1.0):
    p = FancyBboxPatch(
        (x0, y0), x1 - x0, y1 - y0,
        boxstyle=f"round,pad=0,rounding_size={radius}",
        facecolor=face, edgecolor=edge, linewidth=lw, zorder=z, alpha=alpha,
    )
    ax.add_patch(p)
    return p


def label(ax, x, y, text, size=11, color=INK, weight="normal", ha="center", va="center", style="normal"):
    ax.text(x, y, text, fontsize=size, color=color, fontweight=weight,
            ha=ha, va=va, zorder=6, style=style, linespacing=1.5)


def arrow(ax, start, end, color=MUTED, lw=2.0, style="-|>", dashed=False, rad=0.0, z=3):
    a = FancyArrowPatch(
        start, end, arrowstyle=style, mutation_scale=18,
        linewidth=lw, color=color, zorder=z,
        connectionstyle=f"arc3,rad={rad}",
        linestyle=(0, (5, 3)) if dashed else "solid",
        shrinkA=2, shrinkB=2,
    )
    ax.add_patch(a)


def build() -> plt.Figure:
    fig, ax = plt.subplots(figsize=(17.5, 10), dpi=200)
    ax.set_xlim(0, 180)
    ax.set_ylim(0, 104)
    ax.axis("off")
    fig.patch.set_facecolor("white")

    # ------------------------------------------------------------- titles
    label(ax, 4, 99, "Kafka + Avro Order Pipeline", size=23, weight="bold", ha="left")
    label(ax, 4, 94.2,
          "Real-time aggregation  ·  Bounded retry with exponential backoff  ·  Dead Letter Queue",
          size=12.5, color=MUTED, ha="left")
    label(ax, 176, 98.6, "EC8202 Big Data Analytics", size=11.5, color=MUTED, ha="right")
    label(ax, 176, 94.6, "University of Ruhuna", size=11.5, color=FAINT, ha="right")
    ax.plot([4, 176], [90.5, 90.5], color="#CBD5E0", lw=1.2, zorder=1)

    # ------------------------------------------------- schema registry
    box(ax, 58, 78, 122, 88.5, AMBER_F, AMBER_E)
    label(ax, 90, 85, "Schema Registry", size=13, weight="bold")
    label(ax, 90, 81, "subject: orders-value   ·   order.avsc   ·   BACKWARD compatibility",
          size=10.3, color=MUTED)

    # ------------------------------------------------------- producer
    box(ax, 6, 48, 34, 68, TEAL_F, TEAL_E)
    label(ax, 20, 63.5, "Order Producer", size=13, weight="bold")
    label(ax, 20, 58.8, "orderId · product · price", size=10.2, color=MUTED)
    label(ax, 20, 55, "Avro serialize", size=10.5, weight="bold", color=TEAL_E)
    label(ax, 20, 51.5, "key = orderId", size=9.6, color=MUTED)

    # ---------------------------------------------------------- kafka
    box(ax, 45, 43, 76, 71, BLUE_F, BLUE_E, lw=2.2)
    label(ax, 60.5, 67.8, "Apache Kafka", size=13, weight="bold")
    label(ax, 60.5, 64.6, "KRaft mode · no ZooKeeper", size=9.4, color=MUTED)
    label(ax, 60.5, 61, "topic: orders", size=11.4, weight="bold", color=BLUE_E)
    for i in range(3):
        y = 56.5 - i * 4.2
        box(ax, 49, y - 1.55, 72, y + 1.55, "#FFFFFF", "#63B3ED", lw=1.2, radius=0.5, z=4)
        label(ax, 60.5, y, f"partition {i}", size=9.2, color=MUTED)
    label(ax, 60.5, 45.2, "append-only  ·  replayable", size=9.2, color=FAINT, style="italic")

    # ------------------------------------------------------- consumer
    box(ax, 86, 40, 174, 73, "#FFFFFF", GREY_E, lw=2.2)
    # Kept clear of x=99..103, where the Schema Registry arrow comes down.
    label(ax, 136, 69.5, "Order Consumer", size=13, weight="bold")
    label(ax, 136, 65.8, "group: order-processor   ·   manual offset commit",
          size=9.8, color=MUTED)

    stages = [
        (90.0, 108.0, "1. Decode", "Avro deserialize", GREY_F, GREY_E),
        (112.0, 130.0, "2. Validate", "business rules", GREY_F, GREY_E),
        (134.0, 152.0, "3. Reserve", "downstream call", GREY_F, GREY_E),
        (156.0, 171.0, "4. Aggregate", "running average", GREEN_F, GREEN_E),
    ]
    for x0, x1, title, sub, f, e in stages:
        box(ax, x0, 44, x1, 62, f, e, lw=1.6, radius=0.9, z=4)
        label(ax, (x0 + x1) / 2, 57, title, size=11, weight="bold")
        label(ax, (x0 + x1) / 2, 51.5, sub, size=9.4, color=MUTED)

    # retry lives inside stage 3, so nothing collides with the box edge
    label(ax, 143, 47.4, "↻  retry 3×", size=9.4, weight="bold", color=AMBER_E)
    label(ax, 143, 45.2, "backoff + jitter", size=8.6, color=AMBER_E)

    # ------------------------------------------------- running average
    box(ax, 150, 16, 178, 33, GREEN_F, GREEN_E, lw=1.8)
    label(ax, 164, 29, "RUNNING AVERAGE", size=10.6, weight="bold", color=GREEN_E)
    label(ax, 164, 24.6, "updated on every message", size=9.4, color=MUTED)
    label(ax, 164, 20.4, "Welford · global + per product", size=9.0, color=MUTED)

    # ------------------------------------------------------------ DLQ
    box(ax, 86, 16, 140, 33, RED_F, RED_E, lw=2.0)
    label(ax, 113, 29.4, "Dead Letter Queue   ·   orders.dlq", size=12.2, weight="bold", color=RED_E)
    label(ax, 113, 25.2, "original bytes forwarded untouched", size=9.8, color=MUTED)
    label(ax, 113, 21.4,
          "headers:  x-original-topic / partition / offset  ·  x-error-type",
          size=8.9, color=MUTED)
    label(ax, 113, 18.4, "x-error-message  ·  x-retry-attempts  ·  x-failed-at",
          size=8.9, color=MUTED)

    # ----------------------------------------------------------- flow
    arrow(ax, (34, 58), (45, 58), lw=2.4, color=BLUE_E)
    label(ax, 39.5, 60.4, "Avro", size=9.4, weight="bold", color=BLUE_E)
    label(ax, 39.5, 56.0, "bytes", size=9.4, color=MUTED)

    arrow(ax, (76, 56), (90, 54), lw=2.4, color=BLUE_E)
    label(ax, 82.5, 58.4, "poll", size=9.4, weight="bold", color=BLUE_E)

    arrow(ax, (108, 53), (112, 53), lw=2.0)
    arrow(ax, (130, 53), (134, 53), lw=2.0)
    arrow(ax, (152, 53), (156, 53), lw=2.0)

    # schema registry links
    arrow(ax, (62, 78.4), (24, 68.4), color=AMBER_E, lw=1.6, dashed=True, rad=0.16)
    label(ax, 41, 74.2, "register schema", size=9.0, color=AMBER_E, style="italic")
    arrow(ax, (102, 77.6), (99, 62.4), color=AMBER_E, lw=1.6, dashed=True, rad=0.1)
    label(ax, 104.5, 75.4, "fetch by id", size=9.0, color=AMBER_E, style="italic", ha="left")

    # failure paths into the DLQ, labelled below the consumer border
    arrow(ax, (99, 43.6), (99, 33.6), color=RED_E, lw=2.0)
    label(ax, 97, 37.6, "undecodable", size=9.0, color=RED_E, ha="right")

    arrow(ax, (121, 43.6), (114, 33.6), color=RED_E, lw=2.0)
    label(ax, 123, 37.6, "invalid data", size=9.0, color=RED_E, ha="left")

    arrow(ax, (143, 43.6), (130, 33.6), color=RED_E, lw=2.0, rad=0.08)
    label(ax, 145, 37.6, "retries\nexhausted", size=9.0, color=RED_E, ha="left")

    # aggregate output
    arrow(ax, (164, 43.6), (164, 33.4), color=GREEN_E, lw=2.2)

    # ------------------------------------------------------- footnotes
    ax.plot([4, 176], [11.5, 11.5], color="#E2E8F0", lw=1.1, zorder=1)
    notes = (
        "Delivery: at-least-once — offsets are committed only after a message reaches a terminal state "
        "(aggregated or dead-lettered).\n"
        "Producer: acks=all, idempotent.    Ordering: messages keyed by orderId share a partition.    "
        "Retry applies to transient faults only; permanent faults never consume the retry budget."
    )
    label(ax, 4, 6.5, notes, size=9.6, color=MUTED, ha="left", va="center")

    fig.tight_layout(pad=0.6)
    return fig


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig = build()

    png = OUT_DIR / "architecture.png"
    jpg = OUT_DIR / "architecture.jpg"
    fig.savefig(png, dpi=200, facecolor="white", bbox_inches="tight", pad_inches=0.25)
    fig.savefig(jpg, dpi=200, facecolor="white", bbox_inches="tight", pad_inches=0.25,
                pil_kwargs={"quality": 94})
    plt.close(fig)

    for f in (png, jpg):
        print(f"wrote {f}  ({f.stat().st_size / 1024:.0f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
