# Live demonstration script

Everything needed to record or present the demo in one take. Target length **6–7 minutes**.

---

## Before you hit record

```powershell
# 1. Stack up and completely clean
docker compose down -v
docker compose up -d

# 2. Wait until all three report healthy
docker compose ps

# 3. Create topics
.\.venv\Scripts\python.exe scripts\create_topics.py

# 4. Prove the tests pass (do this on camera later, but check it now)
.\.venv\Scripts\python.exe -m pytest -q
```

**Window layout** — three PowerShell windows plus a browser:

```
┌─────────────────────────┬─────────────────────────┐
│  1. CONSUMER            │  3. Browser              │
│     (live dashboard)    │     Kafka UI :8080       │
├─────────────────────────┤                          │
│  2. PRODUCER            │                          │
└─────────────────────────┴──────────────────────────┘
```

Set both terminals to a large font (Ctrl + Shift + `+`). A recording where the examiner
cannot read the running average is a wasted recording.

**Recording tools:** Win + G (Xbox Game Bar) captures a single window. For the multi-window
layout above, use **OBS Studio** with a Display Capture source — it records the whole desktop
and lets you keep the browser and terminals visible at once.

---

## The run sheet

### Scene 1 — The problem and the architecture (45 s)

*Screen: the README, scrolled to the mermaid diagram.*

> "This is a Kafka pipeline for order transactions. A producer publishes orders as Avro
> records; a consumer decodes them, validates them, calls a downstream service, and keeps a
> running average of prices. The interesting part is what happens when things fail — failures
> are split into transient ones, which get retried, and permanent ones, which go to a Dead
> Letter Queue."

### Scene 2 — The infrastructure (30 s)

*Screen: terminal.*

```powershell
docker compose ps
```

> "Three services: Kafka running in KRaft mode — no ZooKeeper — Schema Registry holding the
> Avro contract, and Kafka UI so we can see inside the topics."

```powershell
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

> "Three topics: the main `orders` topic, a retry topic, and the dead letter queue."

### Scene 3 — The Avro contract (40 s)

*Screen: `schemas/order.avsc`.*

> "The schema — orderId and product as strings, price as a float. This is registered in Schema
> Registry, so the schema travels as a four-byte ID rather than being repeated in every
> message. That is the whole reason Avro beats JSON on a high-volume topic."

*Browser: Kafka UI → Schema Registry → `orders-value`.* (Note: this subject appears after the
producer has run once, so either run the producer briefly beforehand, or show this in Scene 6.)

### Scene 4 — Start the consumer (30 s)

*Terminal 1:*

```powershell
.\.venv\Scripts\python.exe -m src.consumer.main
```

> "The consumer starts up and the dashboard is waiting at zero. Retry policy is three attempts
> with exponential backoff, and the simulated downstream service fails about 15% of calls."

### Scene 5 — Start the producer: real-time aggregation (60 s)

*Terminal 2:*

```powershell
.\.venv\Scripts\python.exe -m src.producer.main
```

> "Orders start flowing. Watch the running average at the top — it updates on every single
> message, in real time, along with a per-product breakdown on the right."

**Point at:** `RUNNING AVERAGE`, the climbing `orders aggregated` count, the per-product table.

> "The average is computed with Welford's online algorithm rather than a running sum, so it
> stays numerically stable over a stream that runs for days."

### Scene 6 — Retry logic (60 s)

**Point at:** the yellow `retrying in 0.42s` lines in the consumer, and `recovered by retry`
climbing on the dashboard.

> "These yellow lines are the retry logic. The downstream inventory service failed, so the
> consumer backs off exponentially — with jitter, so a fleet of consumers doesn't all retry at
> the same instant and stampede a service that's already struggling — and tries again. Most
> recover; you can see the 'recovered by retry' counter going up."

If retries are slow to appear, stop the consumer and restart it turned up:

```powershell
.\.venv\Scripts\python.exe -m src.consumer.main --transient-fault-rate 0.7
```

> "Turning the failure rate up to 70% — now you can see retries constantly, and some of them
> exhausting their budget and being dead-lettered."

### Scene 7 — The Dead Letter Queue (75 s)

**Point at:** the red `DLQ <-` lines and the DLQ counters on the dashboard.

> "Two different things end up in the DLQ. The 'invalid' ones are permanent failures — a
> negative price, an empty product name — where retrying could never help, so they go straight
> there without wasting the retry budget. The 'exhausted' ones are transient failures that
> never cleared."

*Terminal 3:*

```powershell
.\.venv\Scripts\python.exe scripts\inspect_dlq.py
```

> "And here is the DLQ itself. Every dead letter carries its original offset, the error type,
> how many attempts were made, and the reason — all as Kafka headers. The original bytes are
> preserved untouched, so these messages can be diagnosed and replayed once the root cause is
> fixed. A DLQ that just swallows messages is useless; this one is an audit trail."

*Browser: Kafka UI → Topics → `orders.dlq` → Messages → expand one → Headers tab.*

> "Same thing from Kafka UI — you can see the headers on the actual record."

### Scene 8 — At-least-once delivery (45 s)

*Terminal 1: press Ctrl+C, then restart the consumer.*

> "The consumer commits offsets manually, and only after a message reaches a terminal state —
> either aggregated or safely in the DLQ. So when I kill it and bring it back, it resumes from
> the last committed offset. Nothing is lost. That's at-least-once delivery; auto-commit would
> advance the offset on a timer and silently skip messages on a crash."

### Scene 9 — Tests (30 s)

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

> "Twenty-seven unit tests covering the aggregation maths, the validation rules, the backoff
> curve, the fact that permanent errors are never retried, and the DLQ header contract."

### Scene 10 — Close (20 s)

> "All of it comes up with a single `docker compose up`, the repository is on GitHub, and the
> design decisions and their trade-offs are written up in `docs/architecture.md`."

---

## Fully scripted alternative

If you would rather record one continuous automated run with no typing:

```powershell
.\scripts\demo.ps1
```

This publishes 60 orders (with deliberate bad data), consumes exactly those 60 with the plain
logger, then prints the DLQ table. It is about 90 seconds end to end and is a good fallback,
but the live two-terminal version above is more convincing because the dashboard visibly moves.

---

## Questions you should be ready for

**"Why Avro and not JSON?"**
Smaller payloads — the schema is a 4-byte ID, not repeated field names in every record — and
enforced schema evolution. The registry rejects an incompatible producer, so the contract is
guarded by infrastructure rather than code review.

**"Why not just retry everything?"**
Because a permanent failure retried forever is a poison pill: it blocks the partition behind a
message that can never succeed, and the consumer stalls indefinitely.

**"Why jitter in the backoff?"**
Without it, every consumer that failed at the same moment retries at the same moment — a
synchronised thundering herd that re-kills the service the instant it recovers.

**"What happens if the consumer crashes mid-processing?"**
The offset was not committed, so the message is redelivered — at-least-once. The trade-off is
that one order can be counted twice in the average. Exactly-once would need Kafka transactions
binding the offset commit and the aggregate update into one atomic write.

**"Would this scale?"**
The topic has 3 partitions, so up to 3 consumers can run in parallel in the group. But the
aggregate is per-process, so a global running average across the group would need a Kafka
Streams state store or partial aggregates on a compacted topic. Stated as a known limitation
in `docs/architecture.md`.

**"Why is the producer sending bad data deliberately?"**
Because retry logic and a DLQ are only observable when things break. A demo on clean data never
executes either code path and proves nothing.
