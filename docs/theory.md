# The theory behind this project

A study guide for EC8202. Every section explains a concept, then points at the line of code in
this repository where it actually happens. Read it in order — each part builds on the last.

---

## Part 1 — Why "Big Data" exists at all

### 1.1 The traditional system: OLTP

For decades, data lived in a **relational database** (MySQL, Oracle, PostgreSQL) doing **OLTP** —
*Online Transaction Processing*. Small, fast operations: insert an order, update a balance,
read a customer row.

Relational databases guarantee **ACID**:

| Letter | Meaning |
|---|---|
| **A**tomicity | A transaction happens completely or not at all. No half-finished transfers. |
| **C**onsistency | The database moves from one valid state to another; constraints are never violated. |
| **I**solation | Concurrent transactions don't see each other's half-finished work. |
| **D**urability | Once committed, it survives a power cut. |

ACID is wonderful and you should not give it up lightly. The problem is not that it is wrong —
it is that it is *expensive to maintain across many machines*.

### 1.2 The analytical system: OLAP and the warehouse

Business questions ("what was average revenue per product last quarter?") are a terrible fit for
an OLTP database — they scan millions of rows and slow down the live system.

So organisations built a second system: the **data warehouse**, doing **OLAP** (*Online
Analytical Processing*). Data is copied from OLTP into the warehouse by an **ETL** process:

- **E**xtract from the source systems
- **T**ransform — clean it, reshape it, join it
- **L**oad into the warehouse

| | OLTP | OLAP |
|---|---|---|
| Purpose | Run the business | Understand the business |
| Operations | Many small reads/writes | Few huge reads |
| Design | Normalised (no duplication) | Denormalised (fast reads) |
| Question | "What is order 1042?" | "What is the average order value?" |

ETL traditionally runs **overnight in batch**. That is the crack the rest of this module lives
in: by the time the report is ready, the data is a day old.

### 1.3 What broke: the three Vs

Traditional systems hit three walls at once:

- **Volume** — more data than one machine's disk can hold.
- **Velocity** — data arriving continuously and needing answers in seconds, not overnight.
- **Variety** — not just neat tables, but JSON, logs, images, sensor readings.

*(You will often see two more added: **Veracity** — can you trust it? — and **Value** — is it
worth storing?)*

**Your project is a Velocity problem.** The running average is recomputed on every single
message. There is no overnight batch job. That is the entire point.

### 1.4 Scaling up vs scaling out

| | **Vertical** (scale up) | **Horizontal** (scale out) |
|---|---|---|
| Method | Buy a bigger machine | Add more ordinary machines |
| Limit | Hard ceiling; cost rises faster than power | Nearly unlimited |
| Failure | One machine dies = everything dies | One machine dies = the rest continue |
| Difficulty | Easy — change nothing | Hard — your software must be distributed |

Big data tools all choose **horizontal**. That choice is what forces every difficulty in the
rest of this document: once your data lives on many machines, you must think about
partitioning, replication, ordering and failure.

### 1.5 The CAP theorem

Once a system is distributed, CAP says you can have at most **two** of:

- **C**onsistency — every read sees the most recent write.
- **A**vailability — every request gets an answer.
- **P**artition tolerance — the system keeps working when the network between machines breaks.

The subtlety students miss: **P is not optional.** Networks *do* fail. So the real choice is
only ever **C or A** during a network partition:

- **CP** — refuse to answer rather than answer with stale data. (Banking.)
- **AP** — answer anyway, possibly with stale data, and reconcile later. (Social feeds.)

**Where Kafka sits:** Kafka is tunable. With `acks=all` (which your producer uses) it leans
**CP** — a write is only acknowledged once all in-sync replicas have it, so the producer waits
rather than risking data loss. With `acks=1` or `acks=0` it leans **AP** — faster, but a broker
failure can lose messages.

> In `src/common/kafka_clients.py`, `"acks": "all"` is a deliberate CAP decision: correctness
> over latency, because losing an order is worse than being slightly slow.

---

## Part 2 — Big data architecture

### 2.1 Lambda architecture

The first serious attempt at "fast *and* correct":

```
                 ┌── batch layer ──▶ complete, accurate, slow (hours)
  raw data ──────┤
                 └── speed layer ──▶ approximate, fast (seconds)
                                          │
                            serving layer merges both
```

The batch layer reprocesses *all* history and is the source of truth. The speed layer covers
only the recent gap. Queries merge the two.

**The flaw:** you write and maintain your business logic **twice**, in two different systems,
and they inevitably drift apart.

### 2.2 Kappa architecture

Kappa's argument: if your stream processor is good enough, delete the batch layer.

```
  raw data ──▶ immutable log (Kafka) ──▶ stream processor ──▶ serving layer
```

Everything is a stream. Need to recompute history? **Replay the log from the beginning** with
new code. One codebase, one system.

This only works because of one property: **the log is durable and replayable**. Kafka does not
delete a message when it is read — that is what makes Kappa possible at all.

> **Your project is Kappa-shaped.** There is no batch layer. If you reset the consumer group's
> offsets, the entire history of orders replays and the average is rebuilt from scratch.

### 2.3 Storage and file formats (context)

Not used directly in this assignment, but examinable:

- **HDFS / cloud object storage (S3)** — store files across many machines with replication.
- **Row formats** (CSV, JSON, Avro) — store record-by-record. Good for *writing* whole records
  and for streaming.
- **Columnar formats** (Parquet, ORC) — store column-by-column. Good for *analytics*: to
  average a price you read only the price column, skipping everything else. Compresses far
  better because similar values sit together.

**Rule of thumb:** Avro for data in motion (streams), Parquet for data at rest (analytics).
Your pipeline is data in motion, hence Avro.

---

## Part 3 — The decoupled pipeline

### 3.1 Point-to-point coupling

The naive design: the order service calls the inventory service, which calls billing, which
calls analytics. With *n* systems you approach *n²* connections. Add one consumer and you must
modify the producer. One slow consumer blocks the producer.

### 3.2 Publish/Subscribe

Insert a broker in the middle:

```
publishers ──▶ [ TOPIC ] ──▶ subscribers
```

The publisher knows nothing about subscribers. Add a fraud detector tomorrow and the producer
does not change by one line. This is **decoupling**, and it is the single most important
architectural idea in this module.

### 3.3 The distributed log — why Kafka is not a queue

This is the distinction that earns marks.

| | Traditional message queue | Kafka (distributed log) |
|---|---|---|
| On read | Message is **deleted** | Message **stays** |
| Position | Broker tracks it | **Consumer** tracks it (offset) |
| Replay | Impossible | Just rewind the offset |
| Multiple consumers | Compete for messages | Each group reads *everything* independently |

A Kafka topic is an **append-only, immutable, ordered log**. Writes go on the end. Nothing is
ever modified. Each record has a permanent position number — its **offset**.

Because a consumer owns its offset:

- Stop the consumer, restart it → it resumes exactly where it stopped.
- Reset the offset to 0 → the entire history replays.
- Two different applications read the same topic without interfering.

> You proved this: after your consumer ran, the committed offsets were 22, 15 and 23 across
> three partitions, with lag 0. Restarting resumed rather than restarted.

---

## Part 4 — Kafka's architecture

### 4.1 The pieces

| Component | Role |
|---|---|
| **Broker** | One Kafka server. A **cluster** is several brokers. |
| **Topic** | A named stream of records (`orders`). |
| **Partition** | A topic is split into partitions — the unit of parallelism and ordering. |
| **Producer** | Writes records. |
| **Consumer** | Reads records. |
| **Consumer group** | A team of consumers sharing the work of one topic. |
| **Offset** | A record's position within a partition. |
| **Controller** | Manages cluster metadata. Historically **ZooKeeper**; modern Kafka uses **KRaft**, where Kafka manages its own metadata. Your `docker-compose.yml` uses KRaft — no ZooKeeper. |

### 4.2 Partitions: parallelism and the ordering trade-off

A topic with 3 partitions is 3 independent logs. This gives parallelism — but at a cost:

> **Kafka guarantees ordering *within* a partition, never *across* partitions.**

If order 1001's "created" event lands in partition 0 and its "cancelled" event in partition 2,
you can process them out of order.

The fix is the **message key**. Kafka hashes the key and always sends the same key to the same
partition:

```
partition = hash(key) mod number_of_partitions
```

> Your producer keys by `orderId`, so every event for one order shares a partition and keeps
> its order. This is why keys exist — not for lookup, but for **ordering and locality**.

### 4.3 Consumer groups

Within a group, **each partition is assigned to exactly one consumer**.

- 3 partitions, 1 consumer → it handles all 3.
- 3 partitions, 3 consumers → one each. Three times the throughput.
- 3 partitions, 4 consumers → **one sits idle forever**.

**Partition count is the hard ceiling on parallelism.** That is why your topics were created
with 3 partitions rather than 1 — it leaves room to scale.

When a consumer joins or dies, Kafka performs a **rebalance**, reassigning partitions.

### 4.4 Replication and durability

Each partition has one **leader** and some **followers**. Writes go to the leader; followers
copy it. Replicas that are caught up form the **ISR** (in-sync replicas).

`acks` controls how long the producer waits:

| Setting | Waits for | Risk |
|---|---|---|
| `acks=0` | Nothing | Fast; messages can vanish |
| `acks=1` | Leader only | Lost if the leader dies before followers copy |
| `acks=all` | All in-sync replicas | Slowest, safest |

> Your producer uses `acks=all`. Note the honest limitation in `docs/architecture.md`: your demo
> cluster has **one** broker and replication factor 1, so `acks=all` is only as safe as that
> single machine. In production you would use 3 brokers with replication factor 3.

---

## Part 5 — Serialization: Avro and schemas

### 5.1 The problem

A message is bytes on a wire. Producer and consumer must agree on what those bytes mean. That
agreement is the **schema**.

With JSON the schema is *implicit* — a convention someone remembers. If a producer renames
`price` to `amount`, nothing stops it. The consumer breaks in production, at 3am.

### 5.2 What Avro gives you

**Compactness.** JSON repeats every field name in every record:

```json
{"orderId":"1001","product":"Item1","price":250.0}     // ~50 bytes
```

Avro writes only the *values*, in schema order, in binary — roughly 15 bytes. Over a billion
messages that difference is enormous, and it is repeated in network, disk, and page cache.

**The wire format** used by Confluent:

```
[ 1 byte: magic 0x00 ][ 4 bytes: schema ID ][ Avro binary payload ]
```

The schema itself never travels. Only a 4-byte pointer to it.

### 5.3 Schema Registry

A server holding the schemas. First time a producer sends, it registers `order.avsc` under the
**subject** `orders-value` and gets back ID 1. Consumers fetch schema 1 once and cache it.

> Confirmed in your project: `curl localhost:8081/subjects` → `["orders-value"]`, version 1,
> id 1.

### 5.4 Schema evolution — the real payoff

Systems change. Avro defines **compatibility rules** the registry *enforces*:

| Mode | Meaning | Safe change |
|---|---|---|
| **BACKWARD** | New consumer can read old data | Add a field **with a default**; delete a field |
| **FORWARD** | Old consumer can read new data | Add a field; delete a field with a default |
| **FULL** | Both | Only add/remove fields with defaults |
| **NONE** | Anything goes | Nothing is checked |

> Yours is set to **BACKWARD**. Adding `currency` with a default `"LKR"` would be accepted —
> old consumers ignore it, new consumers reading old messages get the default. Changing `price`
> from `float` to `string` would be **rejected by the registry**, before it can break anything.

This is **data governance enforced by infrastructure** rather than by a code review someone
might skip — a direct LO-4 point.

---

## Part 6 — Stream processing concepts

### 6.1 Bounded vs unbounded data

- **Bounded** (batch): a finite dataset. You know where it ends. You can sort it, scan it
  twice, compute an exact average.
- **Unbounded** (stream): it never ends. You can never "wait for all the data" — there is no
  all.

This changes everything. You cannot compute an average by summing everything and dividing at
the end, because there is no end. You must maintain the answer **incrementally**.

### 6.2 Event time vs processing time

- **Event time** — when the thing actually happened (the customer clicked "buy").
- **Processing time** — when your system got around to it.

They differ because of network delay, retries, buffering and offline mobile clients. A message
can arrive **late**, or **out of order**.

If you count "orders per minute" by processing time, a network hiccup moves orders into the
wrong minute and your report is wrong. Serious stream processors use event time plus a
**watermark** — a heuristic saying "I believe all events up to time T have now arrived."

> **Honest limitation of your project:** `order.avsc` has no timestamp field, so the pipeline
> cannot do event-time processing. A running average is *order-insensitive* — the mean of the
> same numbers is the same regardless of arrival order — so this does not affect correctness
> here. It would matter immediately for windowed counts. Good viva answer.

### 6.3 Windowing

Since a stream never ends, you slice it:

| Window | Behaviour | Example |
|---|---|---|
| **Tumbling** | Fixed, non-overlapping | Revenue per hour |
| **Sliding** | Fixed size, overlapping | 5-minute average, updated every minute |
| **Session** | Grouped by gaps of inactivity | One user's browsing session |

> Your aggregator uses none of these — it is a **global, unbounded, cumulative** aggregation
> over all messages seen. That is the simplest form of stateful streaming, and it is exactly
> what "running average" means.

### 6.4 Stateless vs stateful

- **Stateless** — each message handled independently (filter, reformat). Trivially parallel.
- **Stateful** — the result depends on previous messages (counts, averages, joins). Now you
  must ask: where does that state live, and what happens when the process dies?

> Your aggregator is **stateful and in-memory**. Kill the consumer and the average resets,
> because `OrderAggregator` is a plain Python object. Production systems persist this in a
> **state store** (Kafka Streams uses RocksDB backed by a changelog topic) so state survives a
> restart. This limitation is documented honestly in `docs/architecture.md` — stating it is
> worth more marks than hiding it.

### 6.5 Why Welford's algorithm

The obvious running average keeps a sum:

```
mean = total_sum / count
```

Correct, but over a long-lived stream `total_sum` becomes very large. Adding a small value to a
very large float loses precision — **catastrophic cancellation**.

Welford's algorithm never materialises the sum:

```
count ← count + 1
δ     ← x − mean
mean  ← mean + δ / count
M2    ← M2 + δ × (x − mean)        variance = M2 / (count − 1)
```

It is numerically stable and gives variance in the same single pass.

> `tests/test_aggregator.py::test_welford_is_stable_over_a_long_stream` demonstrates this with
> 10,000 messages around a large base value.

---

## Part 7 — Delivery semantics and failure

### 7.1 The three guarantees

| Guarantee | Meaning | Cost |
|---|---|---|
| **At-most-once** | Every message delivered 0 or 1 times | Can **lose** data |
| **At-least-once** | Every message delivered 1 or more times | Can **duplicate** data |
| **Exactly-once** | Every message effectively once | Complex and slower |

The choice comes down to **when you commit the offset**:

```
Commit BEFORE processing  →  crash = message skipped        →  at-most-once
Commit AFTER  processing  →  crash = message reprocessed    →  at-least-once
```

Auto-commit (`enable.auto.commit=true`) advances the offset on a **timer**, regardless of
whether processing succeeded — that is silently at-most-once, and it loses data.

> Your consumer sets `enable.auto.commit=false` and commits **synchronously after** each
> message reaches a terminal state (aggregated *or* dead-lettered). That is at-least-once by
> deliberate design.

**The remaining gap, stated honestly:** if the consumer crashes between processing and
committing, that order is counted twice in the average. Closing it requires Kafka
**transactions**, binding the offset commit and the state update into one atomic write.
Justified for financial totals; over-engineering for a monitoring statistic.

### 7.2 Idempotence

An operation is **idempotent** if doing it twice equals doing it once. `SET balance = 100` is
idempotent; `ADD 100 to balance` is not.

At-least-once delivery is only safe if your processing is idempotent. `enable.idempotence=true`
on your producer makes the *producer side* safe: librdkafka attaches a sequence number so a
broker-level retry cannot silently duplicate or reorder a record within a partition.

### 7.3 Transient vs permanent failure — the central idea

Every failure gets one question:

> **Could this message ever succeed if we tried again?**

| | **Transient** | **Permanent** |
|---|---|---|
| Cause lies in | The *environment* | The *message* |
| Examples | Timeout, service busy, deadlock | Negative price, empty product, corrupt bytes |
| Retry helps? | Yes | **Never** |
| Action | Retry with backoff | Straight to the DLQ |

> Encoded as two exception classes in `src/common/errors.py`. `RetryPolicy.run()` re-raises
> `PermanentError` immediately without consuming the retry budget.

### 7.4 The poison pill

Conflating the two causes the classic production incident. A message that can never succeed,
retried forever, **blocks its partition**. Everything behind it stops. One bad record halts a
pipeline — a **poison pill**.

The DLQ exists precisely to get the poison pill out of the way.

### 7.5 Exponential backoff and jitter

```
delay(attempt) = min(base × 2^(attempt−1), ceiling) × random(0.5, 1.0)
```

**Why exponential:** a service that just failed is probably overloaded. Retrying immediately
adds load to something already drowning. Doubling the wait gives it room to recover.

**Why a ceiling:** unbounded doubling means waiting hours.

**Why bounded attempts:** unlimited retry is indistinguishable from a hang. The budget converts
"stuck forever" into "failed, recorded, moved on."

**Why jitter — the part most implementations miss:** if a service blips and 50 consumers fail at
the same instant, a deterministic backoff makes all 50 retry at the *same* instant. The
recovering service is immediately re-killed. This is the **thundering herd**. Randomising the
delay spreads the load out.

### 7.6 In-process retry vs retry topics

Your retry blocks the partition while it sleeps. With a sub-second budget that is fine, and it
preserves ordering.

For **long** backoffs (minutes, hours), blocking a partition is unacceptable. The standard
pattern is **tiered retry topics** — `orders.retry.5m`, `orders.retry.1h` — where the failed
message is republished to a delay topic and the main partition moves on immediately.

> Your `orders.retry` topic is provisioned for exactly this extension but not yet used. Say so
> if asked — knowing where your design stops is an engineering strength.

### 7.7 What makes a DLQ useful

A DLQ that just stores bytes is a graveyard. Two rules make it an operational tool:

1. **Forward the original bytes untouched.** If a message failed to *deserialize*, re-encoding
   it is impossible. And re-encoding one that did decode destroys the evidence.
2. **Attach provenance as headers** — original topic/partition/offset, error type, error
   message, attempt count, consumer group, timestamp. A dead letter must be diagnosable and
   replayable without grepping week-old logs.

> The intended workflow is **inspect → fix root cause → replay**. That is what
> `scripts/inspect_dlq.py` is for.

---

## Part 8 — Where this sits in the wider ecosystem

### 8.1 MapReduce → Spark

**MapReduce** (Google, 2004) made distributed batch processing accessible:

- **Map** — transform each record into key/value pairs, in parallel.
- **Shuffle** — group by key across the cluster.
- **Reduce** — aggregate each group.

Its weakness: it writes intermediate results **to disk** between every stage. Iterative
algorithms (most machine learning) re-read from disk every pass.

**Spark** keeps intermediate data **in memory** and builds a **DAG** of the whole computation
before executing, allowing optimisation across stages — often 10–100× faster.

| Spark API | What it is |
|---|---|
| **RDD** | Low-level distributed collection |
| **DataFrame** | Table-like, with a schema; optimised by Catalyst |
| **Spark SQL** | Actual SQL over DataFrames |
| **MLlib** | Distributed machine learning |
| **Structured Streaming** | Streams treated as infinitely growing tables |

### 8.2 How your project relates

Kafka is the **ingestion** layer; Spark is the **processing** layer. They are complementary,
not competing. A full production pipeline is usually:

```
sources ──▶ Kafka ──▶ Spark Structured Streaming ──▶ warehouse / dashboards
```

Your consumer does the processing directly in Python, which is correct for a running average.
You would reach for Spark when the computation needs **joins across large datasets**,
**machine learning**, or **more throughput than one process can handle**.

**Apache Storm** (in your syllabus) is an older pure-streaming system: **spouts** (sources) and
**bolts** (processors) wired into a **topology**. Historically it offered lower latency than
Spark's micro-batches. Largely superseded by Flink and Kafka Streams.

**Apache Airflow** orchestrates *batch* workflows — DAGs of scheduled tasks with dependencies
and retries. Not used here: your pipeline is event-driven, not scheduled.

### 8.3 Managed services

| Self-managed | Managed equivalent |
|---|---|
| Kafka on your own servers | **AWS MSK**, **Confluent Cloud** |
| Spark on your own cluster | **AWS EMR**, **Databricks** |

The argument: patching brokers, replacing failed disks and tuning JVMs is undifferentiated
work. Paying a provider shifts effort from infrastructure to the application.

The counter-argument: cost at scale, and **vendor lock-in**.

> Worth knowing for the viva: **MSK does not include a Schema Registry.** You would use **AWS
> Glue Schema Registry** or run Confluent's separately. Knowing what a managed service does
> *not* cover is exactly the depth an examiner probes for.

---

## Part 9 — Governance, security and ethics (LO-4)

### 9.1 Data minimisation

Your schema carries `orderId`, `product`, `price` — and nothing else. No name, address or card
number. Every personal field added widens the blast radius of a breach and pulls the topic into
GDPR scope. A narrow schema is a **privacy control**, not an accident.

### 9.2 The DLQ as a retention liability

The DLQ is the one place where messages persist indefinitely, unprocessed — and they are there
*because they were malformed*, which is exactly the kind of record that turns out to contain
something it should not. It needs a retention policy and the same access controls as the main
topic. Commonly overlooked.

### 9.3 Security gaps in this build

Stated plainly, because knowing them is the point:

- **PLAINTEXT, no encryption.** Production needs **TLS** in transit.
- **No authentication.** Production needs **SASL**.
- **No authorization.** Production needs **ACLs** so a compromised consumer cannot publish to
  `orders`.
- **Single broker, replication factor 1.** No fault tolerance.

Kafka's default posture is **open**; security is opt-in. That is worth saying out loud.

### 9.4 Correctness as an ethical property

Silently dropping failed messages — the natural result of auto-commit plus a bare
`except: pass` — means orders vanish with no record. The DLQ exists so failure is **recorded
rather than hidden**, and the number of failed orders becomes an auditable figure instead of an
unknown. "What happened to order 1042?" is a question with real consequences for a real
customer, and your headers can answer it months later.

### 9.5 The right to be forgotten vs the immutable log

A genuine tension worth raising. Kafka's log is **append-only and immutable** — that is its
core value. GDPR grants a right to erasure. These conflict directly.

Practical answers: short retention; **log compaction** with tombstone records; or
**crypto-shredding** — encrypt each subject's data with a per-subject key and delete the key,
rendering the ciphertext permanently unreadable.

---

## Part 10 — Mapping to the learning outcomes

| LO | Covered in |
|---|---|
| **LO-1** — limits of traditional systems; batch, stream, distributed messaging | Parts 1, 2, 3 |
| **LO-2** — apply Kafka/Spark for ingestion and real-time analytics | Parts 4, 5, 6 |
| **LO-3** — architect an end-to-end pipeline | Parts 2, 7, 8 |
| **LO-4** — ethical, security and governance considerations | Part 9, and §5.4 |

---

## Part 11 — Twenty questions you should be able to answer

1. Why is Kafka a log and not a queue? — *§3.3*
2. What does an offset do, and who owns it? — *§3.3, §4.1*
3. Why does your topic have 3 partitions? — *§4.3*
4. Why key messages by `orderId`? — *§4.2*
5. What does `acks=all` cost you, and buy you? — *§4.4*
6. Why Avro instead of JSON? — *§5.2*
7. What actually travels in the message if not the schema? — *§5.2*
8. What is BACKWARD compatibility, and what would it reject? — *§5.4*
9. What is the difference between bounded and unbounded data? — *§6.1*
10. Event time vs processing time — and does it matter here? — *§6.2*
11. Why Welford instead of sum ÷ count? — *§6.5*
12. Which delivery guarantee do you have, and how do you know? — *§7.1*
13. What exactly would break if you enabled auto-commit? — *§7.1*
14. How do you distinguish transient from permanent failure? — *§7.3*
15. What is a poison pill? — *§7.4*
16. Why jitter in the backoff? — *§7.5*
17. Why does the DLQ forward the original bytes? — *§7.7*
18. When would you add Spark to this pipeline? — *§8.2*
19. What does MSK *not* give you? — *§8.3*
20. What are the three biggest limitations of your own system? — *§4.4, §6.4, §9.3*

If you can answer these in your own words, you understand this project.
