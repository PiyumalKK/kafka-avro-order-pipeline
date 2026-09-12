# Architecture and design decisions

Supporting notes for the EC8202 Chapter 3 assignment. The README covers *how to run* the
system; this document covers *why it is built this way*, and is the raw material for the
written report and the viva.

---

## 1. Why Kafka, and why a log

The assignment could have been solved with a message queue that deletes on read. Kafka is a
**distributed, append-only commit log** instead, and that single difference buys three things
this pipeline depends on:

- **Replay.** Offsets are consumer-owned, so stopping the consumer and restarting it resumes
  exactly where it left off — and resetting the group replays history from the beginning.
  A queue that deletes on acknowledgement cannot do this.
- **Decoupling.** The producer knows nothing about the consumer. Adding a second consumer
  group (say, a fraud detector) requires no change to the producer at all.
- **Horizontal scale.** Partitions are the unit of parallelism. Three partitions means up to
  three consumers in a group processing simultaneously, with Kafka handling assignment.

This is the publish/subscribe paradigm over a distributed log, and it is the reason Kafka sits
at the ingestion layer of both Lambda and Kappa architectures.

---

## 2. Why Avro rather than JSON

| | JSON | Avro (Confluent wire format) |
|---|---|---|
| Payload size | Field names repeated in every message | Binary; schema referenced by 4-byte ID |
| Schema | Implicit, by convention | Explicit, registered, versioned |
| Bad data | Discovered downstream, at read time | Rejected at the serialization boundary |
| Evolution | Ad hoc; breaks silently | Compatibility rules enforced by the registry |

On a topic carrying millions of orders, repeating `"orderId"`, `"product"` and `"price"` in
every single record is pure waste — that is storage, network, and broker page cache spent on
punctuation. Avro ships the schema **ID**, not the schema.

The more important benefit is governance. `schemas/order.avsc` is registered under the
`orders-value` subject with **backward** compatibility, so the registry will reject a
producer that tries to publish an incompatible change. The contract is enforced by
infrastructure rather than by a code review that someone might skip.

**Wire format:** `[magic byte 0x00][4-byte schema ID][Avro binary payload]`.

### Schema evolution in practice

Adding an optional `currency` field with a default is backward compatible — old consumers
ignore it, new consumers reading old messages get the default. Removing `price`, renaming
`orderId`, or changing `price` from `float` to `string` are all breaking, and the registry
refuses them.

---

## 3. The failure taxonomy

The central design decision in the whole project is a single question asked of every failure:

> **Could this message ever succeed if we tried again?**

Everything else follows from the answer.

```
                    ┌─ TransientError ──▶ retry with backoff ──▶ still failing? ──▶ DLQ
   failure ─────────┤
                    └─ PermanentError ──▶ DLQ immediately
```

| | Transient | Permanent |
|---|---|---|
| Examples | Downstream timeout, broker unavailable, lock contention | Negative price, empty product, undecodable bytes |
| Cause lies in | The *environment* | The *message* |
| Retrying helps? | Yes, usually | Never |
| Handling | Bounded exponential backoff | Straight to the DLQ |

Conflating the two is the classic production incident. Retrying permanent failures blocks the
partition behind a message that can never succeed — a **poison pill** that stalls the consumer
indefinitely. Not retrying transient failures throws away perfectly good data because a
service was busy for 200 ms. The two exception classes in `src/common/errors.py` make the
distinction explicit and un-skippable, and `RetryPolicy.run()` re-raises `PermanentError`
without touching the retry budget.

---

## 4. Retry policy

```
delay(attempt) = min(base × 2^(attempt−1), ceiling) × uniform(0.5, 1.0)
```

With the defaults (base 0.4 s, ceiling 6 s, 3 attempts) the nominal waits are 0.4 s and 0.8 s,
each scaled by jitter.

**Why exponential:** a service that just failed is probably overloaded. Backing off linearly
keeps the pressure on; backing off exponentially gives it room to recover.

**Why jitter:** this is the part most implementations miss. If a downstream service blips and
fifty consumers all fail at the same instant, a deterministic backoff makes all fifty retry at
the *same* instant — a synchronised thundering herd that re-kills the service just as it comes
back. Randomising the delay spreads the load.

**Why bounded:** unbounded retry is indistinguishable from a hang. The budget is what converts
"stuck forever" into "failed, recorded, and moved on".

### In-process retry vs a retry topic

This implementation retries **in-process**, blocking the partition for the duration. That is
the right call here: the retry budget is sub-second and the ordering guarantee is preserved.

For longer backoffs (minutes or hours), blocking a partition is unacceptable and the standard
pattern is a **tiered retry topic** — `orders.retry.5m`, `orders.retry.1h` — where failures are
republished with a delay and the main partition moves on immediately. The `orders.retry` topic
is provisioned here for exactly that extension.

---

## 5. Dead Letter Queue design

Two rules separate a useful DLQ from a bin of mystery bytes.

**Rule 1 — forward the original bytes untouched.** If the value failed to deserialize,
re-encoding it is literally impossible. And re-encoding a message that *did* decode would
discard the exact bytes that caused the problem, destroying the evidence. `DeadLetterPublisher`
copies `message.value()` verbatim.

**Rule 2 — attach provenance as headers.** A dead letter must be diagnosable and replayable on
its own, without anyone grepping consumer logs from three days ago. Headers are used rather
than wrapping the payload in an envelope, precisely so rule 1 stays intact.

| Header | Why it matters |
|---|---|
| `x-original-topic` / `-partition` / `-offset` | Locates the exact source record for replay |
| `x-error-type` | Lets you filter "bad data" from "downstream was down" |
| `x-error-message` | The human-readable cause (truncated to 900 bytes) |
| `x-retry-attempts` | Distinguishes "never tried" from "tried hard and failed" |
| `x-consumer-group` | Identifies which consumer gave up, once there are several |
| `x-failed-at` | Correlates against downstream incident timelines |

A DLQ is an operational tool, not a graveyard: the intended workflow is to inspect it
(`scripts/inspect_dlq.py`), fix the root cause, and replay the recoverable messages back onto
the main topic.

---

## 6. Delivery semantics

**At-least-once**, deliberately.

Producer side: `acks=all` waits for all in-sync replicas, and `enable.idempotence=true` lets
librdkafka retry internally without duplicating or reordering records within a partition.

Consumer side: `enable.auto.commit=false`, with a synchronous commit issued **only after** the
message reaches a terminal state — aggregated or dead-lettered. Auto-commit would advance the
offset on a timer regardless of whether processing succeeded, so a crash would silently skip
messages. That is at-most-once, and it loses data.

**The remaining gap:** a crash in the window between processing a message and committing its
offset causes that message to be replayed and counted twice in the average. Closing it requires
Kafka transactions binding the offset commit and the aggregate update into one atomic write —
justified for financial totals, over-engineering for a monitoring statistic. The trade-off is
stated rather than hidden.

---

## 7. Real-time aggregation

`OrderAggregator` maintains the running average with **Welford's online algorithm**:

```
count ← count + 1
δ     ← x − mean
mean  ← mean + δ / count
M2    ← M2 + δ × (x − mean)        → variance = M2 / (count − 1)
```

A naive `sum / count` gives the same answer and is easier to read, so the choice needs
justifying. Two reasons:

1. **Numerical stability.** A stream that runs for days accumulates a very large sum; adding
   small increments to it loses precision in float arithmetic. Welford never materialises the
   sum. `test_welford_is_stable_over_a_long_stream` demonstrates this with 10,000 messages
   around a large base value.
2. **Variance for free**, in one pass — which is what you actually need for anomaly detection
   on a live stream.

Both a global aggregate and a per-product breakdown are maintained, giving the demo something
substantive to show rather than a single number.

**Scope limit, stated honestly:** this aggregate is per-process. With three consumers in the
group, each holds the average of its own partitions. Making it globally correct means either a
Kafka Streams state store keyed by product, or publishing partial aggregates to a compacted
topic and folding them downstream. That is beyond the assignment's scope but is the right next
step and a likely viva question.

---

## 8. Why failures are injected deliberately

`src/producer/main.py` emits invalid orders at a configurable rate, and
`src/processing/inventory.py` fails a configurable fraction of downstream calls, with `Item7`
and `Item8` hard-wired to a 60% failure rate.

This is not padding. Retry logic and a DLQ are **only** observable when things break. A demo
run on clean data proves nothing about either — the code paths never execute. Controllable
fault injection makes the two headline features reproducible on demand, in front of an
examiner, in under a minute. It is also how these paths would be tested in production, via
chaos engineering.

---

## 9. Ethics, security and governance

Addressing LO-4 directly.

**Data minimisation.** The schema carries `orderId`, `product`, `price` and nothing else. No
customer name, address, or payment detail. A real order pipeline is under constant pressure to
add "just one more field" for convenience, and every added personal field widens the blast
radius of a breach and pulls the topic into GDPR scope. The narrow schema is a deliberate
privacy control, not an accident of the assignment.

**The DLQ is a data-retention liability.** It is the one place in the system where messages
persist indefinitely, un-processed, and it exists precisely because those messages were
malformed — which is exactly the sort of record that turns out to contain something it should
not. A production deployment needs a retention policy and the same access controls as the main
topic. It is a common blind spot.

**Security gaps in this build, stated plainly.** The stack runs `PLAINTEXT` with no
authentication because it is a single-node local demo. Production requires TLS in transit,
SASL authentication, and topic-level ACLs so that a compromised consumer cannot publish to
`orders`. Kafka's default posture is open; security is opt-in, and that is worth knowing.

**Correctness as an ethical property.** Silently dropping failed messages — the natural
consequence of auto-commit plus a bare `except: pass` — means orders vanish with no record.
The DLQ exists so that failure is *recorded* rather than hidden, and so the number of failed
orders is an auditable figure rather than an unknown one.

**Operational transparency.** Every dead letter carries a timestamp, an error, and its exact
origin. That audit trail is what makes it possible to answer "what happened to order 1042?"
months later — a question with real consequences for a customer.

---

## 10. Mapping to module learning outcomes

| LO | Where it is demonstrated |
|---|---|
| **LO-1** — limitations of traditional systems; batch, stream and distributed messaging paradigms | §1 (log vs queue, partitions, replay); at-least-once vs exactly-once in §6 |
| **LO-2** — apply industry-standard frameworks for ingestion and real-time analytics | Kafka producer/consumer with Schema Registry; running average in §7 |
| **LO-3** — architect an end-to-end pipeline integrating multiple technologies | Kafka (KRaft) + Schema Registry + Avro + Kafka UI, one-command Docker Compose stack |
| **LO-4** — evaluate ethical, security and governance considerations | §9 in full; schema compatibility enforcement in §2 |

---

## 11. Known limitations

Listing these is deliberate — knowing where a system stops is part of engineering it.

1. Aggregation is per-consumer-process, not global across the group (§7).
2. Retry is in-process and blocks the partition; fine at sub-second budgets, wrong at minutes (§4).
3. The stack is single-broker with replication factor 1 — no fault tolerance, by design, for a laptop demo.
4. No TLS, SASL or ACLs (§9).
5. `orders.retry` is provisioned but not yet used; it is the hook for tiered retry.
6. Aggregate state is in memory and lost on restart. Durable state would mean a Kafka Streams store or an external key-value store.
