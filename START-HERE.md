# START HERE — what this project is, in plain English

You are looking at the EC8202 Chapter 3 take-home assignment. This file explains what was
built, what all the strange words mean, and exactly what you still have to do.

Read this first. The `README.md` is the technical version for your lecturer.

---

## 1. What the assignment asked for

The PDF asked for five things. All five are done:

| # | What was asked | Done? | Where it lives |
|---|---|---|---|
| 1 | Send and receive **order messages** through Kafka | ✅ | `src/producer/`, `src/consumer/` |
| 2 | Use **Avro** to encode the messages | ✅ | `schemas/order.avsc` |
| 3 | **Running average** of prices, in real time | ✅ | `src/processing/aggregator.py` |
| 4 | **Retry** when something fails temporarily | ✅ | `src/processing/retry.py` |
| 5 | **Dead Letter Queue** for messages that can never work | ✅ | `src/consumer/dlq.py` |

Plus the two delivery requirements: a **Git repository** (done, on GitHub) and a **live
demonstration** (scripted for you — see section 6).

---

## 2. What the system actually does

Imagine an online shop. Every time someone buys something, an "order" is created.

1. A **producer** program invents orders (`orderId`, `product`, `price`) and sends them.
2. They go into **Kafka**, which is like a queue that keeps the messages safe.
3. A **consumer** program reads them one by one.
4. For each order, the consumer:
   - checks the order makes sense (is the price positive?)
   - calls a pretend "inventory service" to reserve the stock
   - adds the price into a **running average** shown live on screen

Then there are the two interesting cases — what happens when something goes wrong:

- **The inventory service is temporarily down.** Not the order's fault. So the consumer
  **waits and tries again**, up to 3 times. This is the *retry*.
- **The order itself is broken** (price is negative, product name is empty). Trying again
  will never help — a negative price stays negative forever. So the order is moved to a
  special "failed" box called the **Dead Letter Queue**.

That is the whole system.

---

## 3. The words explained

These are the terms your lecturer will expect you to know.

| Word | What it means, simply |
|---|---|
| **Kafka** | A system that carries messages between programs and stores them safely. Like a post office that never loses a letter and remembers every letter it delivered. |
| **Topic** | A named channel inside Kafka. We use three: `orders`, `orders.retry`, `orders.dlq`. |
| **Producer** | The program that *sends* messages. |
| **Consumer** | The program that *reads* messages. |
| **Avro** | A compact way of packing data into bytes. Smaller and stricter than JSON. |
| **Schema** | The agreed shape of a message. Ours says: orderId (text), product (text), price (number). It is the file `schemas/order.avsc`. |
| **Schema Registry** | A server that stores the schema. Messages carry a tiny 4-byte ID instead of the full schema, which saves a lot of space. |
| **Running average** | The average price, recalculated every time a new order arrives — not at the end. |
| **Retry** | Trying a failed operation again after a short wait. |
| **Backoff** | Waiting *longer* each time you retry (0.4s, then 0.8s, then 1.6s). Gives the broken service time to recover. |
| **Jitter** | Adding a small random amount to the wait, so many programs don't all retry at the exact same moment and crash the service again. |
| **DLQ (Dead Letter Queue)** | A separate topic where hopeless messages are parked, with a note explaining why they failed. |
| **Offset** | A bookmark. It records how far the consumer has read, so it can continue after a restart instead of starting over. |
| **Partition** | Kafka splits a topic into parts so several consumers can work at the same time. Ours has 3. |

---

## 4. What's in the folder

You do not need to understand every file. These are the ones that matter:

```
START-HERE.md          <- this file
README.md              <- the technical explanation (for your lecturer)
docs/demo-script.md    <- YOUR SCRIPT for the demo. Very important.
docs/theory.md         <- THE THEORY. Concepts explained, tied to this code.
docs/architecture.md   <- why each decision was made (for the viva and report)

schemas/order.avsc     <- the message shape the assignment specified
docker-compose.yml     <- starts Kafka with one command

src/producer/main.py   <- sends orders
src/consumer/main.py   <- reads orders, retries, aggregates, dead-letters
src/processing/        <- the three required features live here
scripts/inspect_dlq.py <- shows what is inside the Dead Letter Queue
tests/                 <- 32 automatic tests
```

---

## 5. How to run it

You need **Docker Desktop running**. Then open PowerShell in this folder.

**Step 1 — start Kafka** (only once; leave it running)

```powershell
docker compose up -d
python scripts/create_topics.py
```

**Step 2 — open TWO PowerShell windows side by side.**

In window 1 (the consumer — this shows the live dashboard):

```powershell
.\.venv\Scripts\python.exe -m src.consumer.main
```

In window 2 (the producer — this sends the orders):

```powershell
.\.venv\Scripts\python.exe -m src.producer.main
```

Watch window 1. The running average updates on every message. Yellow lines are retries.
Red lines are messages going to the Dead Letter Queue.

**Step 3 — look inside the Dead Letter Queue**

```powershell
.\.venv\Scripts\python.exe scripts\inspect_dlq.py
```

**To stop everything:** press Ctrl+C in both windows, then `docker compose down`.

**Useful extras:**

```powershell
.\.venv\Scripts\python.exe -m pytest        # run the 32 tests
```
Kafka UI (a web page showing the topics): **http://localhost:8090**

---

## 6. What YOU still need to do

Everything is built and working. Three things are left, and they are all yours:

### ☐ 1. Confirm how the demo happens — do this first
The assignment says *"demonstrates the system live"*. That usually means **in person**, in
front of your lecturer — not a recorded video. **Ask your lecturer which one they want.**
Do not assume it is a video. If you prepare the wrong thing you lose marks for no reason.

### ☐ 2. Practise the demo
Open `docs/demo-script.md`. It has 10 scenes, the exact commands to type, and **what to say**
for each one. Run through it once or twice before the real thing. It takes about 6 minutes.

If you are recording a video: use **Win + G** (Xbox Game Bar) for one window, or install
**OBS Studio** if you want both terminals and the browser visible together. I cannot record
your screen — that part has to be you.

### ☐ 3. Be ready for questions
The end of `docs/demo-script.md` lists the 6 questions you are most likely to be asked, with
the answers. The most important ones:

- *Why Avro and not JSON?* → Smaller messages, and the schema is enforced automatically.
- *Why not retry everything?* → A broken message retried forever blocks everything behind it.
- *Why jitter?* → So all the consumers don't retry at the same instant and crash the service again.

### ☐ 4. Read the theory
`docs/theory.md` explains the concepts properly — why Kafka is a log and not a queue, what the
CAP theorem means, why Avro beats JSON, at-least-once vs exactly-once delivery, why jitter
exists, and how all of it maps to the module learning outcomes. It ends with 20 questions you
should be able to answer. If you only read one thing to *understand* the project, read that.

**Optional:** the module also has a Mini Project Report. `docs/architecture.md` is written so
you can reuse it — it covers the design decisions, the trade-offs, and a section on ethics,
security and governance that maps to the module's learning outcomes.

---

## 7. If something breaks

| Problem | Fix |
|---|---|
| `Connection refused` | Docker isn't running, or Kafka isn't ready. Run `docker compose ps` and wait for `healthy`. |
| Consumer shows nothing | It already read everything. Send more orders with the producer. |
| Port already in use | Something else is using that port. Kafka UI was already moved from 8080 to 8090 for this reason. |
| Want to start completely fresh | `docker compose down -v` then `docker compose up -d` then `python scripts/create_topics.py` |

---

## 8. Current status

Last verified, with everything running:

- 3 services healthy: Kafka, Schema Registry, Kafka UI
- 3 topics created, schema registered
- 120 orders sent, 23 correctly parked in the Dead Letter Queue
- Retries observed and recovering
- 32 of 32 tests passing
- Code pushed to GitHub, clean history

The repository: **https://github.com/PiyumalKK/kafka-avro-order-pipeline**
