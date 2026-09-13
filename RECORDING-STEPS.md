# How to record the demo — exact steps

Follow this top to bottom. Do not skip Part A.

Total time: 15 minutes preparation, 6 minutes recording.

---

## PART A — Before you press record

### A1. Start Docker Desktop

Open Docker Desktop from the Start menu. **Wait** until the whale icon at the bottom-left is
green and says "Engine running". This takes 1–2 minutes.

### A2. Open PowerShell in the project folder

Open File Explorer, go to:

```
D:\Final year\Big Data\Take Home\kafka-avro-order-pipeline
```

Click the address bar, type `powershell`, press Enter.

### A3. Wipe the old data so the demo starts clean

**This matters.** There are 120 old orders and 23 old failures from testing. If you don't
clear them, your recording starts with numbers already on screen and looks confusing.

Copy-paste this, one line at a time:

```powershell
docker compose down -v
```

```powershell
docker compose up -d
```

Now **wait 60 seconds.** Then:

```powershell
.\.venv\Scripts\python.exe scripts\create_topics.py
```

You should see:

```
  + created orders (3 partitions)
  + created orders.retry (3 partitions)
  + created orders.dlq (3 partitions)
```

If you see an error, wait another 30 seconds and run it again.

### A4. Open a SECOND PowerShell window

Do step A2 again. You now need **two** PowerShell windows.

Put them **side by side**:
- Click the first window, press `Win + Left Arrow`
- Click the second window, press `Win + Right Arrow`

**LEFT window = consumer** (the dashboard)
**RIGHT window = producer** (sends orders)

### A5. Open the browser

Open Chrome and go to:

```
http://localhost:8090
```

Leave it on that tab. Don't record yet.

### A6. Do one practice run

Run through Part B once **without recording**. This is the most important step. It takes 6
minutes and it will save you from restarting the recording five times.

After practising, redo **A3** to wipe the data clean again.

---

## PART B — Recording

### How to start recording

Press `Win + Shift + S`. In the small toolbar at the top, click the **video camera icon**.
Click **New**. Drag a box around your whole screen. Click **Start**.

If you want to talk over it, click the **microphone icon** to turn it on before pressing Start.

> Alternative: press `Win + Alt + R` (Xbox Game Bar). But Game Bar can stop when you switch
> windows, so the Snipping Tool method above is safer for this demo.

---

### Scene 1 — Show the system is up (20 seconds)

In the **RIGHT** window, type:

```powershell
docker compose ps
```

**Say:** "This is my pipeline running in Docker — Kafka, the Schema Registry, and a web UI."

---

### Scene 2 — Show the message schema (20 seconds)

In the **RIGHT** window:

```powershell
type schemas\order.avsc
```

**Say:** "This is the Avro schema the assignment asked for — orderId, product, and price."

---

### Scene 3 — Start the consumer (20 seconds)

In the **LEFT** window:

```powershell
.\.venv\Scripts\python.exe -m src.consumer.main
```

A dashboard appears with everything at zero.

**Say:** "This is my consumer. It's waiting. The running average is zero because no orders have
arrived yet."

---

### Scene 4 — Start the producer (60 seconds)

In the **RIGHT** window:

```powershell
.\.venv\Scripts\python.exe -m src.producer.main --interval 0.3
```

**Say:** "Now I'm sending orders. Watch the running average on the left — it updates on every
single message. That's the real-time aggregation."

**Let it run for a full minute.** Point your mouse at the RUNNING AVERAGE number.

---

### Scene 5 — Point out the retries (30 seconds)

Look at the **LEFT** window for **yellow** lines that say `retrying in ...`.

**Say:** "These yellow lines are my retry logic. The inventory service failed temporarily, so
it waits and tries again. Each wait is longer than the last, with a small random amount added
so that many consumers don't all retry at the same instant."

Also point at **"recovered by retry"** in the table.

**Say:** "These recovered — they succeeded on a later attempt."

---

### Scene 6 — Point out the Dead Letter Queue (30 seconds)

Look at the **LEFT** window for **red** lines that say `DLQ <-`.

**Say:** "These red lines are permanently broken orders — a negative price, or an empty product
name. Retrying can never fix those, so instead of blocking the pipeline they go straight to the
Dead Letter Queue."

---

### Scene 7 — Stop and show the summary (20 seconds)

Click the **RIGHT** window, press `Ctrl + C` to stop the producer.
Click the **LEFT** window, press `Ctrl + C` to stop the consumer.

A final report prints.

**Say:** "Here's the summary — how many were processed, how many retried, and how many were
dead-lettered."

---

### Scene 8 — Open the Dead Letter Queue (40 seconds)

In the **LEFT** window:

```powershell
.\.venv\Scripts\python.exe scripts\inspect_dlq.py
```

A table appears showing every failed order and **why** it failed.

**Say:** "I can read the Dead Letter Queue back. Every failed message keeps its original
position and the exact reason it failed, so it can be diagnosed and replayed later."

---

### Scene 9 — Show the browser (30 seconds)

Switch to Chrome (`http://localhost:8090`).

Click **Topics**. Click **orders.dlq**. Click the **Messages** tab.

**Say:** "This is the Kafka UI showing the same dead letter queue, and the headers attached to
each failed message."

---

### Scene 10 — Run the tests (20 seconds)

Back in the **LEFT** window:

```powershell
.\.venv\Scripts\python.exe -m pytest
```

**Say:** "And 32 unit tests covering the aggregation, the validation rules, the retry policy
and the dead letter queue."

---

### Stop recording

Click the **Stop** button in the recording toolbar. Save the video somewhere you'll find it.

---

## PART C — After recording

1. **Watch your video back.** Check the text is readable and the audio works.
2. If the text is too small, increase the PowerShell font: right-click the title bar →
   Properties → Font → size 18 or 20. Then record again.
3. Submit the video **and** the GitHub link:
   `https://github.com/PiyumalKK/kafka-avro-order-pipeline`

---

## If something goes wrong

| Problem | Fix |
|---|---|
| `Connection refused` | Docker isn't ready. Wait 60 seconds, try again. |
| Consumer shows nothing | The producer isn't running, or all orders were already read. Run A3 to reset. |
| No yellow retry lines appear | Add `--transient-fault-rate 0.5` to the consumer command. |
| No red DLQ lines appear | Add `--permanent-fault-rate 0.3` to the producer command. |
| Dashboard looks broken | Make the PowerShell window bigger, or maximise it. |
| I want to start over | Run the three commands in A3 again. |

---

## The three sentences that matter most

If you remember nothing else, remember these:

1. **"The running average updates on every message"** — that's the real-time aggregation.
2. **"Temporary failures are retried with increasing waits"** — that's the retry logic.
3. **"Permanently broken messages go to the Dead Letter Queue instead of blocking everything"**
   — that's the DLQ.

Those are the three things the assignment asked for.
