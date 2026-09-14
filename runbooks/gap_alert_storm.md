# Runbook — SEQUENCE_GAP alert storm

**Symptom:** `p3_gap_rate` spiking, many `SEQUENCE_GAP` records across accounts.

**Severity:** high, and **ambiguous** — this is the one alert that looks identical
whether the cause is upstream or catastrophic.

---

## Upstream first. Always.

> **A gap storm is almost always upstream.** A stopped producer or a dead partition
> gaps a whole account range at once, and the engine reports exactly what it sees:
> sequences that never arrived. Correlate with **Kafka partition lag before
> declaring data loss.**

The engine cannot distinguish "the producer stopped" from "the events were lost in
transit". Only the lag can.

---

## Decision tree

### 1. Correlate with partition lag

```bash
docker exec p3-kafka kafka-consumer-groups --bootstrap-server kafka:9092 \
  --describe --all-groups | grep accounts.events
```

| Lag | Gaps | Meaning |
|---|---|---|
| **rising** | rising | a **consumer** problem — the engine is behind, not losing. Gaps are premature; see (3) |
| **flat at zero** | rising | the engine is caught up and events genuinely never arrived → (2) |
| **flat, producer idle** | rising | the producer stopped. The gap range names exactly what was missed |

### 2. Lag flat, gaps persist — where did they go?

**Check whether you shed them yourself.** A `SEQUENCE_GAP` range and the
`BUFFER_OVERFLOW` records inside it describe the **same incident twice**, and read
together they look like double the loss:

```bash
python scripts/consume_topic.py --topic accounts.dlq --count-only
python scripts/consume_topic.py --topic accounts.integrity --filter-kind SEQUENCE_GAP
```

Measured on this system during the Stage-5 burst: `SEQUENCE_GAP(1, 9001)` alongside
9,000 `BUFFER_OVERFLOW` rows. That reads as 18,001 missing events; the true figure
is **9,001**, and only seq 1 was genuinely lost upstream — the other 9,000 are
sitting in the DLQ, recoverable.

**Subtract the DLQ from the gap range before reporting a loss figure.**

If the gap range is *not* covered by DLQ evictions, the events were lost before the
engine saw them: check producer logs for send failures and the topic's retention.

### 3. Lag rising — the gaps may be false

An engine far behind still advances its watermark from the data it *has* processed.
If a whole partition is stalled while others flow, the watermark can pass a missing
sequence that is simply still queued.

```bash
python scripts/wait_for_drain.py --query balance_engine --timeout 600
```

**Let it catch up before acting.** Then re-check whether the gaps persist.

### 4. Lag flat, gaps persist, DLQ empty — upstream sequencing

The producer's `seq_no` generation is broken: duplicate sequences, a reset counter,
or a gap introduced at the source. Check for a producer deploy correlating with the
onset. This is the only branch where the engine is reporting a genuine upstream
defect and there is nothing downstream to do about it.

---

## Reading the range

Gaps are **coalesced**. One record per hole, never one per missing sequence:

```json
{"lo": 101, "hi": 150, "count": 50, "successor_seq": 151,
 "alarm_event_ts": 1767225612345, "watermark_ms": 1767225642100}
```

A 50-event outage is **one** alert with `count: 50`, so the blast radius is legible
at a glance. `alarm_event_ts` tells you when the missing event should have existed;
`watermark_ms` tells you when the engine declared it lost. The difference is bounded
by `watermark + trigger`.

---

## Actions

| Cause | Action |
|---|---|
| Consumer lag | let it drain, then re-assess. Do not act on gaps from a backlogged engine |
| Producer stopped | restart it. Under `FLAG_AND_CONTINUE` the engine has already stepped over; the missing amounts are permanently excluded |
| Shed to the DLQ | reconcile from `accounts.dlq`. Not lost — recoverable |
| Upstream seq bug | escalate to the producer team with the coalesced ranges |
| False positives on reorder | the watermark is too short. See `docs/watermark_and_gap_semantics.md` §5 |

## Policy note

Under **`FLAG_AND_CONTINUE`** the engine has already stepped over the hole and the
missing amounts are excluded from the balance permanently. Replaying the events
later will **not** correct it: they arrive as `seq <= last` and are dropped, by
design — the watermark's verdict is final. Correction is a reconciliation job, not a
replay.

Under **`HOLD`** the account is frozen and will resume when the hole is filled, but
its buffer is growing against the cap the whole time and will eventually overflow to
the DLQ. **Bounded memory beats unbounded hope**, but a held account is a clock
running.
