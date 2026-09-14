# Runbook — state growing unbounded

**Symptom:** `p3_state_rows` or `p3_state_bytes` rising without a plateau, or
executor memory pressure on the streaming job.

**Severity:** high. Unbounded state ends in an OOM that takes down every account
sharing the executor, not just the one causing it.

---

## First, read the right number

`p3_state_rows` counts **accounts held in state — one row per key**. The pending
reorder buffer is a pickled blob *inside* one of those rows, so a single account
holding 10,000 buffered events still shows as **one** row.

If you are chasing a buffer problem, the metric is `p3_buffer_p99` /
`p3_buffer_max`, not state rows. Getting this backwards costs an hour; it cost one
during the build.

---

## Decision tree

### 1. Are state ROWS rising, or is the BUFFER rising?

```
rows rising, buffer flat   -> accounts are not being evicted   -> go to (2)
buffer rising, rows flat   -> one or few accounts reordering    -> go to (3)
both rising                -> traffic growth, or both problems  -> do (2) then (3)
```

### 2. Rows rising — TTL is not evicting

```promql
p3_state_rows                  # rising with no plateau
rate(p3_ttl_flush[5m])         # should be non-zero on any workload with idle accounts
```

**If `p3_ttl_flush` is flat at zero, the TTL is not firing.** In order:

- **Is `state_ttl` configured?** `grep state_ttl conf/engine_config.yml`. With
  `state_ttl_ms = 0` the alarm is never armed and nothing is ever evicted.
- **Is event time advancing?** The TTL is measured **in event time against the
  watermark**, never wall clock. A stalled producer freezes the watermark, and a
  frozen watermark can never pass `last_seen + TTL` — so nothing is evicted however
  long you wait. Check `p3_state_rows` against Kafka partition lag: a stalled
  upstream shows as lag rising while state rows plateau and then never fall.
- **Is the TTL simply longer than the idle period?** Demo 120 s, production 24 h.
  An account quiet for 6 hours is not evicted under a 24-hour TTL, and should not be.

**If `p3_ttl_flush` is non-zero but rows still climb**, eviction is working and
genuine account growth is outpacing it. That is a capacity decision, not an
incident: 10M accounts at 200–500 bytes is 2–5 GB, comfortable across 8–12
executors. Re-examine at 100M.

### 3. Buffer rising — reordering or an attack

```promql
p3_buffer_p99                  # > 100 across many accounts is the signal
p3_accounts_at_cap             # any non-zero means events are being shed
rate(p3_overflow_rate[5m])     # the DLQ rate
```

**`p3_buffer_p99 > 100` across many accounts** means either the watermark is too
large for the traffic, or upstream is producing out-of-order bursts. Correlate with
producer deploys. Shortening the watermark reduces buffering — **and increases
false-positive gaps**, which under `FLAG_AND_CONTINUE` costs real balance. Do not
tune it without reading `docs/watermark_and_gap_semantics.md` §5.

**A single account at the cap** is a replay loop or an attack. The DLQ overflow
should already have fired:

```bash
python scripts/consume_topic.py --topic accounts.dlq --count-only
```

That account degrades **itself** — per-key state means its neighbours keep
advancing. Confirm before escalating: check that other accounts on the same
partition are still advancing their `last_applied_seq`.

---

## Actions

| Situation | Action |
|---|---|
| TTL not configured | set `state_ttl`, redeploy. **Requires a fresh checkpoint** — see `spark_upgrade.md` |
| Watermark frozen | fix the upstream stall. Nothing downstream will help |
| Genuine account growth | scale executors; state is ~200–500 bytes per account |
| One account at the cap | check the DLQ, find the producer, rate-limit it upstream |
| Buffer high everywhere | check for a producer change; do **not** shorten the watermark reflexively |

## Do not

- **Do not raise `max_buffer_size` to make the symptom go away.** The cap is what
  turns an unbounded per-account buffer into a bounded one. Raising it converts a
  DLQ record you can reconcile into an OOM you cannot.
- **Do not shorten the watermark to reduce buffering.** Measured on this system: a
  5-second watermark against a 30-second one, same input, produced 6 false gaps and
  lost 787.04 of balance.
