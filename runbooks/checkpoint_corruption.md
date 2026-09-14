# Runbook — checkpoint corruption

**Symptom:** the engine fails on a batch with a state-store read error —
`EOFException`, a checksum failure, "Failed to read", or an invalid RocksDB SST.
The query terminates and does not recover on restart.

**Severity:** critical. The job is down and will stay down until the checkpoint is
replaced. **Do not keep restarting it** — a corrupt checkpoint fails identically
every time and each attempt costs minutes.

---

## What has actually happened

A file under `checkpoints/<tag>/balance_engine/state/` is unreadable. Spark's own
durability — RocksDB snapshots plus per-batch changelogs inside the checkpoint —
cannot help here, because the corruption is *in that directory*.

This is the single failure the snapshot layer exists for, and nothing else.

> **A snapshot IS** a point-in-time copy of the **durable** checkpoint, Kafka
> offsets included. That is what makes "restore plus bounded replay" possible.
>
> **A snapshot IS NOT** a backup of the executor-local RocksDB directory, which is
> ephemeral by design and worthless to copy.

Getting that backwards produces a backup strategy that protects nothing.

---

## Procedure

### 1. Stop. Do not restart into the corruption.

```bash
./scripts/stop_engine.sh
```

Record the wall clock. `T_recover` is measured from here.

### 2. Find the newest snapshot

```bash
./scripts/snapshot_state.sh --list
```

Check `p3_snapshot_lag_seconds` on the State Health dashboard. The lag at the moment
of failure **is your replay window** — 30 minutes of snapshot cadence means at most
30 minutes of Kafka to re-read.

**If there are no snapshots**, skip to *No snapshot* below.

### 3. Restore to a FRESH path. Never over the corpse.

```bash
./scripts/restore_from_snapshot.sh
```

Restoring onto the broken checkpoint destroys the evidence and risks mixing good
files with bad. Immutability is half the value: the corrupt state is still there to
look at afterwards, and root-causing a corruption you have already overwritten is
guesswork.

### 4. Let the bounded replay absorb the overlap

The restored checkpoint contains the Kafka offsets as of the snapshot, so only
records **after** those offsets are re-read. The overlap is absorbed by the same
three layers that survive a SIGKILL:

1. state and offsets restore **atomically** — the state matches the offsets exactly
2. the state machine's **drop branch** — `seq <= last` → `DUP_DROPPED`, counted
3. the sink's **strict-`>` MERGE guard** — a replayed batch is a no-op

`DUP_DROPPED` rows during recovery are **healthy**. They are the evidence layer 2
engaged.

### 5. Verify before declaring recovery

```bash
python scripts/parity_balance.py
python scripts/diagnose.py
```

Parity against the oracle is the acceptance test. `T_recover` ends when it passes.

---

## No snapshot exists

Replay from earliest, **throttled**:

```bash
P3_MAX_OFFSETS_PER_TRIGGER=2000 ./scripts/run_engine.sh
```

The throttle matters. An unthrottled replay from earliest pulls the full retention
window in the first few batches, spikes consumer lag across every partition, and can
push other consumers of the same topic into their own incident. Recovering slowly on
purpose is correct here.

Expect **hours rather than minutes**, proportional to retention (7 days configured).

---

## Measured on this system

| | |
|---|---|
| Recovery with snapshot | restore + bounded replay + parity |
| Recovery from earliest | full retention replay, throttled |
| Production projection | bounded 30-minute replay vs 7-day replay |

Record your own numbers from `logs/chaos.jsonl` — the drill writes
`{"drill":"corruption","t_snapshot_s":…,"t_scratch_s":…}`.

---

## Prevention

- **Alert on `p3_snapshot_lag_seconds`** above 2× the cadence. A steadily rising
  line means the DAG has stopped and your recovery window is growing silently —
  which you discover during the incident, at the worst possible moment.
- Snapshot prefixes are **immutable and versioned**. Never `mc mirror --remove`.
- Retention: 7 days production, newest 12 in the demo.
