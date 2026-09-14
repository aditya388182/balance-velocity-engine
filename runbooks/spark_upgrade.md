# Runbook — upgrading Spark (state format evolution)

**When:** any Spark version bump, any change to `STATE_SCHEMA`, any change to the
stateful operator's layout, and any change to the query plan that alters operator
IDs.

**Severity if done wrong:** Severity 1, with hours of downtime. Spark's RocksDB
state store format changes across versions, and a new job pointed at an old
checkpoint fails to start — or, worse, starts and misreads.

---

## The one rule

> **The new job NEVER reads old-format state.**

It is seeded from a **snapshot plus a bounded Kafka replay**, which is the only path
that survives an actual format break. Everything below is in service of that
sentence.

This is the same shape as a database migration: **dual-run, validate, cut over.**

---

## Prerequisite, and it is not optional

Checkpoint paths must **already** be versioned:

```
s3a://balance-lake/checkpoints/v3.5.1/balance_engine/
```

That string is built in exactly one place — `conf/config.py::checkpoint_path` — and
has been in use since the job's first run. **A drill that introduces versioning on
the day of the upgrade proves nothing**, because the state you need to migrate is
already sitting in an unversioned path.

If your paths are not versioned yet, that migration is itself this procedure, with
the old path as "unversioned".

---

## Procedure

### 1. Snapshot the old job's state

```bash
./scripts/snapshot_state.sh
./scripts/snapshot_state.sh --list
```

Confirm the newest prefix is recent. This snapshot is what seeds the new path.

### 2. Bump the tag and seed the new path

```bash
# conf/engine_config.yml
spark_version_tag: "v3.6.0"
```

```bash
./scripts/upgrade_dual_run.sh v3.6.0
```

The drill mirrors the snapshot into `checkpoints/v3.6.0/` — **not** a copy of the
old checkpoint. The distinction is the whole procedure: a snapshot carries the
durable state *and the Kafka offsets*, so the new job resumes from a known position
and replays a bounded window rather than reading a format it may not understand.

### 3. Drain the old job gracefully

**SIGTERM here** — the one place graceful shutdown is correct. Everywhere else in
this project (`kill_engine.sh`) uses SIGKILL precisely because a clean shutdown
proves nothing about recovery. Here you want the clean checkpoint.

### 4. The parity window

**Production: 24 hours. Demo: 60 seconds.**

Both paths' balance tables are compared against each other and against an
independent batch recomputation:

```bash
python scripts/parity_balance.py
python scripts/velocity_recompute.py
```

24 hours is not superstition. It covers a full daily traffic cycle, including the
overnight low where TTL eviction and idle-account behaviour differ most from the
peak the first hour showed you.

### 5. Cut over

New job primary. Old path retired to cold storage, **deleted only after the parity
window closes**. The release note from `deploy.yml` records which checkpoint path
the build wrote to — that record is what makes a later rollback a decision rather
than an excavation.

---

## Rollback

Within the parity window, the old path still exists and the old job can be
restarted against it. **After the old path is deleted there is no rollback** — only
a fresh seed from a snapshot, which is this same procedure in reverse.

That asymmetry is why the parity window is long and the deletion is last.

---

## Changes that require this procedure, not just a restart

| Change | Why |
|---|---|
| Spark version bump | RocksDB state format may differ |
| `STATE_SCHEMA` field added/removed/reordered | the state tuple is positional into RocksDB |
| State store provider swapped | switching providers on an existing checkpoint is unsupported |
| Watermark added, removed, or its column changed | checkpoint identity |
| A stream-static join added (e.g. the rejoin re-seed) | the query plan changes and operator IDs move |
| Stateful operator added or removed | same |

CI job 2 asserts `STATE_SCHEMA` and `OUTPUT_SCHEMA` are unchanged, so a field
appearing in a PR fails the build with a message pointing here. That gate exists
because the failure mode is silent: the change merges, deploys, and destroys
accumulated state on the next restart.

---

## What the drill proves, and what it does not

**Proves:** the choreography — versioned paths, snapshot-seeded new path, graceful
drain, parity window, cutover. That is version-agnostic and it is the part that
saves you.

**Does not prove:** cross-version format compatibility. Installing two Spark
versions side by side on one box costs more than it is worth, and the choreography
is designed so that compatibility is never relied upon in the first place. Say this
plainly rather than implying the drill covers more than it does.
