#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"

PASSES, FAILS, WARNS = [], [], []


def ok(label, detail=""):
    PASSES.append(label)
    print(f"  {GREEN}PASS{RESET}  {label}" + (f"  {DIM}{detail}{RESET}" if detail else ""))


def fail(label, detail=""):
    FAILS.append(f"{label}: {detail}")
    print(f"  {RED}FAIL{RESET}  {label}" + (f"  {detail}" if detail else ""))


def warn(label, detail=""):
    WARNS.append(f"{label}: {detail}")
    print(f"  {YELLOW}WARN{RESET}  {label}" + (f"  {detail}" if detail else ""))


def section(title):
    print(f"\n{title}\n" + "-" * len(title))


def check_contracts():
    section("1. Contracts")
    try:
        from conf.config import CFG, checkpoint_path, duration_ms
    except Exception as exc:
        fail("conf.config imports", repr(exc))
        return None

    cp = checkpoint_path(CFG)
    if cp == f"{CFG['checkpoint_root']}/{CFG['spark_version_tag']}/balance_engine":
        ok("checkpoint path is versioned and built in one place", cp)
    else:
        fail("checkpoint path", cp)

    if duration_ms(CFG["watermark_delay"]) == 30_000:
        ok("duration parser", f"watermark_delay -> {duration_ms(CFG['watermark_delay'])} ms")
    else:
        fail("duration parser", str(duration_ms(CFG["watermark_delay"])))

    required = ["watermark_delay", "trigger_interval", "max_buffer_size", "state_ttl",
                "gap_policy", "checkpoint_root", "spark_version_tag", "kafka_bootstrap",
                "vel_window", "vel_slide", "vel_limit"]
    missing = [k for k in required if k not in CFG]
    if missing:
        fail("config keys", f"missing {missing}")
    else:
        ok("all contract config keys present", f"{len(required)} keys")

    for t in ("events", "signals", "dlq", "integrity"):
        if t not in CFG["topics"]:
            fail("topics", f"missing topics.{t}")
            break
    else:
        ok("four topics configured", ", ".join(CFG["topics"].values()))

    if CFG["spark"]["shuffle_partitions"] <= 8:
        ok("shuffle.partitions is laptop-sized", str(CFG["spark"]["shuffle_partitions"]))
    else:
        fail("shuffle.partitions", f"{CFG['spark']['shuffle_partitions']} — 200 RocksDB "
                                   f"instances per operator will hang a 16 GB laptop")

    avsc = json.load(open(REPO_ROOT / "schemas" / "account_event_v1.avsc"))
    fields = {f["name"] for f in avsc["fields"]}
    if fields == {"account_id", "seq_no", "amount_minor", "event_type", "event_ts"}:
        ok("Avro contract fields", ", ".join(sorted(fields)))
    else:
        fail("Avro contract fields", str(sorted(fields)))
    ts = [f for f in avsc["fields"] if f["name"] == "event_ts"][0]["type"]
    if ts.get("logicalType") == "timestamp-millis" and ts.get("type") == "long":
        ok("event_ts is long/timestamp-millis")
    else:
        fail("event_ts logical type", str(ts))
    return CFG


def check_state_layout():
    section("2. State layout (checkpoint identity)")
    try:
        from spark.engine.statecore import (empty_state, pack_buffer, state_to_tuple,
                                            tuple_to_parts, unpack_buffer)
    except Exception as exc:
        fail("statecore imports", repr(exc))
        return

    st = empty_state(now_ms=7)
    if len(st) == 5 and st[0] == 0 and st[1] == 0 and st[3] == 0 and st[4] == 7:
        ok("state tuple arity and field order", "5 fields, last_seen_ms present from day one")
    else:
        fail("state tuple", str(st))

    st = state_to_tuple(4, -900, {6: (10, 1), 7: (20, 2)}, 555)
    last, bal, buf, seen = tuple_to_parts(st)
    if (last, bal, seen) == (4, -900, 555) and buf == {6: (10, 1), 7: (20, 2)} and st[3] == 2:
        ok("state round-trips through the pickle path", "buffer_size denormalised correctly")
    else:
        fail("state round-trip", str((last, bal, buf, seen, st[3])))

    class Fake(int):
        pass
    buf = unpack_buffer(pack_buffer({Fake(9): (Fake(1), Fake(2))}))
    if all(type(x) is int for x in [list(buf)[0], *buf[9]]) and (8 + 1) in buf:
        ok("buffer keys coerced to native int", "the `last + 1 in buf` lookup will hit")
    else:
        fail("buffer key coercion", str({k: type(k) for k in buf}))

    try:
        from spark.engine import state as _  # noqa: F401
        ok("state.py imports PySpark schemas", "(pyspark available)")
    except ImportError:
        warn("state.py not importable", "PySpark not installed — hermetic tier unaffected")


def check_sequencer_core():
    section("3. Sequencer pure core")
    try:
        from spark.engine.sequencer import step
        from spark.engine.statecore import empty_state, tuple_to_parts
    except Exception as exc:
        fail("sequencer imports", repr(exc))
        return

    if "pyspark" not in sys.modules and "pandas" not in sys.modules:
        ok("step() imported without pulling in PySpark or pandas",
           "CI job 1 can run on requirements-core.txt")
    else:
        warn("step() import pulled in heavy deps",
             "hermetic CI will be slower than it needs to be")

    def ev(s, a=100):
        return {"seq_no": s, "amount_minor": a, "event_ts_ms": 1000 + s}

    st, out = step(empty_state(), [ev(1), ev(2), ev(3)], {"max_buffer_size": 1000})
    last, bal, buf, _ = tuple_to_parts(st)
    if (last, bal, buf) == (3, 300, {}) and out[-1][2]["deferred"] == 0:
        ok("branch 1 applies in order, defers nothing", "last=3 balance=300")
    else:
        fail("branch 1", str((last, bal, buf, out)))

    a, _ = step(empty_state(), [ev(1), ev(2), ev(3)], {"max_buffer_size": 1000})
    b, _ = step(empty_state(), [ev(3), ev(1), ev(2)], {"max_buffer_size": 1000})
    if tuple_to_parts(a)[:2] == tuple_to_parts(b)[:2]:
        ok("within-batch disorder absorbed by the sort", "an optimisation, not the mechanism")
    else:
        fail("within-batch sort", str((tuple_to_parts(a)[:2], tuple_to_parts(b)[:2])))

    st, out = step(empty_state(), [ev(3)], {"max_buffer_size": 1000})
    if tuple_to_parts(st)[0] == 0 and out[-1][2]["deferred"] == 1:
        ok("cross-batch disorder is DEFERRED today", "Day 2's buffer is exactly this gap")
    else:
        fail("deferred accounting", str(out))

    st, out = step(empty_state(), [], {"max_buffer_size": 1000},
                   timed_out=True, watermark_ms=1)
    if [k for k, _, _ in out] == ["BALANCE"]:
        ok("timeout invocation with zero rows survives", "works from state alone")
    else:
        fail("timeout path", str(out))


def check_generator_determinism():
    section("4. Generator determinism and event-time semantics")
    tmp = Path(tempfile.mkdtemp(prefix="p3verify-"))
    try:
        env = dict(os.environ, P3_QUIET_CONFIG="1")
        base = [sys.executable, str(REPO_ROOT / "scripts" / "event_generator.py"),
                "--offline", "--accounts", "3", "--ordered", "--rate", "20",
                "--duration", "60", "--seed", "7", "--out-dir", str(tmp)]
        digests = []
        for i in range(2):
            r = subprocess.run(base, capture_output=True, text=True, env=env)
            if r.returncode != 0:
                fail("generator offline run", r.stderr.strip()[:300])
                return
            log = tmp / "delivery_log_offline0007.jsonl"
            digests.append(hashlib.sha256(log.read_bytes()).hexdigest())
            if i == 0:
                shutil.copy(log, tmp / "keep.jsonl")

        if digests[0] == digests[1]:
            ok("two runs at --seed 7 are byte-identical", f"sha256 {digests[0][:16]}…")
        else:
            fail("determinism", f"{digests[0][:16]} != {digests[1][:16]}")

        rows = [json.loads(l) for l in (tmp / "keep.jsonl").read_text().splitlines() if l]
        if len(rows) == 1200:
            ok("delivery log line count", "1200 = rate 20 x duration 60")
        else:
            fail("delivery log line count", str(len(rows)))

        keys = set(rows[0])
        expect = {"run_id", "publish_order", "account_id", "seq_no",
                  "amount_minor", "event_type", "event_ts_ms"}
        if keys == expect:
            ok("delivery log schema matches contract (7)", ", ".join(sorted(keys)))
        else:
            fail("delivery log schema", str(sorted(keys)))

        # Event time must be LOGICAL, not publish order. Note --ordered is dropped
        # here: it disables shuffling by design, and leaving it in would make this
        # check silently vacuous.
        shuffle_cmd = [a for a in base if a != "--ordered"] + ["--shuffle-window", "20"]
        r2 = subprocess.run(shuffle_cmd, capture_output=True, text=True, env=env)
        if r2.returncode != 0:
            fail("generator shuffle run", r2.stderr.strip()[:300])
            return
        srows = [json.loads(l) for l in
                 (tmp / "delivery_log_offline0007.jsonl").read_text().splitlines() if l]
        srows = [r for r in srows if r["account_id"] == "ACCT-0001"]
        by_seq = sorted(srows, key=lambda r: r["seq_no"])
        monotonic = all(by_seq[i]["event_ts_ms"] < by_seq[i + 1]["event_ts_ms"]
                        for i in range(len(by_seq) - 1))
        frontier, late = 0, 0
        for r in srows:
            if r["event_ts_ms"] < frontier:
                late += 1
            frontier = max(frontier, r["event_ts_ms"])
        if monotonic and late > 0:
            ok("shuffled stream is genuinely late in EVENT time",
               f"{late}/{len(srows)} arrivals carry an older event_ts than the frontier")
        else:
            fail("event-time semantics",
                 f"monotonic_in_seq={monotonic} late_arrivals={late} — if late==0, the "
                 f"watermark has nothing to do and Day 3's negative control is vacuous")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def check_oracle():
    section("5. Oracle")
    try:
        from scripts.oracle import coalesce_ranges, compute_oracle
    except Exception as exc:
        fail("oracle imports", repr(exc))
        return

    def log(pairs):
        return [{"publish_order": i, "account_id": "A1", "seq_no": s, "amount_minor": a,
                 "event_type": "CREDIT", "event_ts_ms": 1000 + s}
                for i, (s, a) in enumerate(pairs)]

    o = compute_oracle(log([(1, 10), (2, 20), (2, 20), (4, 40)]), "FLAG_AND_CONTINUE")["A1"]
    checks = [
        (o["expected_balance_minor"] == 70, "balance excludes the duplicate and the gap"),
        (o["expected_dup_dropped"] == [2], "duplicate detected exactly once"),
        (o["expected_gap_ranges"] == [[3, 3, 1]], "gap coalesced to a range with a count"),
        (o["applied_count"] == 3, "applied-set has three members"),
    ]
    for good, label in checks:
        ok(label) if good else fail(label, json.dumps(o, default=str)[:200])

    h = compute_oracle(log([(1, 10), (2, 20), (4, 40)]), "HOLD")["A1"]
    if h["expected_last_applied_seq"] == 2 and h["expected_balance_minor"] == 30:
        ok("HOLD stops at the hole", "nothing after the gap applies")
    else:
        fail("HOLD policy", str(h))

    if coalesce_ranges([7, 8, 9, 15]) == [(7, 9, 3), (15, 15, 1)]:
        ok("range coalescing", "a 50-event outage reads as one alert, not fifty")
    else:
        fail("range coalescing", str(coalesce_ranges([7, 8, 9, 15])))


def check_overflow_invariant():
    section("6. Overflow invariant (the claim Stage 5 rests on)")
    from scripts.oracle import compute_oracle

    def simulate(arrivals, cap):
        """min(buf ∪ {s}) eviction with the head withheld: nothing can apply or drain."""
        buf, evicted = set(), []
        for s in arrivals:
            if s in buf:
                continue
            if len(buf) >= cap:
                victim = min(min(buf), s)
                if victim == s:
                    evicted.append(s)
                    continue
                buf.discard(victim)
                evicted.append(victim)
            buf.add(s)
        return buf, sorted(evicted)

    rng = random.Random(0)
    cap, n, trials = 7, 40, 300
    delivered = list(range(2, n + 2))                 # seq 1 withheld
    predicted_buf = set(sorted(delivered)[-cap:])
    predicted_ev = sorted(delivered)[:-cap]

    bad = 0
    for _ in range(trials):
        order = delivered[:]
        rng.shuffle(order)
        buf, ev = simulate(order, cap)
        if buf != predicted_buf or ev != predicted_ev:
            bad += 1
    if bad == 0:
        ok(f"min-first eviction retains the k largest across {trials} random arrival orders",
           "so the DLQ set is order-independent")
    else:
        fail("overflow invariant", f"{bad}/{trials} arrival orders disagreed")

    rows = [{"publish_order": i, "account_id": "X", "seq_no": s, "amount_minor": 10,
             "event_type": "CREDIT", "event_ts_ms": 1000 + s}
            for i, s in enumerate(delivered)]
    o = compute_oracle(rows, "FLAG_AND_CONTINUE", cap)["X"]
    if o["expected_overflow_evicted"] == predicted_ev and o["overflow_determinate"]:
        ok("oracle predicts the DLQ set from the delivery log alone",
           "Stage 5's proof stays independent of the engine's own output")
    else:
        fail("oracle overflow prediction", str(o["expected_overflow_evicted"])[:120])


def check_environment():
    section("7. Environment (warnings only — the hermetic tier above is the gate)")
    major, minor = sys.version_info[:2]
    if (major, minor) == (3, 11):
        ok("python 3.11", sys.version.split()[0])
    else:
        warn(f"python {major}.{minor}",
             "3.11 recommended; PySpark 3.5.1 on 3.12 has known distutils/typing breakage")

    try:
        import numpy
        if numpy.__version__.startswith("1."):
            ok("numpy pinned below 2.0", numpy.__version__)
        else:
            fail("numpy 2.x installed",
                 f"{numpy.__version__} breaks PySpark 3.5 pandas UDFs — pip install numpy==1.26.4")
    except ImportError:
        warn("numpy not installed", "expected before `pip install -r requirements.txt`")

    for mod in ("pyspark", "delta", "confluent_kafka", "fastavro", "pandas", "pyarrow"):
        try:
            __import__(mod)
            ok(f"import {mod}")
        except ImportError:
            warn(f"import {mod}", "not installed yet")

    java = shutil.which("java")
    if java:
        try:
            out = subprocess.run(["java", "-version"], capture_output=True, text=True)
            ver = (out.stderr or out.stdout).splitlines()[0]
            ok("java present", ver.strip())
        except Exception:
            warn("java present but unreadable version")
    else:
        warn("java not found", "Spark 3.5.1 needs Java 8, 11 or 17 — install openjdk-17")
    if not os.environ.get("JAVA_HOME"):
        warn("JAVA_HOME unset", "PySpark will usually still find java, but set it to be safe")

    if shutil.which("docker"):
        ok("docker present")
    else:
        warn("docker not found", "the stack cannot start without it")

    for port, what in ((29092, "kafka host listener"), (8081, "schema registry"),
                       (9000, "minio s3"), (9001, "minio console")):
        s = socket.socket()
        s.settimeout(0.4)
        listening = s.connect_ex(("127.0.0.1", port)) == 0
        s.close()
        print(f"  {DIM}INFO{RESET}  port {port} ({what}): "
              f"{'in use / stack up' if listening else 'free'}")


def main() -> None:
    p = argparse.ArgumentParser(description="Day 1 verification")
    p.add_argument("--hermetic", action="store_true", help="skip the environment tier")
    args = p.parse_args()

    print("Project 3 — Day 1 verification")
    print("=" * 70)
    check_contracts()
    check_state_layout()
    check_sequencer_core()
    check_generator_determinism()
    check_oracle()
    check_overflow_invariant()
    if not args.hermetic:
        check_environment()

    print("\n" + "=" * 70)
    print(f"{GREEN}{len(PASSES)} passed{RESET}   "
          f"{RED}{len(FAILS)} failed{RESET}   "
          f"{YELLOW}{len(WARNS)} warnings{RESET}")
    if FAILS:
        print("\nfailures:")
        for f in FAILS:
            print(f"  - {f}")
        sys.exit(1)
    print("\nHermetic tier green. Bring the stack up and run Stage 0.")
    sys.exit(0)


if __name__ == "__main__":
    main()
