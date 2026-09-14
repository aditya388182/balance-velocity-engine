#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from spark.engine.sequencer import compute_alarm, step  # noqa: E402
from spark.engine.statecore import empty_state, tuple_to_parts  # noqa: E402
from spark.tests.harness import run_stream  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"
PASSES, FAILS = [], []

BASE, STEP, TTL = 1_000_000_000_000, 50, 120_000
CFG = {"max_buffer_size": 1000, "gap_policy": "FLAG_AND_CONTINUE",
       "watermark_delay_ms": 30_000, "gap_realert_ms": 60_000, "state_ttl_ms": TTL}


def ok(label, detail=""):
    PASSES.append(label)
    print(f"  {GREEN}PASS{RESET}  {label}" + (f"  {DIM}{detail}{RESET}" if detail else ""))


def fail(label, detail=""):
    FAILS.append(f"{label}: {detail}")
    print(f"  {RED}FAIL{RESET}  {label}" + (f"  {detail}" if detail else ""))


def section(t):
    print(f"\n{t}\n" + "-" * len(t))


def ev(s, a=100):
    return {"seq_no": s, "amount_minor": a, "event_ts_ms": BASE + s * STEP}


def kinds(out):
    return [k for k, _s, _d in out]


def check_ttl_alarm():
    section("1. The TTL alarm — the branch that was None for two days")

    if compute_alarm({}, watermark_ms=BASE, last_seen_ms=BASE, state_ttl_ms=TTL) == BASE + TTL:
        ok("an empty buffer now arms a TTL alarm", "Days 3-4 returned None, so nothing evicted")
    else:
        fail("ttl alarm", str(compute_alarm({}, BASE, last_seen_ms=BASE, state_ttl_ms=TTL)))

    if compute_alarm({}, watermark_ms=BASE + 999_999,
                     last_seen_ms=BASE, state_ttl_ms=TTL) == BASE + 1_000_000:
        ok("clamped above the watermark", "already-stale keys fire next batch, not never")
    else:
        fail("ttl clamp", "")

    buf = {7: (100, BASE + 7_000)}
    if compute_alarm(buf, BASE, last_seen_ms=BASE, state_ttl_ms=TTL) == BASE + 7_000:
        ok("a pending buffer still wins the alarm slot",
           "an account with an open hole is not idle")
    else:
        fail("alarm precedence", "")

    if compute_alarm({}, BASE, last_seen_ms=BASE, state_ttl_ms=0) is None:
        ok("no TTL configured means no alarm", "the Day 3-4 behaviour is still reachable")
    else:
        fail("ttl disabled", "")


def check_timeout_double_duty():
    section("2. One timeout slot, two alarms — disambiguated by state, never guessed")

    st, _ = step(empty_state(), [ev(1), ev(5)], CFG, watermark_ms=BASE)
    _s, out = step(st, [], CFG, timed_out=True, watermark_ms=BASE + 10_000_000)
    if "SEQUENCE_GAP" in kinds(out) and "TTL_FLUSH" not in kinds(out):
        ok("open buffer -> GAP", "gap detection outranks TTL")
    else:
        fail("gap branch", str(kinds(out)))

    st, _ = step(empty_state(), [ev(1), ev(2)], CFG, watermark_ms=BASE)
    _s, out = step(st, [], CFG, timed_out=True, watermark_ms=BASE + 2 * STEP + TTL)
    if "TTL_FLUSH" in kinds(out) and "SEQUENCE_GAP" not in kinds(out):
        ok("empty + stale -> TTL flush", f"idle_ms={out[-1][2].get('idle_ms')}")
    else:
        fail("ttl branch", str(kinds(out)))

    st2, out = step(st, [], CFG, timed_out=True, watermark_ms=BASE + 2 * STEP + 10)
    if "TTL_FLUSH" not in kinds(out) and out[-1][2]["alarm_ts"] is not None:
        ok("empty + fresh -> just re-arm", "it must not go silent")
    else:
        fail("re-arm branch", str(out[-1][2]))

    st3, out = step(st, [ev(3)], CFG, timed_out=True, watermark_ms=BASE + 2 * STEP + TTL)
    if "TTL_FLUSH" not in kinds(out) and tuple_to_parts(st3)[0] == 3:
        ok("a flush that also received rows is not idle after all",
           "the rows ARE the account speaking again")
    else:
        fail("flush-with-rows", str(kinds(out)))

    _s, out = step(st, [], CFG, timed_out=True, watermark_ms=BASE + 2 * STEP + TTL)
    if out[-1][0] == "TTL_FLUSH" and out[-1][2]["alarm_ts"] is None:
        ok("a released key is not left armed", "no alarm survives the removal")
    else:
        fail("flush alarm", str(out[-1][2].get("alarm_ts")))


def check_rejoin():
    section("3. The rejoin hole — why TTL is unsafe without a re-seed")

    st, _ = step(empty_state(), [ev(21)], CFG, state_exists=False)
    last, bal, buf, _ = tuple_to_parts(st)
    if (last, bal, sorted(buf)) == (0, 0, [21]):
        ok("WITHOUT the re-seed: seq 21 buffers behind a phantom gap 1..20",
           "and the balance would restart from zero")
    else:
        fail("rejoin bug reproduction", str((last, bal, sorted(buf))))

    st, out = step(empty_state(), [ev(21)], CFG, opening=(20, 2000), state_exists=False)
    last, bal, buf, _ = tuple_to_parts(st)
    if (last, bal, buf) == (21, 2100, {}) and "SEQUENCE_GAP" not in kinds(out):
        ok("WITH the re-seed: applies directly, no gap, balance continues",
           "2000 -> 2100, and last 21 > stored 20 so the MERGE guard accepts it")
    else:
        fail("rejoin fix", str((last, bal, sorted(buf), kinds(out))))

    st, _ = step(empty_state(), [ev(1), ev(2)], CFG)
    st2, out = step(st, [ev(3)], CFG, opening=(99, 999_999), state_exists=True)
    if tuple_to_parts(st2)[:2] == (3, 300) and "reseeded_from" not in out[-1][2]:
        ok("a LIVE account ignores the opening",
           "a stale Delta read must never replace live state")
    else:
        fail("cold-only guard", str(tuple_to_parts(st2)[:2]))

    st, out = step(empty_state(), [ev(1)], CFG, opening=(0, 0), state_exists=False)
    if tuple_to_parts(st)[:2] == (1, 100) and "reseeded_from" not in out[-1][2]:
        ok("an opening of (0, 0) is not a re-seed", "a row that never applied seeds nothing")
    else:
        fail("zero opening", str(tuple_to_parts(st)[:2]))


def check_rejoin_property(trials):
    section("4. Flush then rejoin, randomised")

    bad = []
    for t in range(trials):
        rng = random.Random(700_000 + t)
        n = rng.randint(5, 60)
        amounts = {s: rng.randint(-5000, 5000) for s in range(1, n + 1)}
        st = empty_state()
        for s in range(1, n + 1):
            st, _ = step(st, [ev(s, amounts[s])], CFG)
        last_before, bal_before, _b, _ls = tuple_to_parts(st)

        # flush, release, and come back
        st2, out = step(empty_state(), [ev(n + 1, amounts.get(n + 1, 77))], CFG,
                        opening=(last_before, bal_before), state_exists=False)
        last, bal, buf, _ = tuple_to_parts(st2)
        expect = bal_before + amounts.get(n + 1, 77)
        if not (last == n + 1 and bal == expect and buf == {}
                and "SEQUENCE_GAP" not in kinds(out) and last > last_before):
            bad.append(t)

    if not bad:
        ok(f"{trials} flush/rejoin round trips preserve the balance exactly",
           "no phantom gap, and always a higher seq than the stored row")
    else:
        fail("rejoin property", f"{len(bad)}/{trials} failed, first={bad[0]}")


def check_eviction_curve():
    section("5. The eviction curve, end to end")

    events = [ev(s) for s in range(1, 21)]
    st, rec = run_stream(events, CFG, batch_size=5,
                         trailing_idle_batches=60, idle_event_time_step_ms=5000)
    flushes = [(s, d) for _b, k, s, d in rec.outputs if k == "TTL_FLUSH"]
    if len(flushes) == 1 and tuple_to_parts(st)[:2] == (0, 0):
        ok("an idle account is flushed EXACTLY ONCE and released",
           f"final seq {flushes[0][0]}, idle {flushes[0][1]['idle_ms']} ms")
    else:
        fail("eviction", f"{len(flushes)} flush(es), state {tuple_to_parts(st)[:2]}")

    busy = [ev(s) for s in range(1, 61)]
    st, rec = run_stream(busy, CFG, batch_size=5)
    if not [1 for _b, k, _s, _d in rec.outputs if k == "TTL_FLUSH"]:
        ok("a busy account is never flushed", "TTL is idleness, not age")
    else:
        fail("busy flushed", "")


def check_native_custom_split():
    section("6. The native/custom split, as a property of the code")

    # Read as TEXT, not by importing: velocity.py imports pyspark at module level
    # (it is nothing but Spark, which is the point), and this check must stay
    # hermetic so CI job 1 can run it on a bare runner.
    src_vel = (REPO_ROOT / "spark" / "engine" / "velocity.py").read_text()
    src_seq = (REPO_ROOT / "spark" / "engine" / "sequencer.py").read_text()

    custom_markers = ["pending_buffer", "state.update", "setTimeoutTimestamp",
                      "max_buffer_size"]
    leaked = [m for m in custom_markers if m in src_vel]
    if not leaked:
        ok("velocity.py holds ZERO custom state",
           "no buffer, no dedup, no alarm — window state is Spark's problem there")
    else:
        fail("velocity leaked custom state", str(leaked))

    if "window(" in src_vel and "groupBy" in src_vel:
        ok("velocity is a native time-windowed aggregation", "groupBy + window, nothing else")
    else:
        fail("velocity shape", "no native window found")

    for marker in ("setTimeoutTimestamp", "max_buffer_size", "pending"):
        if marker not in src_seq:
            fail("sequencer lost custom machinery", marker)
            break
    else:
        ok("the sequencer keeps the machinery only IT needs",
           "nothing native expresses 'apply in seq order and alarm on holes'")

    vel_lines = len([l for l in src_vel.splitlines()
                     if l.strip() and not l.strip().startswith("#")])
    seq_lines = len([l for l in src_seq.splitlines()
                     if l.strip() and not l.strip().startswith("#")])
    ok("the size difference is the argument",
       f"velocity {vel_lines} lines vs sequencer {seq_lines} — every line of custom "
       f"state you did not write cannot double-apply money")


def check_metric_names():
    section("7. Metric names are fixed before the dashboards reference them")

    import importlib
    metrics = importlib.import_module("spark.engine.metrics")
    required = ["p3_state_rows", "p3_buffer_p99", "p3_gap_rate", "p3_dup_dropped",
                "p3_overflow_rate", "p3_ttl_flush", "p3_snapshot_lag_seconds"]
    missing = [m for m in required if m not in metrics.ALL_METRICS]
    if not missing:
        ok(f"all {len(required)} runbook metric names defined", ", ".join(required[:4]) + " ...")
    else:
        fail("metric names", str(missing))

    try:
        metrics.emit({"p3_state_rows": 1}, job="verify")
        ok("emit() survives with no pushgateway running",
           "a broken side-channel must never fail a batch")
    except Exception as exc:
        fail("emit raised", repr(exc))


def main() -> None:
    p = argparse.ArgumentParser(description="Day 5 verification")
    p.add_argument("--trials", type=int, default=400)
    args = p.parse_args()

    print("Project 3 — Day 5 verification (hermetic: no Docker, no Kafka, no JVM)")
    print("=" * 74)
    print(f"{DIM}Velocity itself cannot be unit-tested here — the whole claim is that")
    print(f"Spark's native windows do the job, so there is nothing of ours to test.")
    print(f"scripts/velocity_recompute.py is that check, and it needs the stack.{RESET}")

    check_ttl_alarm()
    check_timeout_double_duty()
    check_rejoin()
    check_rejoin_property(args.trials)
    check_eviction_curve()
    check_native_custom_split()
    check_metric_names()

    print("\n" + "=" * 74)
    print(f"{GREEN}{len(PASSES)} passed{RESET}   {RED}{len(FAILS)} failed{RESET}")
    if FAILS:
        print("\nfailures:")
        for f in FAILS:
            print(f"  - {f}")
        sys.exit(1)
    print("\nTTL and rejoin green. Bring the stack up for velocity parity and the curve.")
    sys.exit(0)


if __name__ == "__main__":
    main()
