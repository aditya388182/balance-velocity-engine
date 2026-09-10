#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import random
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.oracle import compute_oracle  # noqa: E402
from spark.engine.sequencer import step  # noqa: E402
from spark.engine.statecore import empty_state, tuple_to_parts  # noqa: E402
from spark.tests.harness import run_stream, run_stream_with_crash  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"
PASSES, FAILS = [], []

BASE, STEP = 1_000_000_000_000, 50
FLAG = {"max_buffer_size": 1000, "gap_policy": "FLAG_AND_CONTINUE",
        "watermark_delay_ms": 30_000, "gap_realert_ms": 60_000}


def ok(label, detail=""):
    PASSES.append(label)
    print(f"  {GREEN}PASS{RESET}  {label}" + (f"  {DIM}{detail}{RESET}" if detail else ""))


def fail(label, detail=""):
    FAILS.append(f"{label}: {detail}")
    print(f"  {RED}FAIL{RESET}  {label}" + (f"  {detail}" if detail else ""))


def section(title):
    print(f"\n{title}\n" + "-" * len(title))


def ev(s, a=100):
    return {"seq_no": s, "amount_minor": a, "event_ts_ms": BASE + s * STEP}


def check_recovery_identity(trials):
    section("1. A crash changes nothing — the whole point of Stage 3")

    evs = [ev(s) for s in range(1, 201)]
    clean, _ = run_stream(evs, FLAG, batch_size=25)
    crashed, _rec, _lost = run_stream_with_crash(evs, FLAG, crash_before_batch=3,
                                                 batch_size=25)
    if tuple_to_parts(clean)[:3] == tuple_to_parts(crashed)[:3]:
        ok("SIGKILL mid-batch then restart == never crashed at all",
           f"last={tuple_to_parts(clean)[0]} balance={tuple_to_parts(clean)[1]}")
    else:
        fail("crash identity", f"{tuple_to_parts(clean)[:2]} vs {tuple_to_parts(crashed)[:2]}")

    bad = []
    for t in range(trials):
        rng = random.Random(400_000 + t)
        n = rng.randint(20, 120)
        amounts = {s: rng.randint(-5000, 5000) for s in range(1, n + 1)}
        delivery = list(range(1, n + 1))
        for _ in range(rng.randint(0, 4)):
            delivery.append(rng.randint(1, n))
        rng.shuffle(delivery)
        evs = [ev(s, amounts[s]) for s in delivery]
        bs = rng.randint(3, 20)
        nbatches = max(1, (len(evs) + bs - 1) // bs)
        crash_at = rng.randint(0, nbatches - 1)

        clean, _ = run_stream(evs, FLAG, batch_size=bs, trailing_idle_batches=20,
                              idle_event_time_step_ms=5000)
        crashed, _r, _l = run_stream_with_crash(evs, FLAG, crash_before_batch=crash_at,
                                                batch_size=bs, trailing_idle_batches=20,
                                                idle_event_time_step_ms=5000)
        if tuple_to_parts(clean)[:3] != tuple_to_parts(crashed)[:3]:
            bad.append(t)

    if not bad:
        ok(f"{trials} random streams, random batch sizes, random crash points",
           "final state identical to the uncrashed run every time")
    else:
        fail("randomised recovery", f"{len(bad)}/{trials} diverged, first={bad[0]}")


def check_recovery_vs_oracle(trials):
    section("2. And the recovered state still equals the ORACLE, not just itself")

    bad = []
    for t in range(trials):
        rng = random.Random(500_000 + t)
        n = rng.randint(20, 100)
        amounts = {s: rng.randint(-5000, 5000) for s in range(1, n + 1)}
        delivery = list(range(1, n + 1))
        for _ in range(rng.randint(0, 4)):
            delivery.append(rng.randint(1, n))
        rng.shuffle(delivery)
        evs = [ev(s, amounts[s]) for s in delivery]
        bs = rng.randint(3, 15)
        nbatches = max(1, (len(evs) + bs - 1) // bs)

        crashed, _r, _l = run_stream_with_crash(
            evs, FLAG, crash_before_batch=rng.randint(0, nbatches - 1),
            batch_size=bs, trailing_idle_batches=20, idle_event_time_step_ms=5000)
        last, bal, buf, _ = tuple_to_parts(crashed)

        rows = [{"publish_order": j, "account_id": "A1", "seq_no": s,
                 "amount_minor": amounts[s], "event_type": "CREDIT",
                 "event_ts_ms": BASE + s * STEP} for j, s in enumerate(delivery)]
        o = compute_oracle(rows, "FLAG_AND_CONTINUE")["A1"]
        if not (buf == {} and bal == o["expected_balance_minor"]
                and last == o["expected_last_applied_seq"]):
            bad.append(t)

    if not bad:
        ok(f"{trials} crashed runs match the independent oracle exactly",
           "a double-apply that nets zero against a skip would pass a sum, never a set")
    else:
        fail("recovery vs oracle", f"{len(bad)}/{trials} diverged, first={bad[0]}")


def check_what_recovery_evidence_actually_is():
    section("3. What the replay ACTUALLY leaves behind (the plan gets this wrong)")

    evs = [ev(s) for s in range(1, 201)]
    evs.insert(80, ev(60))
    evs.insert(85, ev(61))
    _st, rec, lost = run_stream_with_crash(evs, FLAG, crash_before_batch=3, batch_size=25)

    recovery_dups = [(b, s) for b, s, _d in rec.dups() if b == 3]
    pre = {(b, k, s) for b, k, s, _d in lost.outputs}
    post = {(b, k, s) for b, k, s, _d in rec.outputs}
    written_twice = pre & post

    if written_twice:
        ok("the observable trace is DUPLICATE ROWS WITH THE SAME batch_id",
           f"{sorted(written_twice)} — the foreachBatch body re-ran")
    else:
        fail("sink replay trace", "the doomed attempt wrote nothing to compare")

    pre_batch = [b for b in lost.batches if b["batch_id"] == 3][0]
    post_batch = [b for b in rec.batches if b["batch_id"] == 3][0]
    if post_batch["last_applied_seq"] == pre_batch["last_applied_seq"]:
        ok("the replayed MERGE is a NO-OP under the strict > guard",
           f"s.last_applied_seq {post_batch['last_applied_seq']} is not > "
           f"t.last_applied_seq {pre_batch['last_applied_seq']}")
    else:
        fail("merge guard", f"{pre_batch['last_applied_seq']} vs {post_batch['last_applied_seq']}")

    # The stream above contains GENUINE duplicates in batch 3, so DUP records there
    # are expected. The claim is that the replay re-emits the SAME ones rather than
    # adding new ones: a faithful re-execution, not an extra delivery.
    lost_dups = sorted((b, s) for b, s, _d in lost.dups() if b == 3)
    if recovery_dups and sorted(recovery_dups) == lost_dups:
        ok("the replay re-emits the SAME dup records, never extra ones",
           f"{lost_dups} both times — a faithful re-execution of batch 3")
    else:
        fail("replay fidelity", f"pre-crash {lost_dups} vs replay {sorted(recovery_dups)}")

    # And on a stream with no duplicates in the data, recovery causes none at all.
    plain = [ev(s) for s in range(1, 201)]
    _st2, rec2, _l2 = run_stream_with_crash(plain, FLAG, crash_before_batch=3, batch_size=25)
    if not rec2.dups():
        ok("recovery ITSELF causes ZERO DUP_DROPPED records — and that is CORRECT",
           "state rolled back with the offsets, so the state machine was never shown "
           "anything twice; expecting recovery-caused dups misreads how Spark checkpoints")
    else:
        fail("recovery dups", f"{rec2.dups()[:5]} on a duplicate-free stream")


def check_mid_gap_kill():
    section("4. The compound case — a crash INSIDE an open gap window")

    evs = [ev(s) for s in range(1, 61) if s != 25]
    clean, rec_clean = run_stream(evs, FLAG, batch_size=10, trailing_idle_batches=40,
                                  idle_event_time_step_ms=5000)
    crashed, rec_crash, _l = run_stream_with_crash(
        evs, FLAG, crash_before_batch=4, batch_size=10,
        trailing_idle_batches=40, idle_event_time_step_ms=5000)

    if tuple_to_parts(clean)[:3] == tuple_to_parts(crashed)[:3]:
        ok("killed mid-gap-window, final state identical to the uncrashed run",
           f"last={tuple_to_parts(crashed)[0]} balance={tuple_to_parts(crashed)[1]}")
    else:
        fail("mid-gap crash", f"{tuple_to_parts(clean)[:2]} vs {tuple_to_parts(crashed)[:2]}")

    ranges_clean = sorted((d["lo"], d["hi"]) for _b, _s, d in rec_clean.gaps())
    ranges_crash = sorted((d["lo"], d["hi"]) for _b, _s, d in rec_crash.gaps())
    if len(ranges_crash) == 1 and ranges_crash == ranges_clean:
        ok("EXACTLY ONE gap range survives the crash",
           f"{ranges_crash} — not zero (a lost alarm), not two (a double fire)")
    else:
        fail("gap after crash", f"clean={ranges_clean} crashed={ranges_crash}")

    st, _ = step(empty_state(), [ev(1), ev(5), ev(6)], FLAG, watermark_ms=BASE)
    import pickle
    restored = pickle.loads(pickle.dumps(st))
    _, _, buf, _ = tuple_to_parts(restored)
    if sorted(buf) == [5, 6]:
        ok("the pending buffer survives the checkpoint round-trip", "RocksDB restored it")
    else:
        fail("buffer survival", str(sorted(buf)))

    jumped = BASE + 10_000_000
    _st2, out = step(restored, [], FLAG, timed_out=False, watermark_ms=jumped)
    alarm = out[-1][2]["alarm_ts"]
    if alarm == jumped + 1:
        ok("the alarm re-arms correctly when the watermark JUMPED during downtime",
           f"clamped to watermark+1 = {alarm}, so it fires on the very next batch")
    else:
        fail("alarm re-arm after jump", f"alarm={alarm} watermark={jumped}")


def check_burst(trials):
    section("5. Stage 5 — the burst, the cap, and the DLQ")

    cfg = dict(FLAG, max_buffer_size=1000)
    delivered = list(range(2, 10_002))            # seq 1 withheld: pure buffer pressure
    rng = random.Random(7)
    order = delivered[:]
    rng.shuffle(order)
    st = empty_state()
    evicted = []
    for i in range(0, len(order), 500):
        st, out = step(st, [ev(s, -1000) for s in order[i:i + 500]], cfg)
        evicted += [s for k, s, _d in out if k == "BUFFER_OVERFLOW"]
    _last, _bal, buf, _ = tuple_to_parts(st)

    if len(buf) == 1000 and sorted(buf) == list(range(9002, 10002)):
        ok("10,000 out-of-order events: the buffer pins at EXACTLY 1000",
           "holding 9002..10001 — the k largest, whatever order they arrived in")
    else:
        fail("burst cap", f"buffer={len(buf)} lo={min(buf)} hi={max(buf)}")

    if len(evicted) == 9000:
        ok("exactly 9,000 evictions to the DLQ", "10,000 delivered minus the 1,000 retained")
    else:
        fail("eviction count", str(len(evicted)))

    rows = [{"publish_order": i, "account_id": "HOT-1", "seq_no": s,
             "amount_minor": -1000, "event_type": "DEBIT", "event_ts_ms": BASE + s * 10}
            for i, s in enumerate(order)]
    o = compute_oracle(rows, "FLAG_AND_CONTINUE", 1000)["HOT-1"]
    if sorted(evicted) == o["expected_overflow_evicted"] and o["overflow_determinate"]:
        ok("the oracle predicted that exact DLQ set from the delivery log alone",
           "Stage 5's proof never reads the engine's own output back in")
    else:
        fail("oracle burst prediction", f"engine={len(evicted)} "
                                        f"oracle={len(o['expected_overflow_evicted'])}")

    bad = 0
    for t in range(min(trials, 200)):
        r = random.Random(600_000 + t)
        d = list(range(2, 302))
        o2 = d[:]
        r.shuffle(o2)
        s2 = empty_state()
        ev2 = []
        i = 0
        while i < len(o2):
            k = r.randint(1, 40)
            s2, out = step(s2, [ev(q, -1000) for q in o2[i:i + k]], dict(FLAG, max_buffer_size=50))
            ev2 += [q for kk, q, _d in out if kk == "BUFFER_OVERFLOW"]
            i += k
        if sorted(tuple_to_parts(s2)[2]) != d[-50:] or sorted(ev2) != d[:-50]:
            bad += 1
    if bad == 0:
        ok(f"{min(trials, 200)} random arrival orders and batch splits agree",
           "the k-largest invariant is what makes the DLQ set predictable")
    else:
        fail("k-largest under burst", f"{bad} disagreed")


def check_neighbours():
    section("6. The hostile account degrades ITSELF, not its neighbours")

    cfg = dict(FLAG, max_buffer_size=100)
    hot = empty_state()
    for i in range(2, 500):
        hot, _ = step(hot, [ev(i, -1000)], cfg)
    calm = empty_state()
    for s in range(1, 51):
        calm, _ = step(calm, [ev(s, 100)], cfg)

    hot_last = tuple_to_parts(hot)[0]
    calm_last, calm_bal, calm_buf, _ = tuple_to_parts(calm)
    if hot_last == 0 and (calm_last, calm_bal, calm_buf) == (50, 5000, {}):
        ok("the hot account is stuck at last=0 while its neighbour is fully applied",
           "per-key state means one account's pathology is bounded to that account")
    else:
        fail("neighbour isolation", f"hot={hot_last} calm={(calm_last, calm_bal)}")


def check_rss_checker():
    section("7. The RSS flatness checker itself")

    from importlib import import_module
    mod = import_module("scripts.rss_monitor")

    tmp = Path(tempfile.mkdtemp())
    flat = tmp / "flat.csv"
    climb = tmp / "climb.csv"
    for path, fn in ((flat, lambda i: 900 + (i % 5)), (climb, lambda i: 900 + i * 12)):
        with open(path, "w", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(["elapsed_s", "wall_ms", "rss_kb", "rss_mb"])
            for i in range(60):
                w.writerow([i * 5, i * 5000, int(fn(i) * 1024), fn(i)])

    s_flat = mod.summarise(str(flat))
    s_climb = mod.summarise(str(climb))
    if s_flat["growth_mb"] < 50 and s_climb["growth_mb"] > 400:
        ok("the checker separates a flat curve from a climbing one",
           f"flat grew {s_flat['growth_mb']:.0f} MB, climbing grew "
           f"{s_climb['growth_mb']:.0f} MB")
    else:
        fail("rss checker", f"flat={s_flat['growth_mb']} climb={s_climb['growth_mb']}")


def check_fixtures():
    section("8. The committed CI fixtures")

    import json
    d = REPO_ROOT / "data" / "fixtures" / "ci_sequences"
    files = sorted(d.glob("*.json"))
    expected = {"in_order", "out_of_order", "duplicate_applied", "duplicate_buffered",
                "gap", "hold_policy", "restart", "overflow"}
    names = {json.loads(p.read_text())["name"] for p in files}
    if names == expected:
        ok(f"all {len(expected)} matrix scenarios are committed as data", ", ".join(sorted(names)))
    else:
        fail("fixtures", f"missing {sorted(expected - names)} extra {sorted(names - expected)}")

    undocumented = [json.loads(p.read_text())["name"] for p in files
                    if not json.loads(p.read_text()).get("description", "").strip()]
    if not undocumented:
        ok("every fixture documents the claim it makes",
           "the fixtures are the reviewable contract, so an undocumented one is not one")
    else:
        fail("fixture docs", str(undocumented))


def main() -> None:
    p = argparse.ArgumentParser(description="Day 4 verification")
    p.add_argument("--trials", type=int, default=300)
    args = p.parse_args()

    print("Project 3 — verification 4th part (hermetic: no Docker, no Kafka, no JVM)")
    print("=" * 74)
    print(f"{DIM}Recovery is proved against a MODEL of Spark's checkpoint semantics")
    print(f"(offsets/N before the batch, commits/N after, state rolls back with it).")
    print(f"Block 4.2 measures it for real.{RESET}")

    check_recovery_identity(args.trials)
    check_recovery_vs_oracle(args.trials)
    check_what_recovery_evidence_actually_is()
    check_mid_gap_kill()
    check_burst(args.trials)
    check_neighbours()
    check_rss_checker()
    check_fixtures()

    print("\n" + "=" * 74)
    print(f"{GREEN}{len(PASSES)} passed{RESET}   {RED}{len(FAILS)} failed{RESET}")
    if FAILS:
        print("\nfailures:")
        for f in FAILS:
            print(f"  - {f}")
        sys.exit(1)
    print("\nRecovery and backpressure green. Bring the stack up and kill it for real.")
    sys.exit(0)


if __name__ == "__main__":
    main()
