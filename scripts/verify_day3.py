#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.oracle import compute_oracle  # noqa: E402
from spark.engine import gap_policy  # noqa: E402
from spark.engine.sequencer import compute_alarm  # noqa: E402
from spark.engine.statecore import tuple_to_parts  # noqa: E402
from spark.tests.harness import run_stream  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"
PASSES, FAILS = [], []

BASE = 1_000_000_000_000
STEP = 50                                   # 50 ms/event == rate 20
FLAG = {"max_buffer_size": 1000, "gap_policy": "FLAG_AND_CONTINUE",
        "watermark_delay_ms": 30_000, "gap_realert_ms": 60_000}
HOLD = dict(FLAG, gap_policy="HOLD")


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


def stream(missing=(), n=60, cfg=FLAG, batch_size=10, idle=40, idle_step=5000,
           wm=None, events=None):
    evs = events if events is not None else [ev(s) for s in range(1, n + 1)
                                             if s not in set(missing)]
    return run_stream(evs, cfg, batch_size=batch_size, trailing_idle_batches=idle,
                      idle_event_time_step_ms=idle_step, watermark_delay_ms=wm)


def check_alarm_arithmetic():
    section("1. The alarm — computed in the pure core, so it is testable at all")

    if compute_alarm({}, watermark_ms=500) is None:
        ok("no buffer means no alarm", "nothing to wait for")
    else:
        fail("empty-buffer alarm", str(compute_alarm({}, 500)))

    buf = {7: (100, 7_000), 9: (100, 9_000), 8: (100, 8_000)}
    if compute_alarm(buf, watermark_ms=0) == 7_000:
        ok("alarm = the earliest buffered successor's event_ts",
           "seq N-1 happened no later than seq N")
    else:
        fail("alarm point", str(compute_alarm(buf, 0)))

    if compute_alarm({7: (100, 7_000)}, watermark_ms=9_000) == 9_001:
        ok("clamped to watermark + 1",
           "setTimeoutTimestamp throws at or below the watermark; and if the alarm "
           "is already behind it, the gap is already confirmable")
    else:
        fail("clamp", str(compute_alarm({7: (100, 7_000)}, 9_000)))

    if compute_alarm({7: (100, 7_000)}, watermark_ms=100_000, defer_ms=60_000) == 160_000:
        ok("HOLD defers from the watermark, not the buffer", "bounded re-assertion")
    else:
        fail("hold defer", str(compute_alarm({7: (100, 7_000)}, 100_000, defer_ms=60_000)))


def check_three_way_property():
    section("2. The three-way timing property")

    _st, rec = stream(missing=[25])
    gaps = rec.gaps()
    if len(gaps) == 1:
        ok("exactly one gap for one hole", "not zero (lost alarm), not two (double fire)")
    else:
        fail("gap count", str(len(gaps)))
        return

    batch, seq, d = gaps[0]

    before = [b for b in rec.batches[:batch] if b["watermark_ms"] >= d["alarm_event_ts"]]
    if not before:
        ok("(a) NOT-BEFORE — no batch alerted while the watermark was behind the alarm",
           f"alarm at event_ts +{d['alarm_event_ts'] - BASE} ms")
    else:
        fail("not-before", f"{len(before)} eligible batches preceded the firing batch")

    if seq == 25 and (d["lo"], d["hi"], d["count"]) == (25, 25, 1):
        ok("(b) NOT-NEVER — the signal exists, with the range and its evidence",
           f"lo=25 hi=25 successor={d['successor_seq']}")
    else:
        fail("not-never", str(d))

    eligible = rec.first_batch_with_watermark_at_or_past(d["alarm_event_ts"])
    if eligible is not None and batch == eligible:
        ok("(c) BOUNDED — fires in the FIRST batch whose watermark passes the alarm",
           f"batch {batch}, bounded by watermark + one trigger")
    else:
        fail("bounded", f"fired in batch {batch}, first eligible was {eligible}")


def check_negative_controls():
    section("3. The two negative controls — the step that proves mechanism, not luck")

    rng = random.Random(4)
    evs = [ev(s) for s in range(1, 121)]
    for i in range(0, len(evs), 20):
        chunk = evs[i:i + 20]
        rng.shuffle(chunk)
        evs[i:i + 20] = chunk
    _st, rec = stream(events=evs)
    if not rec.gaps():
        ok("control 1: shuffled but complete stream -> ZERO alerts",
           "a reorder within the watermark is LATE, not LOST")
    else:
        fail("control 1", f"{len(rec.gaps())} false positives on a complete stream")

    rng = random.Random(11)
    evs = [ev(s) for s in range(1, 601)]
    for i in range(0, len(evs), 300):
        chunk = evs[i:i + 300]
        rng.shuffle(chunk)
        evs[i:i + 300] = chunk
    st_ok, rec_ok = stream(events=evs, batch_size=25, wm=30_000)
    st_bad, rec_bad = stream(events=evs, batch_size=25, wm=5_000)

    bal_ok = tuple_to_parts(st_ok)[1]
    bal_bad = tuple_to_parts(st_bad)[1]
    if not rec_ok.gaps() and rec_bad.gaps() and bal_bad < bal_ok:
        ok("control 2: same input, 5 s watermark -> false positives AND a short balance",
           f"{len(rec_bad.gaps())} spurious gaps, balance {bal_bad} vs {bal_ok} "
           f"({bal_ok - bal_bad} minor units of real money stepped over)")
    else:
        fail("control 2",
             f"gaps_30s={len(rec_ok.gaps())} gaps_5s={len(rec_bad.gaps())} "
             f"bal_30s={bal_ok} bal_5s={bal_bad}")


def check_policies():
    section("4. FLAG_AND_CONTINUE vs HOLD")

    st, _ = stream(missing=[25], cfg=FLAG)
    last, bal, buf, _ = tuple_to_parts(st)
    if (last, bal, buf) == (60, 59 * 100, {}):
        ok("FLAG_AND_CONTINUE steps over and drains",
           "the missing amount is excluded; everything else applies; account stays live")
    else:
        fail("flag_and_continue", str((last, bal, len(buf))))

    st, rec = stream(missing=[25], cfg=HOLD)
    last, bal, buf, _ = tuple_to_parts(st)
    if last == 24 and len(buf) == 35 and rec.gaps():
        ok("HOLD advances nothing and keeps buffering",
           f"last stays at 24, {len(buf)} successors held")
    else:
        fail("hold", str((last, len(buf), len(rec.gaps()))))

    _st, rec = stream(missing=[25], cfg=HOLD, idle=200)
    fires = [rec.watermark_at(b) for b, _s, _d in rec.gaps()]
    spacings = [fires[i + 1] - fires[i] for i in range(len(fires) - 1)]
    if len(fires) > 1 and all(sp >= 60_000 for sp in spacings):
        ok("HOLD re-asserts on a bounded cadence, not every trigger",
           f"{len(fires)} assertions, spacings {spacings[:3]} ms")
    else:
        fail("hold re-alert cadence", f"fires={len(fires)} spacings={spacings[:5]}")

    cfg = dict(HOLD, max_buffer_size=10)
    _st, rec = stream(missing=[5], cfg=cfg)
    if rec.overflows():
        ok("a held account eventually overflows to the DLQ — by design",
           "bounded memory beats unbounded hope")
    else:
        fail("hold overflow", "no overflow under HOLD with a small cap")


def check_coalescing():
    section("5. Range coalescing — one alert per outage, not one per event")

    _st, rec = stream(missing=range(101, 151), n=200)
    gaps = rec.gaps()
    if len(gaps) == 1 and gaps[0][2]["count"] == 50:
        _b, _s, d = gaps[0]
        ok("a 50-event outage is ONE alert", f"lo={d['lo']} hi={d['hi']} count={d['count']}")
    else:
        fail("coalescing", f"{len(gaps)} records for one contiguous outage")

    _st, rec = stream(missing=[10, 40], n=80)
    ranges = sorted((d["lo"], d["hi"]) for _b, _s, d in rec.gaps())
    if ranges == [(10, 10), (40, 40)]:
        ok("two separate holes stay two ranges", str(ranges))
    else:
        fail("separate holes", str(ranges))


def check_gap_parity(trials):
    section("6. Gapped streams now match the oracle — new on Day 3")

    bad = []
    for t in range(trials):
        rng = random.Random(200_000 + t)
        n = rng.randint(20, 80)
        holes = set(rng.sample(range(2, n), rng.randint(1, 3)))
        amounts = {s: rng.randint(-5000, 5000) for s in range(1, n + 1)}
        delivery = [s for s in range(1, n + 1) if s not in holes]
        rng.shuffle(delivery)
        evs = [{"seq_no": s, "amount_minor": amounts[s], "event_ts_ms": BASE + s * STEP}
               for s in delivery]

        st, rec = run_stream(evs, FLAG, batch_size=rng.randint(1, 10),
                             trailing_idle_batches=60, idle_event_time_step_ms=5000)
        last, bal, buf, _ = tuple_to_parts(st)

        rows = [{"publish_order": j, "account_id": "A1", "seq_no": s,
                 "amount_minor": amounts[s], "event_type": "CREDIT",
                 "event_ts_ms": BASE + s * STEP} for j, s in enumerate(delivery)]
        o = compute_oracle(rows, "FLAG_AND_CONTINUE")["A1"]

        engine_ranges = sorted((d["lo"], d["hi"]) for _b, _s, d in rec.gaps())
        oracle_ranges = sorted((r[0], r[1]) for r in o["expected_gap_ranges"])

        if not (buf == {}
                and bal == o["expected_balance_minor"]
                and last == o["expected_last_applied_seq"]
                and engine_ranges == oracle_ranges):
            bad.append((t, last, o["expected_last_applied_seq"], engine_ranges, oracle_ranges))

    if not bad:
        ok(f"{trials} random gapped + shuffled streams match the oracle exactly",
           "balance, last_applied_seq, empty buffer, AND the coalesced gap ranges")
    else:
        t, l, ol, er, orr = bad[0]
        fail("gap parity", f"{len(bad)}/{trials} disagreed; first trial {t}: "
                           f"last={l} oracle={ol} ranges={er} vs {orr}")


def check_interactions():
    section("7. Interaction with the other branches")

    from spark.engine.sequencer import step
    from spark.engine.statecore import empty_state

    st, _ = step(empty_state(), [ev(1), ev(2), ev(4), ev(5)], FLAG, watermark_ms=0)
    st, _ = step(st, [], FLAG, timed_out=True, watermark_ms=BASE + 10 * STEP)
    st, out = step(st, [ev(3)], FLAG, watermark_ms=BASE + 10 * STEP)
    if tuple_to_parts(st)[1] == 400 and any(k == "DUP_DROPPED" for k, _s, _d in out):
        ok("a stepped-over event that arrives later is DROPPED, not applied",
           "the watermark's verdict is final — otherwise the balance would move twice")
    else:
        fail("post-gap arrival", str(tuple_to_parts(st)[:2]))

    st, _ = step(empty_state(), [ev(1), ev(2)], FLAG)
    st2, out = step(st, [], FLAG, timed_out=True, watermark_ms=BASE + 99_999)
    if not any(k == "SEQUENCE_GAP" for k, _s, _d in out):
        ok("a timeout on an EMPTY buffer is a no-op today", "Day 5 makes it the TTL flush")
    else:
        fail("empty-buffer timeout", str(out))

    try:
        gap_policy.advance(1, 100, {3: (100, 300)}, "NONSENSE")
        fail("unknown policy", "did not raise")
    except ValueError:
        ok("an unknown gap_policy fails loudly at the point of use",
           "a typo in config must not silently select a default")


def main() -> None:
    p = argparse.ArgumentParser(description="Day 3 verification")
    p.add_argument("--trials", type=int, default=300)
    args = p.parse_args()

    print("Project 3 — Day 3 verification (hermetic: no Docker, no Kafka, no JVM)")
    print("=" * 74)
    print(f"{DIM}Timing is proved through spark/tests/harness.py, a MODEL of Spark's")
    print(f"watermark and timeout semantics. Block 3.4 measures it for real.{RESET}")

    check_alarm_arithmetic()
    check_three_way_property()
    check_negative_controls()
    check_policies()
    check_coalescing()
    check_gap_parity(args.trials)
    check_interactions()

    print("\n" + "=" * 74)
    print(f"{GREEN}{len(PASSES)} passed{RESET}   {RED}{len(FAILS)} failed{RESET}")
    if FAILS:
        print("\nfailures:")
        for f in FAILS:
            print(f"  - {f}")
        sys.exit(1)
    print("\nGap logic green. Bring the stack up and measure it for real.")
    sys.exit(0)


if __name__ == "__main__":
    main()
