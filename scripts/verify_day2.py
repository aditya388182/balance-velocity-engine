#!/usr/bin/env python3
from __future__ import annotations

import argparse
import random
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.oracle import compute_oracle  # noqa: E402
from spark.engine.sequencer import (DUP_PATH_APPLIED, DUP_PATH_BUFFERED,  # noqa: E402
                                    step)
from spark.engine.statecore import (KIND_BALANCE, KIND_DUP, KIND_OVERFLOW,  # noqa: E402
                                    empty_state, tuple_to_parts)

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"
PASSES, FAILS = [], []

CFG = {"max_buffer_size": 1000, "gap_policy": "FLAG_AND_CONTINUE"}


def ok(label, detail=""):
    PASSES.append(label)
    print(f"  {GREEN}PASS{RESET}  {label}" + (f"  {DIM}{detail}{RESET}" if detail else ""))


def fail(label, detail=""):
    FAILS.append(f"{label}: {detail}")
    print(f"  {RED}FAIL{RESET}  {label}" + (f"  {detail}" if detail else ""))


def section(title):
    print(f"\n{title}\n" + "-" * len(title))


def ev(s, a=100, ts=None):
    return {"seq_no": s, "amount_minor": a, "event_ts_ms": ts if ts is not None else 1_000_000 + s}


def feed(state, seqs, cfg=CFG, amounts=None):
    events = [ev(s, amounts[s] if amounts else 100) for s in seqs]
    return step(state, events, cfg)


def of_kind(out, kind):
    return [(s, d) for k, s, d in out if k == kind]


def check_branch_1_and_drain():
    section("1. Branch 1 — apply and drain (the reordering mechanism)")

    st, out = feed(empty_state(), [1, 2, 3, 4, 5])
    last, bal, buf, _ = tuple_to_parts(st)
    if (last, bal, buf) == (5, 500, {}) and [k for k, _, _ in out] == [KIND_BALANCE]:
        ok("in-order applies, no integrity events", "last=5 balance=500")
    else:
        fail("in-order", str((last, bal, buf)))

    st = empty_state()
    trace = []
    for s in [3, 2, 1]:
        st, _ = feed(st, [s])
        last, _, buf, _ = tuple_to_parts(st)
        trace.append((s, last, sorted(buf)))
    if trace == [(3, 0, [3]), (2, 0, [2, 3]), (1, 3, [])]:
        ok("[3,2,1] one batch at a time: 3 buffers, 2 buffers, 1 applies then DRAINS two",
           " -> ".join(f"{s}:last={l},buf={b}" for s, l, b in trace))
    else:
        fail("drain loop", str(trace))

    ordered = empty_state()
    for s in [1, 2, 3, 4, 5]:
        ordered, _ = feed(ordered, [s])
    jumbled = empty_state()
    for s in [3, 2, 1, 5, 4]:
        jumbled, _ = feed(jumbled, [s])
    if tuple_to_parts(ordered)[:3] == tuple_to_parts(jumbled)[:3]:
        ok("out-of-order reaches the identical final state", "this is Stage 1 in miniature")
    else:
        fail("out-of-order parity", f"{tuple_to_parts(ordered)[:2]} vs {tuple_to_parts(jumbled)[:2]}")

    st, _ = feed(empty_state(), [1, 2])
    st, out = feed(st, [5, 4, 3])
    if tuple_to_parts(st)[:3] == (5, 500, {}) and [k for k, _, _ in out] == [KIND_BALANCE]:
        ok("within-batch disorder never touches the buffer", "the sort is an optimisation")
    else:
        fail("within-batch sort", str(tuple_to_parts(st)[:3]))


def check_branch_4_duplicates():
    section("2. Branches 4 and 4b — duplicate drops, counted never silent")

    st, _ = feed(empty_state(), [1, 2, 3])
    st, out = feed(st, [2])
    dups = of_kind(out, KIND_DUP)
    if tuple_to_parts(st)[:2] == (3, 300) and len(dups) == 1 \
            and dups[0][1]["path"] == DUP_PATH_APPLIED:
        ok("duplicate of an APPLIED seq dropped, balance unmoved", 'path="applied"')
    else:
        fail("applied-duplicate", str((tuple_to_parts(st)[:2], dups)))

    st, _ = feed(empty_state(), [1, 3])
    st, out = feed(st, [3])
    dups = of_kind(out, KIND_DUP)
    buf = tuple_to_parts(st)[2]
    if len(buf) == 1 and len(dups) == 1 and dups[0][1]["path"] == DUP_PATH_BUFFERED:
        ok("duplicate of a BUFFERED seq not double-inserted", 'path="buffered"')
    else:
        fail("buffered-duplicate", str((sorted(buf), dups)))

    st = empty_state()
    for s in [1, 3, 3, 2]:
        st, _ = feed(st, [s])
    if tuple_to_parts(st)[:3] == (3, 300, {}):
        ok("[1,3,3,2] applies 3 exactly once on drain", "300, not 400")
    else:
        fail("buffered dup then drain", str(tuple_to_parts(st)[:3]))

    st, _ = feed(empty_state(), [1, 2, 3, 4, 5])
    before = tuple_to_parts(st)[:2]
    st, out = feed(st, [1, 2, 3, 4, 5])
    if tuple_to_parts(st)[:2] == before and len(of_kind(out, KIND_DUP)) == 5:
        ok("replaying an entire applied window changes nothing",
           "5 DUP records — the evidence the drop branch engaged")
    else:
        fail("full replay", str((tuple_to_parts(st)[:2], len(of_kind(out, KIND_DUP)))))


def check_branch_3_overflow():
    section("3. Branch 3 — min-first eviction over the buffer AND the arrival")
    small = {"max_buffer_size": 5, "gap_policy": "FLAG_AND_CONTINUE"}

    st = empty_state()
    evicted = []
    for s in range(2, 13):
        st, out = feed(st, [s], small)
        evicted += [q for q, _ in of_kind(out, KIND_OVERFLOW)]
    buf = tuple_to_parts(st)[2]
    if sorted(buf) == [8, 9, 10, 11, 12] and evicted == [2, 3, 4, 5, 6, 7]:
        ok("buffer pins at the cap, evicts min-first", f"kept {sorted(buf)}, evicted {evicted}")
    else:
        fail("overflow", f"buf={sorted(buf)} evicted={evicted}")

    st = empty_state()
    for s in [10, 11, 12, 13, 14]:
        st, _ = feed(st, [s], small)
    st, out = feed(st, [3], small)
    ovf = of_kind(out, KIND_OVERFLOW)
    if len(ovf) == 1 and ovf[0][0] == 3 and ovf[0][1]["evicted_on_arrival"] is True \
            and sorted(tuple_to_parts(st)[2]) == [10, 11, 12, 13, 14]:
        ok("an arrival below everything buffered is evicted ON ARRIVAL",
           "min(buf) alone would have discarded seq 10 and admitted seq 3")
    else:
        fail("evict-on-arrival", str((ovf, sorted(tuple_to_parts(st)[2]))))


def check_k_largest_invariant(trials):
    section("4. The k-largest invariant, against the REAL step() this time")
    small = {"max_buffer_size": 5, "gap_policy": "FLAG_AND_CONTINUE"}
    rng = random.Random(0)
    delivered = list(range(2, 42))               # head withheld
    want_buf = sorted(delivered)[-5:]
    want_ev = sorted(delivered)[:-5]
    bad = 0
    for t in range(trials):
        order = delivered[:]
        rng.shuffle(order)
        st = empty_state()
        evicted = []
        i = 0
        while i < len(order):                     # random batch boundaries too
            size = rng.randint(1, 4)
            st, out = feed(st, order[i:i + size], small)
            evicted += [q for q, _ in of_kind(out, KIND_OVERFLOW)]
            i += size
        if sorted(tuple_to_parts(st)[2]) != want_buf or sorted(evicted) != want_ev:
            bad += 1
    if bad == 0:
        ok(f"{trials} random arrival orders AND batch splits agree",
           f"buffer always {want_buf[0]}..{want_buf[-1]}, DLQ always the rest")
    else:
        fail("k-largest invariant", f"{bad}/{trials} disagreed")

    rows = [{"publish_order": i, "account_id": "A1", "seq_no": s, "amount_minor": 100,
             "event_type": "CREDIT", "event_ts_ms": 1000 + s}
            for i, s in enumerate(delivered)]
    o = compute_oracle(rows, "FLAG_AND_CONTINUE", 5)["A1"]
    if o["expected_overflow_evicted"] == want_ev and o["overflow_determinate"]:
        ok("the oracle predicts that same DLQ set from the delivery log alone",
           "Stage 5 never has to read the engine's own output back in")
    else:
        fail("oracle overflow prediction", str(o["expected_overflow_evicted"])[:100])


def check_restart():
    section("5. Restart — the cheapest exactly-once regression test")
    import pickle

    st, _ = feed(empty_state(), [1, 2, 3])
    restored = pickle.loads(pickle.dumps(st))
    restored, out = feed(restored, [2, 3])
    if tuple_to_parts(restored)[:2] == (3, 300) and len(of_kind(out, KIND_DUP)) == 2:
        ok("state round-trip then replay applies nothing twice",
           "2 DUP records prove the branch engaged, rather than the disaster not occurring")
    else:
        fail("restart replay", str(tuple_to_parts(restored)[:2]))

    restored, _ = feed(restored, [4])
    if tuple_to_parts(restored)[:2] == (4, 400):
        ok("and the stream continues correctly after the replay")
    else:
        fail("post-replay continuation", str(tuple_to_parts(restored)[:2]))

    st, _ = feed(empty_state(), [1, 5, 6])
    restored = pickle.loads(pickle.dumps(st))
    if sorted(tuple_to_parts(restored)[2]) == [5, 6]:
        ok("a pending buffer survives serialisation", "Day 4's mid-gap kill depends on this")
    else:
        fail("buffer survival", str(sorted(tuple_to_parts(restored)[2])))


def check_engine_vs_oracle(trials):
    section("6. Engine vs oracle — randomised property test")
    bad = []
    for t in range(trials):
        rng = random.Random(50_000 + t)
        n = rng.randint(3, 60)
        amounts = {s: rng.randint(-5000, 5000) for s in range(1, n + 1)}
        delivery = list(range(1, n + 1))
        for _ in range(rng.randint(0, 6)):
            delivery.append(rng.randint(1, n))
        rng.shuffle(delivery)

        st = empty_state()
        dup_seen = set()
        i = 0
        while i < len(delivery):
            size = rng.randint(1, 5)
            st, out = step(st, [ev(s, amounts[s]) for s in delivery[i:i + size]], CFG)
            dup_seen |= {q for q, _ in of_kind(out, KIND_DUP)}
            i += size
        last, bal, buf, _ = tuple_to_parts(st)

        rows = [{"publish_order": j, "account_id": "A1", "seq_no": s,
                 "amount_minor": amounts[s], "event_type": "CREDIT",
                 "event_ts_ms": 1000 + j} for j, s in enumerate(delivery)]
        o = compute_oracle(rows, "FLAG_AND_CONTINUE")["A1"]

        if not (buf == {} and bal == o["expected_balance_minor"]
                and last == o["expected_last_applied_seq"]
                and dup_seen == set(o["expected_dup_dropped"])):
            bad.append(t)

    if not bad:
        ok(f"{trials} random shuffled+duplicated streams match the oracle exactly",
           "balance, last_applied_seq, empty buffer, and the DUP set")
    else:
        fail("engine/oracle property test", f"{len(bad)} trials disagreed, first={bad[0]}")


def check_day3_boundary():
    section("7. The Day-2 boundary, asserted not assumed")
    st = empty_state()
    for s in [1, 2, 4, 5]:
        st, out = feed(st, [s])
    last, _, buf, _ = tuple_to_parts(st)
    if last == 2 and sorted(buf) == [4, 5] and "SEQUENCE_GAP" not in [k for k, _, _ in out]:
        ok("a real gap still stalls today — successors buffer forever",
           "no SEQUENCE_GAP exists future work: Day 3; do NOT run parity on a --gap stream")
    else:
        fail("gap boundary", str((last, sorted(buf))))

    st, out = step(empty_state(), [], CFG, timed_out=True, watermark_ms=999)
    if [k for k, _, _ in out] == [KIND_BALANCE]:
        ok("a timed-out invocation with zero rows survives", "works from state alone")
    else:
        fail("timeout path", str(out))


def main() -> None:
    p = argparse.ArgumentParser(description="Day 2 verification")
    p.add_argument("--trials", type=int, default=400,
                   help="property-test trial count (default 400)")
    args = p.parse_args()

    print("Day 2 verification (hermetic: no Docker, no Kafka, no JVM)")
    print("=" * 74)
    check_branch_1_and_drain()
    check_branch_4_duplicates()
    check_branch_3_overflow()
    check_k_largest_invariant(args.trials)
    check_restart()
    check_engine_vs_oracle(args.trials)
    check_day3_boundary()

    print("\n" + "=" * 74)
    print(f"{GREEN}{len(PASSES)} passed{RESET}   {RED}{len(FAILS)} failed{RESET}")
    if FAILS:
        print("\nfailures:")
        for f in FAILS:
            print(f"  - {f}")
        sys.exit(1)
    print("\nFour-branch machine green. Bring the stack up and run Stage 1.")
    sys.exit(0)


if __name__ == "__main__":
    main()
