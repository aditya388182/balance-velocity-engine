#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from conf.config import CFG  # noqa: E402
from scripts.oracle import compute_oracle, read_delivery_log  # noqa: E402
from spark.utils.session import build_spark  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"

KIND_DUP = "DUP_DROPPED"
KIND_OVERFLOW = "BUFFER_OVERFLOW"
KIND_GAP = "SEQUENCE_GAP"


def latest_delivery_log() -> str:
    logs = sorted(glob.glob(str(REPO_ROOT / "delivery_log_*.jsonl")),
                  key=lambda p: Path(p).stat().st_mtime)
    if not logs:
        raise SystemExit("no delivery_log_*.jsonl found — run the generator first")
    return logs[-1]


def read_engine_state(spark):
    """Return (balances_by_account, integrity_by_account) with read-side dedup."""
    balances = {}
    for r in (spark.read.format("delta").load(CFG["paths"]["balances"])
              .select("account_id", "last_applied_seq", "balance_minor", "buffer_size")
              .collect()):
        balances[r["account_id"]] = r.asDict()

    integrity = {}
    try:
        raw = (spark.read.format("delta").load(CFG["paths"]["integrity"])
               .select("account_id", "kind", "seq_no", "detail")
               .collect())
    except Exception:
        raw = []   # no integrity events have ever been written — legitimate

    for r in raw:
        node = integrity.setdefault(r["account_id"], {
            "dup": set(), "overflow": set(), "gap_ranges": set(),
            "rows": 0, "dup_paths": {},
        })
        node["rows"] += 1
        seq = int(r["seq_no"]) if r["seq_no"] is not None else None
        detail = json.loads(r["detail"]) if r["detail"] else {}
        if seq is None:
            continue
        if r["kind"] == KIND_DUP:
            node["dup"].add(seq)                    # read-side dedup by (acct, kind, seq)
            node["dup_paths"].setdefault(detail.get("path", "?"), set()).add(seq)
        elif r["kind"] == KIND_OVERFLOW:
            node["overflow"].add(seq)
        elif r["kind"] == KIND_GAP:
            node["gap_ranges"].add((int(detail.get("lo", seq)), int(detail.get("hi", seq))))
    return balances, integrity


def derive_applied_count(bal_row, integ) -> int:
    """last_applied_seq minus everything below it that never applied.

    The two exclusion sets OVERLAP and must be UNIONED, not summed. When a burst
    overflows before its gap is confirmed, the alarm fires with last=0 and the
    earliest buffered seq far above, so the SEQUENCE_GAP range spans everything
    below it — including the seqs we ourselves evicted to the DLQ. Those seqs are
    then in both sets.

    Summing the counts double-subtracts them and produces a NEGATIVE applied count
    (-8000 on a run where the true answer was 1000). A negative count is arithmetic,
    not physics, and it made a correct engine look broken.
    """
    last = int(bal_row["last_applied_seq"])
    never_applied = set()
    for lo, hi in integ.get("gap_ranges", set()):
        lo, hi = int(lo), min(int(hi), last)
        if hi >= lo:
            never_applied |= set(range(lo, hi + 1))
    never_applied |= {int(s) for s in integ.get("overflow", set()) if int(s) <= last}
    return last - len(never_applied)


def main() -> None:
    p = argparse.ArgumentParser(description="Delta balances + integrity vs. oracle")
    p.add_argument("--delivery-log", default=None)
    p.add_argument("--gap-policy", default=None, choices=["FLAG_AND_CONTINUE", "HOLD"])
    p.add_argument("--only-account", action="append", default=None,
                   help="compare only these accounts. A run that also published tick or "
                        "heartbeat traffic under a SEPARATE delivery log will otherwise "
                        "report those accounts as 'the oracle never saw' — true, and "
                        "not a defect.")
    p.add_argument("--strict", action="store_true",
                   help="fail on accounts the oracle marks INDETERMINATE instead of "
                        "reporting them as uncomparable")
    p.add_argument("--expect-gap-ranges", action="store_true",
                   help="Stage 2: assert the engine's SEQUENCE_GAP ranges equal the oracle's")
    p.add_argument("--expect-empty-buffer", action="store_true",
                   help="Stage 1: assert every account fully drained (buffer_size == 0)")
    p.add_argument("--expect-no-integrity", action="store_true",
                   help="Stage 1 shuffle run: nothing was lost, only late")
    # Day 1's flag. The `deferred` counter it referred to was a Day-1 scaffold that
    # branches 2-4 have now claimed, so the equivalent assertion is "no integrity
    # events at all". Accepted as an alias so every command in the Day 1 plan still
    # runs against Day 2 code.
    p.add_argument("--expect-zero-deferred", action="store_true",
                   help="Day 1 alias for --expect-no-integrity")
    p.add_argument("--save-state", default=None,
                   help="write this run's engine state to JSON (for an A/B comparison)")
    p.add_argument("--compare-state", default=None,
                   help="assert this run's engine state is identical to a saved run")
    args = p.parse_args()
    if args.expect_zero_deferred:
        args.expect_no_integrity = True
        print(f"{YELLOW}note{RESET} --expect-zero-deferred is a Day-1 alias for "
              f"--expect-no-integrity (the deferred counter was superseded by "
              f"branches 2-4)")

    log_path = args.delivery_log or latest_delivery_log()
    rows = read_delivery_log(log_path)
    oracle = compute_oracle(rows, args.gap_policy)

    spark = build_spark(CFG, app_name="parity-balance", streaming=False)
    try:
        balances, integrity = read_engine_state(spark)
    finally:
        spark.stop()

    print(f"delivery log : {log_path}")
    print(f"gap policy   : {args.gap_policy or CFG['gap_policy']}   "
          f"max_buffer_size: {CFG['max_buffer_size']}")
    print(f"oracle accts : {len(oracle)}   engine accts: {len(balances)}")
    print("=" * 112)
    print(f"{'account':<12}{'oracle_bal':>16}{'engine_bal':>16}{'o_seq':>8}{'e_seq':>8}"
          f"{'o_appl':>8}{'e_appl':>8}{'buf':>6}{'dup':>6}{'ovf':>6}   verdict")
    print("=" * 112)

    failures = []
    snapshot = {}
    indeterminate = []
    empty_integ = {"dup": set(), "overflow": set(), "gap_ranges": set(), "dup_paths": {}}

    scope = set(args.only_account) if args.only_account else None
    for acct in sorted(set(oracle) | set(balances)):
        if scope and acct not in scope:
            continue
        o = oracle.get(acct)
        e = balances.get(acct)
        integ = integrity.get(acct, empty_integ)

        if o is None:
            failures.append(f"{acct}: engine has an account the oracle never saw")
            print(f"{acct:<12}{'--':>16}{e['balance_minor']:>16}{'--':>8}"
                  f"{e['last_applied_seq']:>8}{'--':>8}{'--':>8}"
                  f"{e['buffer_size']:>6}{'--':>6}{'--':>6}   {RED}FAIL{RESET}")
            continue
        if e is None:
            failures.append(f"{acct}: missing from the engine's balances table")
            print(f"{acct:<12}{o['expected_balance_minor']:>16}{'--':>16}"
                  f"{o['expected_last_applied_seq']:>8}{'--':>8}"
                  f"{o['applied_count']:>8}{'--':>8}{'--':>6}{'--':>6}{'--':>6}"
                  f"   {RED}FAIL{RESET}")
            continue

        e_applied = derive_applied_count(e, integ)
        acct_fail = []

        # The oracle is order-independent BY DESIGN — that is what keeps it honest.
        # When it reports overflow as indeterminate it is telling you the outcome
        # depends on arrival order, and its balance and applied-count are computed on
        # a "nothing was evicted" assumption that the run may have violated. Comparing
        # them anyway turns the oracle's own admission of uncertainty into a FAIL.
        if not o["overflow_determinate"] and not args.strict:
            indeterminate.append(acct)
            print(f"{acct:<12}{o['expected_balance_minor']:>16}{e['balance_minor']:>16}"
                  f"{o['expected_last_applied_seq']:>8}{e['last_applied_seq']:>8}"
                  f"{o['applied_count']:>8}{e_applied:>8}{e['buffer_size']:>6}"
                  f"{len(integ['dup']):>6}{len(integ['overflow']):>6}   {YELLOW}N/A{RESET}")
            snapshot[acct] = {
                "balance_minor": int(e["balance_minor"]),
                "last_applied_seq": int(e["last_applied_seq"]),
                "applied_count": int(e_applied),
                "buffer_size": int(e["buffer_size"]),
                "dup": sorted(integ["dup"]), "overflow": sorted(integ["overflow"]),
            }
            continue

        if e["balance_minor"] != o["expected_balance_minor"]:
            acct_fail.append(
                f"balance diff {e['balance_minor'] - o['expected_balance_minor']:+d} minor units")
        if e["last_applied_seq"] != o["expected_last_applied_seq"]:
            acct_fail.append(f"last_applied_seq {e['last_applied_seq']} "
                             f"!= oracle {o['expected_last_applied_seq']}")
        if e_applied != o["applied_count"]:
            acct_fail.append(f"applied-count {e_applied} != oracle {o['applied_count']} "
                             f"(derived from balances + integrity)")

        o_dups = set(o["expected_dup_dropped"])
        if integ["dup"] != o_dups:
            missing = sorted(o_dups - integ["dup"])[:10]
            extra = sorted(integ["dup"] - o_dups)[:10]
            acct_fail.append(f"DUP_DROPPED set mismatch (missing={missing} extra={extra})")

        if o["overflow_determinate"]:
            o_ovf = set(o["expected_overflow_evicted"])
            if integ["overflow"] != o_ovf:
                acct_fail.append(f"BUFFER_OVERFLOW set mismatch "
                                 f"(engine={len(integ['overflow'])} oracle={len(o_ovf)})")

        if args.expect_gap_ranges:
            o_gaps = {(int(r[0]), int(r[1])) for r in o["expected_gap_ranges"]}
            if integ["gap_ranges"] != o_gaps:
                acct_fail.append(f"SEQUENCE_GAP ranges {sorted(integ['gap_ranges'])} "
                                 f"!= oracle {sorted(o_gaps)}")

        if args.expect_empty_buffer and int(e["buffer_size"]) != 0:
            acct_fail.append(f"buffer not drained: buffer_size={e['buffer_size']}")

        if args.expect_no_integrity and (integ["dup"] or integ["overflow"] or integ["gap_ranges"]):
            acct_fail.append("integrity events present on a run that should have none "
                             "(a reorder within the watermark is late, not lost)")

        verdict = f"{GREEN}PASS{RESET}" if not acct_fail else f"{RED}FAIL{RESET}"
        print(f"{acct:<12}{o['expected_balance_minor']:>16}{e['balance_minor']:>16}"
              f"{o['expected_last_applied_seq']:>8}{e['last_applied_seq']:>8}"
              f"{o['applied_count']:>8}{e_applied:>8}{e['buffer_size']:>6}"
              f"{len(integ['dup']):>6}{len(integ['overflow']):>6}   {verdict}")
        failures.extend(f"{acct}: {f}" for f in acct_fail)

        snapshot[acct] = {
            "balance_minor": int(e["balance_minor"]),
            "last_applied_seq": int(e["last_applied_seq"]),
            "applied_count": int(e_applied),
            "buffer_size": int(e["buffer_size"]),
            "dup": sorted(integ["dup"]),
            "overflow": sorted(integ["overflow"]),
        }

    print("=" * 112)

    dup_paths = {}
    for integ in integrity.values():
        for path, seqs in integ.get("dup_paths", {}).items():
            dup_paths[path] = dup_paths.get(path, 0) + len(seqs)
    if dup_paths:
        print("duplicate drop paths exercised: "
              + ", ".join(f"{k}={v}" for k, v in sorted(dup_paths.items())))

    if indeterminate:
        print()
        print(f"{YELLOW}{len(indeterminate)} account(s) UNCOMPARABLE{RESET}: "
              f"{', '.join(indeterminate)}")
        print(f"  {DIM}The oracle marked overflow indeterminate for these. That happens "
              f"when a burst larger than the cap has an APPLIABLE head, so which events")
        print(f"  survive depends on arrival order and micro-batch boundaries — something "
              f"no order-independent model can predict from a delivery log alone.")
        print(f"  Assert these with scripts/late_arrival_proof.py or "
              f"scripts/buffer_proof.py instead, or pass --strict to compare anyway.{RESET}")

    if args.save_state:
        with open(args.save_state, "w") as fh:
            json.dump(snapshot, fh, indent=2, sort_keys=True)
        print(f"engine state saved -> {args.save_state}")

    if args.compare_state:
        with open(args.compare_state) as fh:
            baseline = json.load(fh)
        ab_fail = []
        if set(baseline) != set(snapshot):
            ab_fail.append(f"A/B account sets differ: baseline={sorted(baseline)} "
                           f"this_run={sorted(snapshot)}")
        for acct in sorted(set(baseline) & set(snapshot)):
            b, c = baseline[acct], snapshot[acct]
            for field in ("balance_minor", "last_applied_seq", "applied_count"):
                if b[field] != c[field]:
                    ab_fail.append(f"{acct}: A/B {field} {c[field]} != baseline {b[field]}")
        failures.extend(ab_fail)
        if not ab_fail:
            print(f"{GREEN}A/B IDENTICAL{RESET} — this run == the baseline run "
                  f"for all {len(snapshot)} account(s)")

    if failures:
        print(f"{RED}PARITY FAIL{RESET} — {len(failures)} mismatch(es):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)

    print(f"{GREEN}PARITY PASS{RESET} — engine state == oracle for all {len(oracle)} account(s)")
    sys.exit(0)


if __name__ == "__main__":
    main()