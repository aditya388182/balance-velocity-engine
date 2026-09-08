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

GREEN, RED, RESET = "\033[92m", "\033[91m", "\033[0m"


def latest_delivery_log() -> str:
    logs = sorted(glob.glob(str(REPO_ROOT / "delivery_log_*.jsonl")),
                  key=lambda p: Path(p).stat().st_mtime)
    if not logs:
        raise SystemExit("no delivery_log_*.jsonl found — run the generator first")
    return logs[-1]


def main() -> None:
    p = argparse.ArgumentParser(description="Delta balances vs. oracle")
    p.add_argument("--delivery-log", default=None)
    p.add_argument("--gap-policy", default=None, choices=["FLAG_AND_CONTINUE", "HOLD"])
    p.add_argument("--expect-zero-deferred", action="store_true",
                   help="Stage 0: assert the engine deferred nothing")
    p.add_argument("--save-state", default=None,
                   help="write this run's engine state to JSON (for a Day-2 A/B compare)")
    args = p.parse_args()

    log_path = args.delivery_log or latest_delivery_log()
    rows = read_delivery_log(log_path)
    oracle = compute_oracle(rows, args.gap_policy)

    spark = build_spark(CFG, app_name="parity-balance", streaming=False)
    try:
        engine_rows = (spark.read.format("delta").load(CFG["paths"]["balances"])
                       .select("account_id", "last_applied_seq", "balance_minor",
                               "buffer_size", "detail")
                       .collect())
    finally:
        spark.stop()

    engine = {r["account_id"]: r.asDict() for r in engine_rows}

    print(f"delivery log : {log_path}")
    print(f"gap policy   : {args.gap_policy or CFG['gap_policy']}")
    print(f"oracle accts : {len(oracle)}   engine accts: {len(engine)}")
    print("=" * 104)
    print(f"{'account':<12}{'oracle_bal':>18}{'engine_bal':>18}{'o_seq':>9}{'e_seq':>9}"
          f"{'buf':>6}{'defer':>7}   verdict")
    print("=" * 104)

    failures = []
    snapshot = {}

    for acct in sorted(set(oracle) | set(engine)):
        o = oracle.get(acct)
        e = engine.get(acct)

        if o is None:
            failures.append(f"{acct}: engine has an account the oracle never saw")
            print(f"{acct:<12}{'--':>18}{e['balance_minor']:>18}{'--':>9}"
                  f"{e['last_applied_seq']:>9}{e['buffer_size']:>6}{'--':>7}   {RED}FAIL{RESET}")
            continue
        if e is None:
            failures.append(f"{acct}: missing from the engine's balances table")
            print(f"{acct:<12}{o['expected_balance_minor']:>18}{'--':>18}"
                  f"{o['expected_last_applied_seq']:>9}{'--':>9}{'--':>6}{'--':>7}"
                  f"   {RED}FAIL{RESET}")
            continue

        detail = json.loads(e["detail"]) if e["detail"] else {}
        deferred = int(detail.get("deferred", 0))
        acct_fail = []

        if e["balance_minor"] != o["expected_balance_minor"]:
            acct_fail.append(
                f"balance diff {e['balance_minor'] - o['expected_balance_minor']:+d} minor units")
        if e["last_applied_seq"] != o["expected_last_applied_seq"]:
            acct_fail.append(f"last_applied_seq {e['last_applied_seq']} "
                             f"!= oracle {o['expected_last_applied_seq']}")
        if args.expect_zero_deferred and deferred:
            acct_fail.append(f"{deferred} deferred event(s) on an ordered stream "
                             f"(seqs: {detail.get('deferred_seqs')})")

        verdict = f"{GREEN}PASS{RESET}" if not acct_fail else f"{RED}FAIL{RESET}"
        print(f"{acct:<12}{o['expected_balance_minor']:>18}{e['balance_minor']:>18}"
              f"{o['expected_last_applied_seq']:>9}{e['last_applied_seq']:>9}"
              f"{e['buffer_size']:>6}{deferred:>7}   {verdict}")
        failures.extend(f"{acct}: {f}" for f in acct_fail)

        snapshot[acct] = {
            "balance_minor": int(e["balance_minor"]),
            "last_applied_seq": int(e["last_applied_seq"]),
            "buffer_size": int(e["buffer_size"]),
            "deferred": deferred,
        }

    print("=" * 104)

    if args.save_state:
        with open(args.save_state, "w") as fh:
            json.dump(snapshot, fh, indent=2, sort_keys=True)
        print(f"engine state saved -> {args.save_state}")

    if failures:
        print(f"{RED}PARITY FAIL{RESET} — {len(failures)} mismatch(es):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)

    print(f"{GREEN}PARITY PASS{RESET} — engine state == oracle for all {len(oracle)} account(s)")
    sys.exit(0)


if __name__ == "__main__":
    main()
