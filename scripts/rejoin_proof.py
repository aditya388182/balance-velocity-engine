#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from conf.config import CFG  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"


def read_state(account: str):
    from spark.utils.session import build_spark
    spark = build_spark(CFG, app_name="rejoin-proof", streaming=False)
    try:
        rows = (spark.read.format("delta").load(CFG["paths"]["balances"])
                .select("account_id", "last_applied_seq", "balance_minor").collect())
        try:
            integ = (spark.read.format("delta").load(CFG["paths"]["integrity"])
                     .select("account_id", "kind", "seq_no").collect())
        except Exception:
            integ = []
    finally:
        spark.stop()
    row = next((r for r in rows if r["account_id"] == account), None)
    ttl = sum(1 for r in integ if r["account_id"] == account and r["kind"] == "TTL_FLUSH")
    gaps = sum(1 for r in integ if r["account_id"] == account and r["kind"] == "SEQUENCE_GAP")
    return row, ttl, gaps, len(rows)


def main() -> None:
    p = argparse.ArgumentParser(description="TTL rejoin proof")
    p.add_argument("--account", required=True)
    p.add_argument("--snapshot", help="write the current state to JSON and exit")
    p.add_argument("--compare", help="compare against a snapshot taken earlier")
    p.add_argument("--expect-delta", type=int, default=None,
                   help="the exact balance change the post-rejoin events should cause")
    args = p.parse_args()

    row, ttl_flushes, gaps, n_accounts = read_state(args.account)
    if row is None:
        print(f"{RED}account {args.account} is not in the balances table{RESET}")
        sys.exit(1)

    state = {"account_id": args.account,
             "last_applied_seq": int(row["last_applied_seq"]),
             "balance_minor": int(row["balance_minor"]),
             "ttl_flushes": ttl_flushes, "gap_rows": gaps,
             "accounts_in_table": n_accounts}

    if args.snapshot:
        Path(args.snapshot).write_text(json.dumps(state, indent=2))
        print(f"snapshot -> {args.snapshot}")
        print(f"  last_applied_seq {state['last_applied_seq']}   "
              f"balance {state['balance_minor']}")
        sys.exit(0)

    if not args.compare:
        print(json.dumps(state, indent=2))
        sys.exit(0)

    before = json.loads(Path(args.compare).read_text())
    d_seq = state["last_applied_seq"] - before["last_applied_seq"]
    d_bal = state["balance_minor"] - before["balance_minor"]
    new_flushes = state["ttl_flushes"] - before["ttl_flushes"]
    new_gaps = state["gap_rows"] - before["gap_rows"]

    print(f"account            : {args.account}")
    print(f"last_applied_seq   : {before['last_applied_seq']} -> "
          f"{state['last_applied_seq']}   ({d_seq:+d})")
    print(f"balance_minor      : {before['balance_minor']} -> "
          f"{state['balance_minor']}   ({d_bal:+d})")
    print(f"TTL_FLUSH rows     : +{new_flushes}")
    print(f"SEQUENCE_GAP rows  : +{new_gaps}")
    print("=" * 78)

    failures = []
    if new_flushes < 1:
        failures.append("no TTL_FLUSH was recorded — the account was never evicted, so "
                        "this run does not test a rejoin at all")
    if d_seq <= 0:
        failures.append(f"last_applied_seq did not advance ({d_seq:+d}) — the returning "
                        f"event never applied")
    if new_gaps > 0:
        failures.append(f"{new_gaps} PHANTOM GAP(S) on the rejoin — the re-seed did not "
                        f"happen, so the account restarted from zero and the balance is "
                        f"now wrong. Check `rejoin_reseed` in engine_config.yml and the "
                        f"'[engine] rejoin' line in logs/engine.log.")
    if args.expect_delta is not None and d_bal != args.expect_delta:
        failures.append(f"balance moved by {d_bal:+d}, expected {args.expect_delta:+d}")
    if d_bal == -before["balance_minor"] + (args.expect_delta or 0):
        failures.append("the balance was REPLACED rather than continued — the opening "
                        "balance was lost")

    if failures:
        print(f"{RED}REJOIN PROOF FAIL{RESET}")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)

    print(f"{GREEN}THE BALANCE SURVIVED EVICTION{RESET} — the account was flushed and "
          f"released, came back cold, was re-seeded from its durable row, and continued "
          f"from {before['balance_minor']} instead of from zero.")
    print(f"{DIM}Without the re-seed this same run produces a phantom gap and a balance "
          f"computed from zero that the MERGE guard would have accepted.{RESET}")
    sys.exit(0)


if __name__ == "__main__":
    main()
