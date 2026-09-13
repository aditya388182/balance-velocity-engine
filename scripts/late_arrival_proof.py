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


def main() -> None:
    p = argparse.ArgumentParser(description="Late-arrival-after-gap proof")
    p.add_argument("--account", default="HOT-1")
    p.add_argument("--seq", type=int, default=1, help="the seq that was released late")
    p.add_argument("--expect-balance", type=int, required=True,
                   help="the balance BEFORE the late arrival — it must not have moved")
    p.add_argument("--expect-last", type=int, required=True)
    p.add_argument("--save", default=None)
    args = p.parse_args()

    from spark.utils.session import build_spark
    spark = build_spark(CFG, app_name="late-arrival-proof", streaming=False)
    try:
        bal_rows = (spark.read.format("delta").load(CFG["paths"]["balances"])
                    .select("account_id", "last_applied_seq", "balance_minor")
                    .collect())
        try:
            integ = (spark.read.format("delta").load(CFG["paths"]["integrity"])
                     .select("account_id", "kind", "seq_no", "detail").collect())
        except Exception:
            integ = []
    finally:
        spark.stop()

    row = next((r for r in bal_rows if r["account_id"] == args.account), None)
    if row is None:
        print(f"{RED}account {args.account} is not in the balances table{RESET}")
        sys.exit(1)

    last = int(row["last_applied_seq"])
    bal = int(row["balance_minor"])

    dup_rows = [r for r in integ
                if r["account_id"] == args.account
                and r["kind"] == "DUP_DROPPED"
                and r["seq_no"] is not None and int(r["seq_no"]) == args.seq]
    gap_rows = [r for r in integ if r["account_id"] == args.account
                and r["kind"] == "SEQUENCE_GAP"]

    paths = set()
    for r in dup_rows:
        try:
            paths.add((json.loads(r["detail"]) or {}).get("path"))
        except Exception:
            pass

    print(f"account            : {args.account}")
    print(f"released seq       : {args.seq}")
    print(f"last_applied_seq   : {last:<12} expected {args.expect_last} (unchanged)")
    print(f"balance_minor      : {bal:<12} expected {args.expect_balance} (unchanged)")
    print(f"SEQUENCE_GAP rows  : {len(gap_rows)}  "
          f"{DIM}(the hole was confirmed before the head arrived){RESET}")
    print(f"DUP_DROPPED seq={args.seq:<4}: {len(dup_rows)} row(s)  paths={sorted(p for p in paths if p)}")
    print("=" * 78)

    failures = []
    if last != args.expect_last:
        failures.append(f"last_applied_seq moved: {last} != {args.expect_last}")
    if bal != args.expect_balance:
        failures.append(f"THE BALANCE MOVED: {bal} != {args.expect_balance} "
                        f"(diff {bal - args.expect_balance:+d}) — an event the engine "
                        f"already reported as LOST was applied after the fact")
    if not gap_rows:
        failures.append("no SEQUENCE_GAP was ever recorded — the hole was never "
                        "confirmed, so this is not the scenario under test")
    if not dup_rows:
        failures.append(f"no DUP_DROPPED record for seq {args.seq} — the drop was "
                        f"SILENT, or the event was never consumed (was the engine given "
                        f"time to commit a batch?)")
    elif "applied" not in paths:
        failures.append(f"DUP_DROPPED seq {args.seq} took path {sorted(paths)}, "
                        f"expected 'applied' (seq <= last)")

    if args.save:
        Path(args.save).write_text(json.dumps({
            "account": args.account, "released_seq": args.seq,
            "last_applied_seq": last, "balance_minor": bal,
            "dup_rows": len(dup_rows), "dup_paths": sorted(p for p in paths if p),
            "gap_rows": len(gap_rows),
        }, indent=2))
        print(f"saved -> {args.save}")

    if failures:
        print(f"{RED}LATE ARRIVAL PROOF FAIL{RESET}")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)

    print(f"{GREEN}THE VERDICT HELD{RESET} — seq {args.seq} arrived after the watermark "
          f"had already declared it lost, was dropped with a record, and the balance "
          f"did not move.")
    print(f"{DIM}Without this, an event reported as LOST could turn up later and "
          f"silently move a balance nobody expected to move.{RESET}")
    sys.exit(0)


if __name__ == "__main__":
    main()
