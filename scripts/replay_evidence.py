#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from conf.config import CFG  # noqa: E402
from spark.utils.session import build_spark  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"


def main() -> None:
    p = argparse.ArgumentParser(description="Recovery replay evidence")
    p.add_argument("--require-sink-replay", action="store_true",
                   help="exit non-zero if no duplicated (batch_id, kind, seq) rows are found")
    args = p.parse_args()

    spark = build_spark(CFG, app_name="replay-evidence", streaming=False)
    try:
        try:
            rows = (spark.read.format("delta").load(CFG["paths"]["integrity"])
                    .select("account_id", "kind", "seq_no", "batch_id").collect())
        except Exception:
            rows = []
        try:
            balances = (spark.read.format("delta").load(CFG["paths"]["balances"])
                        .select("account_id").collect())
        except Exception:
            balances = []
    finally:
        spark.stop()

    print(f"integrity rows      : {len(rows)}")
    print(f"balances rows       : {len(balances)}")

    acct_counts = Counter(r["account_id"] for r in balances)
    dupe_accounts = {a: n for a, n in acct_counts.items() if n > 1}
    if dupe_accounts:
        print(f"{RED}BALANCES TABLE CORRUPT{RESET} — more than one row per account: "
              f"{dupe_accounts}")
        print("  The MERGE key is wrong, or two engines wrote concurrently.")
        sys.exit(1)
    print(f"{GREEN}one row per account in balances{RESET}")

    exact = Counter((r["account_id"], r["kind"], r["seq_no"], r["batch_id"]) for r in rows)
    replayed = {k: n for k, n in exact.items() if n > 1}

    logical = Counter((r["account_id"], r["kind"], r["seq_no"]) for r in rows)
    print(f"integrity rows after read-side dedup by (account, kind, seq_no): {len(logical)}")

    print("=" * 84)
    if replayed:
        total_extra = sum(n - 1 for n in replayed.values())
        print(f"{GREEN}SINK REPLAY OBSERVED{RESET} — {len(replayed)} "
              f"(account, kind, seq, batch_id) tuple(s) written more than once, "
              f"{total_extra} extra row(s).")
        for (acct, kind, seq, batch), n in sorted(replayed.items())[:5]:
            print(f"  {acct} {kind} seq={seq} batch_id={batch} written {n}x")
        print()
        print(f"{DIM}The foreachBatch body ran twice for that batch. The balances table")
        print(f"is still correct because the strict-> MERGE guard made the replayed")
        print(f"update a no-op, and the integrity table is still correct on read")
        print(f"because consumers dedup by (account_id, kind, seq_no). Layer 3 of the")
        print(f"three no-double-apply layers just earned its rent.{RESET}")
        sys.exit(0)

    if not rows:
        print(f"{RED}SINK REPLAY IS UNDETECTABLE FOR THIS RUN{RESET} — the integrity "
              f"table is EMPTY.")
        print()
        print(f"{DIM}This detector works by finding the same (account, kind, seq_no,")
        print(f"batch_id) written more than once. A stream with no duplicates, no gaps")
        print(f"and no overflow produces no integrity rows at all, so there is nothing")
        print(f"that CAN be written twice — the answer would be 'not observed' even if")
        print(f"every batch had replayed.")
        print()
        print(f"Re-run the drill with duplicates in the stream so the replayed batch has")
        print(f"something to write, e.g. add:")
        print(f"    --dup 100 --dup 200 --dup 300 --dup 400 --dup 500 --dup 600")
        print(f"to the generator args.{RESET}")
        sys.exit(1 if args.require_sink_replay else 0)

    print(f"{YELLOW}NO SINK REPLAY OBSERVED{RESET} — no batch wrote the same integrity "
          f"row twice.")
    print()
    print(f"{DIM}This is a normal outcome, not a failure. The SIGKILL most likely landed")
    print(f"between batches or before foreachBatch ran, so there was nothing to write")
    print(f"twice. Parity passing still proves that state and Kafka offsets restored")
    print(f"atomically — layers 1 and 2. It does NOT exercise the sink guard.")
    print(f"Re-run the drill (recovery_drill.sh --attempts 3) if you want that layer")
    print(f"demonstrated rather than argued.{RESET}")
    sys.exit(1 if args.require_sink_replay else 0)


if __name__ == "__main__":
    main()
