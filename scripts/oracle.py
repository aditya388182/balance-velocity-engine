#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from conf.config import CFG  # noqa: E402


def read_delivery_log(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    rows.sort(key=lambda r: r["publish_order"])
    return rows


def coalesce_ranges(seqs) -> List[Tuple[int, int, int]]:
    """[3] -> [(3,3,1)];  [7,8,9,15] -> [(7,9,3),(15,15,1)]

    Coalescing matters operationally, not cosmetically: a 50-event upstream outage
    must read as ONE SEQUENCE_GAP(lo=101, hi=150, count=50), not fifty alerts. The
    gap-storm runbook depends on the blast radius being legible at a glance.
    """
    ranges: List[List[int]] = []
    for s in sorted(set(int(x) for x in seqs)):
        if ranges and s == ranges[-1][1] + 1:
            ranges[-1][1] = s
        else:
            ranges.append([s, s])
    return [(lo, hi, hi - lo + 1) for lo, hi in ranges]


def compute_oracle(rows: List[Dict[str, Any]], gap_policy: str | None = None,
                   max_buffer_size: int | None = None) -> Dict[str, Dict[str, Any]]:
    gap_policy = gap_policy or CFG["gap_policy"]
    max_buffer_size = int(max_buffer_size or CFG["max_buffer_size"])

    per_account: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        per_account[r["account_id"]].append(r)

    result: Dict[str, Dict[str, Any]] = {}
    for account_id, events in per_account.items():
        first_amount: Dict[int, int] = {}
        dup_dropped: List[int] = []

        for e in events:
            s = int(e["seq_no"])
            if s in first_amount:
                dup_dropped.append(s)         # every re-delivery is one drop
            else:
                first_amount[s] = int(e["amount_minor"])

        delivered = set(first_amount)
        hi_seq = max(delivered)
        missing = sorted(set(range(1, hi_seq + 1)) - delivered)
        gap_ranges = coalesce_ranges(missing)
        head_withheld = 1 in missing
        overflow_evicted: List[int] = []
        overflow_determinate = True
        if head_withheld:
            ordered = sorted(delivered)
            if len(ordered) > max_buffer_size:
                overflow_evicted = ordered[:-max_buffer_size]
        elif len(delivered) > max_buffer_size:
            # With an appliable head the buffer drains, so overflow only occurs
            # under a burst larger than the cap AND depends on batch boundaries.
            overflow_determinate = False

        if gap_policy == "HOLD" and missing:
            first_hole = missing[0]
            applied = {s for s in delivered if s < first_hole}
        else:
            applied = set(delivered)
        applied -= set(overflow_evicted)

        balance = sum(first_amount[s] for s in applied)
        last_applied_seq = max(applied) if applied else 0

        result[account_id] = {
            "account_id": account_id,
            "delivered_count": len(events),
            "unique_delivered": len(delivered),
            "applied_set": sorted(applied),
            "applied_count": len(applied),
            "expected_balance_minor": balance,
            "expected_last_applied_seq": last_applied_seq,
            "expected_gap_ranges": [list(r) for r in gap_ranges],
            "expected_dup_dropped": sorted(set(dup_dropped)),
            "expected_dup_multiplicity": len(dup_dropped),
            "expected_overflow_evicted": sorted(overflow_evicted),
            "overflow_determinate": overflow_determinate,
            "gap_policy": gap_policy,
            "max_buffer_size": max_buffer_size,
        }
    return result


def main() -> None:
    p = argparse.ArgumentParser(description="Independent oracle over a delivery log")
    p.add_argument("delivery_log")
    p.add_argument("--gap-policy", default=None, choices=["FLAG_AND_CONTINUE", "HOLD"])
    p.add_argument("--max-buffer-size", type=int, default=None)
    p.add_argument("--json-out", default=None)
    p.add_argument("--quiet", action="store_true")
    args = p.parse_args()

    rows = read_delivery_log(args.delivery_log)
    oracle = compute_oracle(rows, args.gap_policy, args.max_buffer_size)

    if not args.quiet:
        print(f"delivery log : {args.delivery_log}")
        print(f"published    : {len(rows)}")
        print(f"gap policy   : {args.gap_policy or CFG['gap_policy']}   "
              f"max_buffer_size: {args.max_buffer_size or CFG['max_buffer_size']}")
        print("-" * 92)
        print(f"{'account':<12}{'balance':>16}{'last_seq':>10}{'applied':>9}"
              f"{'delivered':>11}{'gaps':>7}{'dups':>7}{'overflow':>10}")
        print("-" * 92)
        for acct in sorted(oracle):
            o = oracle[acct]
            print(f"{acct:<12}{o['expected_balance_minor']:>16}"
                  f"{o['expected_last_applied_seq']:>10}{o['applied_count']:>9}"
                  f"{o['unique_delivered']:>11}{len(o['expected_gap_ranges']):>7}"
                  f"{len(o['expected_dup_dropped']):>7}"
                  f"{len(o['expected_overflow_evicted']):>10}")
        print("-" * 92)
        total = sum(len(o["expected_gap_ranges"]) + len(o["expected_dup_dropped"])
                    + len(o["expected_overflow_evicted"]) for o in oracle.values())
        print(f"expected integrity events total: {total}")
        for acct, o in sorted(oracle.items()):
            if o["expected_gap_ranges"]:
                print(f"  {acct} gap ranges      : {o['expected_gap_ranges']}")
            if o["expected_dup_dropped"]:
                print(f"  {acct} dup dropped     : {o['expected_dup_dropped']}")
            if not o["overflow_determinate"]:
                print(f"  {acct} overflow        : INDETERMINATE "
                      f"(appliable head + burst > cap; assert invariants, not the set)")

    if args.json_out:
        with open(args.json_out, "w") as fh:
            json.dump(oracle, fh, indent=2, sort_keys=True)
        if not args.quiet:
            print(f"wrote {args.json_out}")


if __name__ == "__main__":
    main()
