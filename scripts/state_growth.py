#!/usr/bin/env python3
# scripts/state_growth.py
"""Measure the STATE STORE, not the process. The correct instrument for Stage 5.

WHY RSS WAS THE WRONG INSTRUMENT
--------------------------------
Two things made the capped-vs-uncapped RSS contrast unmeasurable on a laptop:

  1. run/engine.pid is the PYTHON driver. PySpark launches the JVM as a child
     process, and the state store, the RocksDB block cache and the whole heap live
     in the JVM. Sampling the Python process measures an interpreter holding no
     data — which is exactly what "251.9 MB baseline, 252.0 MB peak, 0.1 MB growth
     on both runs" looks like.

  2. Even with the right process, the burst is too small. A 10,000-event burst at
     cap 1000 versus cap 200000 differs by about 9,000 buffered entries — roughly
     650 KiB. Next to a JVM heap that is noise. RSS would need a burst three orders
     of magnitude larger to move.

WHY THIS SCRIPT IS *ALSO* NOT THE STAGE-5 INSTRUMENT
----------------------------------------------------
It was written as the fix for the RSS problem and it is still wrong for that job.
applyInPandasWithState keeps ONE STATE ROW PER GROUPING KEY, and the pending buffer
is a pickled blob INSIDE that row — so five accounts report five state rows whether
they hold 1,000 buffered events or 10,000. `memoryUsedBytes` does not help either:
for the RocksDB provider it is dominated by memtable and block-cache overhead, not
by the logical size of the values.

Use scripts/buffer_proof.py for Stage 5. It reads `buffer_size` from the balances
table — the number the mechanism itself emits — and the DLQ record count.

This script remains useful for what it actually measures: how many ACCOUNTS are
being held in state over time. That is Day 5's TTL curve, where state rows falling
as idle accounts evict is exactly the thing to watch.

    # after a run
    python scripts/state_growth.py --check logs/progress_capped.jsonl

    # the contrast
    python scripts/state_growth.py --compare logs/progress_capped.jsonl \
                                             logs/progress_uncapped.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"


DEFAULT_QUERY = "balance_engine"


def load(path: str, query_name: str | None = DEFAULT_QUERY) -> List[Dict[str, Any]]:
    """One query's progress series.

    progress.jsonl holds EVERY query since the Day-5 velocity fix, each line tagged
    with query_name. Reading them all as one series interleaves three unrelated
    curves: the sequencer holds one state row per ACCOUNT (~1000), while each
    velocity query holds one per account x window (~10,000). The mixture oscillates
    between them and no threshold on it means anything.

    Filtering is therefore not a nicety. A state-row curve is only a curve if every
    point comes from the same operator.
    """
    rows, seen_names = [], set()
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if r.get("batch_id") is None:
            continue
        name = r.get("query_name")
        if name:
            seen_names.add(name)
        if query_name and name and name != query_name:
            continue
        rows.append(r)
    rows.sort(key=lambda r: r["batch_id"])
    if not rows and seen_names:
        raise SystemExit(
            f"no progress rows for query {query_name!r}; "
            f"this file holds {sorted(seen_names)}. Pass --query.")
    return rows


def summarise(path: str, query_name: str | None = DEFAULT_QUERY) -> Dict[str, Any]:
    rows = load(path, query_name)
    if not rows:
        raise SystemExit(f"{path} has no usable progress rows — was the engine running?")
    r_rows = [r.get("state_rows") for r in rows if r.get("state_rows") is not None]
    r_bytes = [r.get("state_bytes") for r in rows if r.get("state_bytes") is not None]
    if not r_rows:
        raise SystemExit(f"{path} has no stateOperators metrics — "
                         f"check spark.sql.streaming.metricsEnabled")
    return {
        "path": path,
        "query_name": query_name,
        "batches": len(rows),
        "peak_state_rows": max(r_rows),
        "final_state_rows": r_rows[-1],
        "peak_state_mb": (max(r_bytes) / 1024 / 1024) if r_bytes else float("nan"),
        "final_state_mb": (r_bytes[-1] / 1024 / 1024) if r_bytes else float("nan"),
        "rows_series": r_rows,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="State store growth from progress.jsonl")
    p.add_argument("--check", help="assert the state store plateaued rather than climbing")
    p.add_argument("--max-rows", type=int, default=None,
                   help="expected plateau, e.g. max_buffer_size + a few account rows")
    p.add_argument("--compare", nargs=2, metavar=("CAPPED", "UNCAPPED"))
    p.add_argument("--expect-eviction", action="store_true",
                   help="Stage 6: assert state rows PEAKED and then FELL as idle "
                        "accounts were evicted. This is the job this script is "
                        "actually right for — state rows count ACCOUNTS, and TTL "
                        "eviction is precisely a change in the number of accounts.")
    p.add_argument("--min-decline-pct", type=float, default=50.0)
    p.add_argument("--query", default=DEFAULT_QUERY,
                   help="which streaming query's series to read. progress.jsonl "
                        "holds all of them; mixing them is meaningless.")
    args = p.parse_args()

    if args.compare:
        a, b = (summarise(x, args.query) for x in args.compare)
        print(f"{'':<20}{'capped':>16}{'uncapped':>16}")
        for k in ("batches", "peak_state_rows", "final_state_rows",
                  "peak_state_mb", "final_state_mb"):
            va, vb = a[k], b[k]
            fmt = ".1f" if isinstance(va, float) else "d"
            print(f"{k:<20}{va:>16{fmt}}{vb:>16{fmt}}")
        print()
        if b["peak_state_rows"] > a["peak_state_rows"] * 1.5:
            print(f"{GREEN}CONTRAST HOLDS{RESET} — the cap held the state store at "
                  f"{a['peak_state_rows']} rows where the uncapped run reached "
                  f"{b['peak_state_rows']}.")
            print(f"{DIM}That ratio is the Interview-Q4 answer: the cap is what turns an")
            print(f"unbounded per-account buffer into a bounded one, and the DLQ is where")
            print(f"the difference went.{RESET}")
            sys.exit(0)
        print(f"{RED}CONTRAST DID NOT HOLD{RESET} — the uncapped run did not hold more "
              f"state ({b['peak_state_rows']} vs {a['peak_state_rows']}).")
        print("  Check: was P3_MAX_BUFFER_SIZE actually echoed by [config] at startup,")
        print("  and did the burst finish publishing before the engine was stopped?")
        sys.exit(1)

    if args.expect_eviction:
        s = summarise(args.check or "logs/progress.jsonl", args.query)
        series = s["rows_series"]
        peak = max(series)
        peak_at = series.index(peak)
        after = series[peak_at:]
        final = series[-1]
        decline_pct = 0.0 if peak == 0 else 100.0 * (peak - final) / peak
        print(f"file             : {s['path']}")
        print(f"query            : {s['query_name']}")
        print(f"batches          : {s['batches']}")
        print(f"peak state rows  : {peak} (batch index {peak_at})")
        print(f"final state rows : {final}")
        print(f"decline          : {decline_pct:.0f}%   "
              f"(need >= {args.min_decline_pct:.0f}%)")
        print("curve            : " + " ".join(str(x) for x in series[::max(1, len(series)//20)]))
        if len(series) < 6:
            print(f"{YELLOW}TOO FEW BATCHES{RESET} — a curve through 5 points is not a curve")
            sys.exit(1)
        if peak_at == len(series) - 1:
            print(f"{RED}STATE NEVER FELL{RESET} — it peaked on the last batch, so the "
                  f"run ended before TTL could evict. Let it idle for at least one "
                  f"state_ttl beyond the last event.")
            sys.exit(1)
        if decline_pct >= args.min_decline_pct:
            print(f"{GREEN}TTL EVICTION OBSERVED{RESET} — state rose to {peak} accounts "
                  f"and fell to {final} as idle keys were released")
            sys.exit(0)
        print(f"{RED}INSUFFICIENT DECLINE{RESET} — {decline_pct:.0f}% is below the "
              f"{args.min_decline_pct:.0f}% threshold. Is state_ttl_ms set, and did "
              f"event time advance past it? The TTL is measured against the WATERMARK.")
        sys.exit(1)

    if not args.check:
        raise SystemExit("--check or --compare is required")

    s = summarise(args.check, args.query)
    print(f"file             : {s['path']}")
    print(f"batches          : {s['batches']}")
    print(f"peak state rows  : {s['peak_state_rows']}")
    print(f"final state rows : {s['final_state_rows']}")
    print(f"peak state size  : {s['peak_state_mb']:.1f} MB")

    if s["batches"] < 4:
        print(f"{YELLOW}TOO FEW BATCHES{RESET} — a plateau across 3 points is not evidence")
        sys.exit(1)

    if args.max_rows is not None:
        if s["peak_state_rows"] <= args.max_rows:
            print(f"{GREEN}STATE BOUNDED{RESET} — peaked at {s['peak_state_rows']} rows, "
                  f"within the expected {args.max_rows}")
            sys.exit(0)
        print(f"{RED}STATE EXCEEDED THE CAP{RESET} — {s['peak_state_rows']} rows "
              f"where at most {args.max_rows} was expected. Is max_buffer_size in effect?")
        sys.exit(1)

    series = s["rows_series"]
    tail = series[len(series) // 2:]
    if tail and max(tail) - min(tail) <= max(2, 0.1 * max(tail)):
        print(f"{GREEN}STATE PLATEAUED{RESET} — the second half of the run stayed within "
              f"{max(tail) - min(tail)} rows")
        sys.exit(0)
    print(f"{RED}STATE STILL CLIMBING{RESET} — second half moved from {min(tail)} to "
          f"{max(tail)} rows")
    sys.exit(1)


if __name__ == "__main__":
    main()
