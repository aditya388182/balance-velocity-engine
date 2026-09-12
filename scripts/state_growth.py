#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"


def load(path: str) -> List[Dict[str, Any]]:
    rows = []
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if r.get("batch_id") is not None:
            rows.append(r)
    rows.sort(key=lambda r: r["batch_id"])
    return rows


def summarise(path: str) -> Dict[str, Any]:
    rows = load(path)
    if not rows:
        raise SystemExit(f"{path} has no usable progress rows — was the engine running?")
    r_rows = [r.get("state_rows") for r in rows if r.get("state_rows") is not None]
    r_bytes = [r.get("state_bytes") for r in rows if r.get("state_bytes") is not None]
    if not r_rows:
        raise SystemExit(f"{path} has no stateOperators metrics — "
                         f"check spark.sql.streaming.metricsEnabled")
    return {
        "path": path,
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
    args = p.parse_args()

    if args.compare:
        a, b = (summarise(x) for x in args.compare)
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

    if not args.check:
        raise SystemExit("--check or --compare is required")

    s = summarise(args.check)
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
