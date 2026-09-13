#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"


def read_rows(path: Path, query: str):
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if r.get("query_name") not in (None, query):
            continue
        rows.append(r)
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description="Wait for a query to drain")
    p.add_argument("--query", default="balance_engine")
    p.add_argument("--progress-file", default=str(REPO_ROOT / "logs" / "progress.jsonl"))
    p.add_argument("--idle-batches", type=int, default=3,
                   help="consecutive zero-input batches that count as drained")
    p.add_argument("--timeout", type=float, default=300.0)
    p.add_argument("--min-batches", type=int, default=1,
                   help="refuse to call it drained before this many batches exist")
    args = p.parse_args()

    path = Path(args.progress_file)
    t0 = time.time()
    last_report = 0.0

    while time.time() - t0 < args.timeout:
        rows = read_rows(path, args.query)
        stopped = [r for r in rows if r.get("event") == "STOPPED"]
        if stopped:
            print(f"{RED}QUERY STOPPED{RESET} — {args.query} is no longer running")
            exc = stopped[-1].get("exception")
            if exc:
                print(f"  {exc[:400]}")
            sys.exit(1)

        batches = [r for r in rows if r.get("batch_id") is not None]
        if len(batches) >= args.min_batches:
            tail = batches[-args.idle_batches:]
            if (len(tail) == args.idle_batches
                    and all((r.get("num_input_rows") or 0) == 0 for r in tail)):
                elapsed = time.time() - t0
                print(f"{GREEN}DRAINED{RESET} — {args.query} committed "
                      f"{len(batches)} batch(es); the last {args.idle_batches} were "
                      f"empty ({elapsed:.0f}s)")
                sys.exit(0)

        now = time.time()
        if now - last_report > 20:
            last_report = now
            if batches:
                recent = sum((r.get("num_input_rows") or 0) for r in batches[-3:])
                dur = batches[-1].get("duration_ms")
                print(f"  {DIM}waiting: {len(batches)} batches, last 3 took in "
                      f"{recent} rows, last batch {dur} ms{RESET}")
            else:
                print(f"  {DIM}waiting: no batches committed yet{RESET}")
        time.sleep(2)

    rows = read_rows(path, args.query)
    batches = [r for r in rows if r.get("batch_id") is not None]
    print(f"{YELLOW}TIMEOUT{RESET} after {args.timeout:.0f}s — {args.query} committed "
          f"{len(batches)} batch(es) and is still taking in rows.")
    if batches:
        durs = [r.get("duration_ms") or 0 for r in batches[-5:]]
        print(f"  recent batch durations (ms): {durs}")
        print(f"  {DIM}If these are far above the trigger interval the engine is "
              f"backlogged: reduce the event rate or the account count, or raise "
              f"the timeout. A drill that runs while the engine is behind measures "
              f"the backlog, not the feature.{RESET}")
    sys.exit(1)


if __name__ == "__main__":
    main()
