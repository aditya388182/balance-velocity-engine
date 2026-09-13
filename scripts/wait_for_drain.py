#!/usr/bin/env python3
# scripts/wait_for_drain.py
"""Block until a streaming query has actually caught up. Replaces blind sleeps.

WHY SLEEPS WERE NOT ENOUGH
--------------------------
Every drill used `sleep N` to mean "the engine has finished". That holds only while
the engine keeps up. Stage 6's first run put 12,000 events and two windowed
aggregations on a local[4] driver, batches stretched to ~33 s against a 5 s trigger,
and a 45-second sleep covered barely one batch. A sleep asserts a duration; a drill
needs a condition.

AND THE FIRST CONDITION I CHOSE WAS UNSATISFIABLE
-------------------------------------------------
The first version waited for "N consecutive committed batches with zero input
rows". Structured Streaming does not COMMIT a batch when there is no new data and
nothing else to do — it skips the trigger entirely. lastProgress stays pinned to the
last real batch forever.

So the waiter sat for 300 seconds watching batch 7, whose duration was 7 ms, at the
end of a sequence that fell 13265 -> 11316 -> 9795 -> 3847 -> 7. That is a query
that CAUGHT UP. It then reported a timeout and blamed the engine.

    An empty batch is not a signal Spark reliably emits.
    The signal of a drained query is the ABSENCE of new batches.

THE THREE SIGNALS, IN ORDER OF AUTHORITY
----------------------------------------
1. CAUGHT UP   the last batch's source endOffset == latestOffset. The query's own
               statement that it read everything the source had. Authoritative.
2. IDLE        no new batch_id for long enough, scaled to the last batch's duration
               so a genuinely slow batch is not mistaken for silence.
3. EMPTY BATCH the last batch took in zero rows. Happens when a timeout fires with
               no data, so it is real but not something to rely on.

    python scripts/wait_for_drain.py --query balance_engine --timeout 300
"""
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
    p.add_argument("--timeout", type=float, default=300.0)
    p.add_argument("--min-batches", type=int, default=1,
                   help="refuse to call it drained before this many batches exist")
    p.add_argument("--stable-seconds", type=float, default=20.0,
                   help="silence that counts as idle. Scaled up automatically to 3x "
                        "the last batch's duration, so a slow batch in progress is "
                        "not mistaken for a finished query.")
    args = p.parse_args()

    path = Path(args.progress_file)
    t0 = time.time()
    last_report = 0.0
    last_batch_id = None
    last_change = time.time()

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
        now = time.time()
        if batches and batches[-1]["batch_id"] != last_batch_id:
            last_batch_id = batches[-1]["batch_id"]
            last_change = now

        if len(batches) >= args.min_batches:
            last = batches[-1]
            dur_s = (last.get("duration_ms") or 0) / 1000.0
            quiet_for = now - last_change
            required_quiet = max(args.stable_seconds, 3 * dur_s)

            # 1. the authoritative signal
            end_off = last.get("source_end_offset")
            latest_off = last.get("source_latest_offset")
            if end_off and latest_off and end_off not in ("null", "None"):
                if end_off == latest_off:
                    print(f"{GREEN}CAUGHT UP{RESET} — {args.query} consumed everything "
                          f"the source had as of batch {last['batch_id']} "
                          f"({now - t0:.0f}s)")
                    sys.exit(0)

            # 2. idle: no new batch for long enough, scaled to the last batch
            if quiet_for >= required_quiet:
                print(f"{GREEN}DRAINED{RESET} — {args.query} committed "
                      f"{len(batches)} batch(es) and has created none for "
                      f"{quiet_for:.0f}s (threshold {required_quiet:.0f}s). "
                      f"Spark skips a trigger when there is nothing to do, so "
                      f"silence is the signal.")
                sys.exit(0)

            # 3. an explicitly empty batch
            if (last.get("num_input_rows") or 0) == 0 and quiet_for > 2:
                print(f"{GREEN}DRAINED{RESET} — {args.query}'s last batch "
                      f"({last['batch_id']}) took in zero rows ({now - t0:.0f}s)")
                sys.exit(0)

        if now - last_report > 20:
            last_report = now
            if batches:
                last = batches[-1]
                print(f"  {DIM}waiting: batch {last['batch_id']}, "
                      f"{last.get('num_input_rows')} rows in, "
                      f"{last.get('duration_ms')} ms, quiet for "
                      f"{now - last_change:.0f}s{RESET}")
            else:
                print(f"  {DIM}waiting: no batches committed yet{RESET}")
        time.sleep(2)

    rows = read_rows(path, args.query)
    batches = [r for r in rows if r.get("batch_id") is not None]
    print(f"{YELLOW}TIMEOUT{RESET} after {args.timeout:.0f}s — {args.query} committed "
          f"{len(batches)} batch(es).")
    if batches:
        durs = [r.get("duration_ms") or 0 for r in batches[-5:]]
        ins = [r.get("num_input_rows") or 0 for r in batches[-5:]]
        print(f"  recent batch durations (ms): {durs}")
        print(f"  recent input rows          : {ins}")
        if durs and durs[-1] > 5000 and ins[-1] > 0:
            print(f"  {DIM}The engine is BACKLOGGED — batches far exceed the trigger "
                  f"interval and are still taking in rows. Reduce the event rate or "
                  f"the account count. A drill that runs while the engine is behind "
                  f"measures the backlog, not the feature.{RESET}")
        else:
            print(f"  {DIM}The engine looks IDLE — short batches, no new ones. If you "
                  f"see this, the offset signal is unavailable (an older "
                  f"progress.jsonl without source offsets) and --stable-seconds was "
                  f"never reached. Lower it, or re-run with the Day-5 progress "
                  f"writer installed.{RESET}")
    sys.exit(1)


if __name__ == "__main__":
    main()
