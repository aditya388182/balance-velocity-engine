#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Tuple

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"


def _one_rss_kb(pid: int) -> int | None:
    statm = Path(f"/proc/{pid}/statm")
    if statm.exists():
        try:
            return int(statm.read_text().split()[1]) * 4      # 4 KiB pages
        except Exception:
            return None
    try:
        out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)],
                             capture_output=True, text=True, timeout=5)
        val = out.stdout.strip()
        return int(val) if val else None
    except Exception:
        return None


def _descendants(pid: int) -> list[int]:
    """Every child, grandchild and so on of pid."""
    out, frontier = [], [pid]
    seen = {pid}
    while frontier:
        cur = frontier.pop()
        try:
            r = subprocess.run(["pgrep", "-P", str(cur)],
                               capture_output=True, text=True, timeout=5)
            kids = [int(x) for x in r.stdout.split() if x.strip().isdigit()]
        except Exception:
            kids = []
        for k in kids:
            if k not in seen:
                seen.add(k)
                out.append(k)
                frontier.append(k)
    return out


def rss_kb(pid: int, tree: bool = True) -> int | None:
    base = _one_rss_kb(pid)
    if base is None:
        return None
    if not tree:
        return base
    total = base
    for kid in _descendants(pid):
        v = _one_rss_kb(kid)
        if v:
            total += v
    return total


def sample(pid: int, out_path: str, interval: float, duration: float | None,
           tree: bool = True) -> None:
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    n = 0
    with open(out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["elapsed_s", "wall_ms", "rss_kb", "rss_mb"])
        while True:
            if duration is not None and (time.time() - t0) > duration:
                break
            kb = rss_kb(pid, tree=tree)
            if kb is None:
                print(f"process {pid} is gone after {n} samples", file=sys.stderr)
                break
            w.writerow([round(time.time() - t0, 2), int(time.time() * 1000),
                        kb, round(kb / 1024, 1)])
            fh.flush()
            n += 1
            time.sleep(interval)
    print(f"wrote {n} samples -> {out}")


def load(path: str) -> List[Tuple[float, float]]:
    rows = []
    with open(path) as fh:
        for r in csv.DictReader(fh):
            rows.append((float(r["elapsed_s"]), float(r["rss_mb"])))
    return rows


def summarise(path: str):
    rows = load(path)
    if not rows:
        raise SystemExit(f"{path} has no samples")
    mbs = [m for _t, m in rows]
    first_q = mbs[:max(1, len(mbs) // 4)]
    last_q = mbs[-max(1, len(mbs) // 4):]
    baseline = sum(first_q) / len(first_q)
    ending = sum(last_q) / len(last_q)
    return {
        "samples": len(rows),
        "duration_s": rows[-1][0],
        "min_mb": min(mbs), "max_mb": max(mbs),
        "baseline_mb": baseline, "ending_mb": ending,
        "growth_mb": ending - baseline,
        "peak_over_baseline_mb": max(mbs) - baseline,
    }


def main() -> None:
    p = argparse.ArgumentParser(description="RSS sampler and flatness check")
    p.add_argument("--pid", type=int)
    p.add_argument("--out", default="logs/rss.csv")
    p.add_argument("--interval", type=float, default=5.0)
    p.add_argument("--no-tree", dest="tree", action="store_false", default=True,
                   help="sample only the given pid instead of the whole process tree "
                        "(the JVM child is where the state store actually lives)")
    p.add_argument("--duration", type=float, default=None)
    p.add_argument("--check", help="assert this CSV stayed flat")
    p.add_argument("--max-growth-mb", type=float, default=250.0)
    p.add_argument("--compare", nargs=2, metavar=("CAPPED", "UNCAPPED"))
    args = p.parse_args()

    if args.compare:
        print(f"{YELLOW}note{RESET} RSS is the WEAK instrument for this contrast. A JVM "
              f"with -Xmx reserves its heap\n     up front, so a few thousand buffered "
              f"rows may not move it at all. Use\n     scripts/state_growth.py for the "
              f"state store's own reported size.\n")
        a, b = (summarise(x) for x in args.compare)
        print(f"{'':<22}{'capped':>14}{'uncapped':>14}")
        for k in ("samples", "duration_s", "baseline_mb", "max_mb",
                  "ending_mb", "growth_mb", "peak_over_baseline_mb"):
            print(f"{k:<22}{a[k]:>14.1f}{b[k]:>14.1f}")
        print()
        if b["growth_mb"] > a["growth_mb"]:
            print(f"{GREEN}CONTRAST HOLDS{RESET} — the cap kept growth at "
                  f"{a['growth_mb']:.0f} MB where the uncapped run grew "
                  f"{b['growth_mb']:.0f} MB")
            print(f"{DIM}Those two curves are the Interview-Q4 answer in two pictures.{RESET}")
            sys.exit(0)
        print(f"{RED}CONTRAST DID NOT HOLD{RESET} — the uncapped run did not grow more. "
              f"Did it actually run long enough, and was the cap override in effect?")
        sys.exit(1)

    if args.check:
        s = summarise(args.check)
        print(f"file      : {args.check}")
        print(f"samples   : {s['samples']} over {s['duration_s']:.0f}s")
        print(f"baseline  : {s['baseline_mb']:.0f} MB   peak: {s['max_mb']:.0f} MB   "
              f"ending: {s['ending_mb']:.0f} MB")
        print(f"growth    : {s['growth_mb']:+.0f} MB   (allowed: {args.max_growth_mb:.0f} MB)")
        if s["samples"] < 5:
            print(f"{YELLOW}TOO FEW SAMPLES{RESET} — a flat line through 3 points is not evidence")
            sys.exit(1)
        if s["growth_mb"] <= args.max_growth_mb:
            print(f"{GREEN}MEMORY FLAT{RESET} — the hostile account degraded itself, "
                  f"not the executor")
            sys.exit(0)
        print(f"{RED}MEMORY CLIMBED{RESET} — {s['growth_mb']:.0f} MB of growth. "
              f"Is max_buffer_size actually in effect?")
        sys.exit(1)

    if not args.pid:
        raise SystemExit("--pid is required unless --check or --compare is given")
    sample(args.pid, args.out, args.interval, args.duration, tree=args.tree)


if __name__ == "__main__":
    main()
