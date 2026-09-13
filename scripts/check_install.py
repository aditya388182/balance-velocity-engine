#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"

# path -> (marker that only the corrected version contains, what that correction was)
CHECKS = {
    "scripts/release_head_drill.sh": (
        "full parity BEFORE the release",
        "runs parity before the append, while the oracle can still model the run"),
    "scripts/parity_balance.py": (
        "never_applied",
        "applied-count unions the gap range with the DLQ set instead of summing them"),
    "scripts/parity_balance.py#2": (
        "--only-account",
        "can scope the comparison to one account"),
    "scripts/late_arrival_proof.py": (
        "THE VERDICT HELD",
        "asserts a post-gap arrival is dropped and the balance does not move"),
    "scripts/buffer_proof.py": (
        "TERMINAL state rather than on a transient reading",
        "asserts terminal balance/DLQ instead of a transient buffer_size"),
    "scripts/stage5_proof.sh": (
        "event-time-step-ms 2",
        "shortens the burst's event-time span so both runs reach terminal state"),
    "scripts/sink_replay_drill.sh": (
        "P3_SINK_FAIL_ONCE=1",
        "demonstrates exactly-once layer 3 deterministically"),
    "scripts/replay_evidence.py": (
        "UNDETECTABLE FOR THIS RUN",
        "distinguishes 'not observed' from 'impossible to observe'"),
    "scripts/gap_timing_probe.py": (
        "--after-crash",
        "skips the latency bound after a crash drill, and explains zero signals"),
    "scripts/diagnose.py": (
        "_coverage_check",
        "warns when the delivery log covers only a fragment of the run"),
    "scripts/diagnose.py#2": (
        "INDETERMINATE for this run",
        "stops reporting an indeterminate oracle as a balance mismatch"),
    "scripts/inject_burst.py": (
        "--append-to",
        "can extend an existing delivery log instead of starting a new one"),
    "scripts/recovery_drill.sh": (
        "waiting for the first committed batch",
        "waits for real work before the SIGKILL"),
    "scripts/rss_monitor.py": (
        "_descendants",
        "sums the process tree, since the state lives in the JVM child"),
    "scripts/state_growth.py": (
        "NOT THE STAGE-5 INSTRUMENT",
        "scope corrected — it counts accounts, which belongs to Day 5's TTL curve"),
    "scripts/chaos/kill_engine.sh": (
        "SIGKILL",
        "kills the driver without a graceful shutdown"),
    "spark/engine/sinks.py": (
        "P3_SINK_FAIL_ONCE",
        "one-shot sink failure knob for the layer-3 drill"),
    "spark/engine/sinks.py#2": (
        "buffer_size <> t.buffer_size",
        "observability columns update while an account is stalled"),
}


def main() -> None:
    print("Day-4 install check")
    print("=" * 78)
    ok, stale, missing = [], [], []

    for key, (marker, what) in CHECKS.items():
        rel = key.split("#")[0]
        path = REPO_ROOT / rel
        label = key if "#" in key else rel
        if not path.exists():
            missing.append((label, what))
            print(f"  {RED}MISSING{RESET}  {label}")
            continue
        if marker in path.read_text():
            ok.append(label)
            print(f"  {GREEN}OK     {RESET}  {label}")
        else:
            stale.append((label, what, marker))
            print(f"  {YELLOW}STALE  {RESET}  {label}")
            print(f"           {DIM}missing: {what}{RESET}")

    print("=" * 78)
    print(f"{GREEN}{len(ok)} current{RESET}   {YELLOW}{len(stale)} stale{RESET}   "
          f"{RED}{len(missing)} missing{RESET}")

    if stale or missing:
        print()
        print("A half-installed bundle fails in places that have nothing to do with the")
        print("engine. Install the complete bundle — the zip, not individual files:")
        print()
        print("    cd ~/balance-velocity-engine")
        print("    unzip -o ~/balance-velocity-engine-day4-releasehead2.zip")
        print("    chmod +x scripts/*.py scripts/*.sh scripts/chaos/*.sh")
        print("    python scripts/check_install.py")
        sys.exit(1)

    print()
    print(f"{GREEN}All Day-4 corrections are on disk.{RESET} Drills will measure the "
          f"engine rather than the install.")
    sys.exit(0)


if __name__ == "__main__":
    main()
