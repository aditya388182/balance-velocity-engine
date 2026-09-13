#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from conf.config import CFG  # noqa: E402

GREEN, RED, YELLOW, BLUE, DIM, RESET = (
    "\033[92m", "\033[91m", "\033[93m", "\033[94m", "\033[2m", "\033[0m")
FINDINGS = []


def head(t):
    print(f"\n{BLUE}{t}{RESET}\n" + "-" * len(t))


def bad(msg, fix=""):
    FINDINGS.append(msg)
    print(f"  {RED}ISSUE{RESET} {msg}")
    if fix:
        print(f"        {DIM}{fix}{RESET}")


def warn(msg, fix=""):
    print(f"  {YELLOW}NOTE{RESET}  {msg}")
    if fix:
        print(f"        {DIM}{fix}{RESET}")


def good(msg):
    print(f"  {GREEN}OK{RESET}    {msg}")


def scan_engine_log():
    head("1. The engine log — did the velocity queries start, and did they die?")
    p = REPO_ROOT / "logs" / "engine.log"
    if not p.exists():
        bad("logs/engine.log is missing", "no record of what the engine did")
        return
    text = p.read_text(errors="replace")

    starts = re.findall(r"\[engine\] velocity\s*:\s*(\S+)", text)
    print(f"  velocity queries announced : {starts or 'NONE'}")
    if not starts:
        bad("the engine never announced a velocity query",
            "either the Day-5 balance_engine.py is not installed, or it failed "
            "before reaching start_velocity_queries — run scripts/check_install.py")

    # the shapes a dead streaming query leaves behind
    patterns = {
        "StreamingQueryException": "a query terminated with an exception",
        "ConcurrentAppendException": "two writers hit the same Delta table",
        "ConcurrentModificationException": "concurrent Delta metadata commit",
        "DeltaConcurrentModificationException": "concurrent Delta metadata commit",
        "ProtocolChangedException": "Delta table protocol changed under a writer",
        "MetadataChangedException": "Delta table metadata changed under a writer",
        "Query velocity_": "a named velocity query event",
        "Terminated with exception": "a query terminated abnormally",
    }
    hits = {k: text.count(k) for k, _ in patterns.items() if text.count(k)}
    if hits:
        for k, n in hits.items():
            print(f"  {RED}{k}{RESET} x{n}   {DIM}{patterns[k]}{RESET}")
        if any("Concurrent" in k or "Protocol" in k or "Metadata" in k for k in hits):
            bad("Delta concurrency exception(s) in the log",
                "two velocity queries writing to ONE table on the same trigger. "
                "The Day-5 fix gives each window kind its own path.")
    else:
        good("no Delta concurrency or query-termination exceptions found")

    tail_err = [l for l in text.splitlines()[-400:]
                if "ERROR" in l or "Exception" in l]
    if tail_err:
        print(f"  {DIM}last errors:{RESET}")
        for l in tail_err[-5:]:
            print(f"    {l[:160]}")


def scan_progress():
    head("2. Per-query progress — is velocity observable at all?")
    p = REPO_ROOT / "logs" / "progress.jsonl"
    if not p.exists() or not p.read_text().strip():
        bad("logs/progress.jsonl missing or empty")
        return
    rows = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
    names = Counter(r.get("query_name", "(unnamed)") for r in rows)
    print(f"  progress rows by query : {dict(names)}")
    if set(names) <= {"(unnamed)", "balance_engine"}:
        warn("only the sequencer has progress rows",
             "start_progress_writer was polling ONE query, so the velocity queries "
             "had no health signal. The Day-5 fix polls all of them — this is why "
             "the failure was hard to read.")
    else:
        for name in names:
            qrows = [r for r in rows if r.get("query_name") == name]
            total_in = sum(r.get("num_input_rows") or 0 for r in qrows)
            print(f"    {name:<20} batches {len(qrows):>4}  input rows {total_in:>8}"
                  f"  last batch_id {qrows[-1].get('batch_id')}")


def scan_velocity_table():
    head("3. The velocity table — what is actually in it")
    from pyspark.sql import functions as F

    from spark.utils.session import build_spark
    spark = build_spark(CFG, app_name="diagnose-velocity", streaming=False)
    try:
        base = CFG["paths"]["velocity"]
        frames = {}
        # Day 5 writes one path per kind; the earlier shape was a single shared path
        for kind in ("short", "long"):
            for path in (f"{base}/{kind}", base):
                try:
                    df = spark.read.format("delta").load(path)
                    if "window_kind" in df.columns:
                        df = df.filter(F.col("window_kind") == kind)
                    if df.take(1):
                        frames[kind] = (path, df)
                        break
                except Exception:
                    continue

        if not frames:
            bad("no velocity rows found at any path",
                f"tried {base}/short, {base}/long and {base}. The queries wrote "
                f"nothing at all — see section 1.")
            return

        for kind, (path, df) in frames.items():
            print(f"\n  --- {kind}  ({path}) ---")
            n = df.count()
            accounts = df.select("account_id").distinct().count()
            span = df.agg(F.min("window_start").alias("lo"),
                          F.max("window_start").alias("hi")).first()
            print(f"    rows {n}   accounts {accounts}")
            print(f"    window_start span {span['lo']} .. {span['hi']}")

            batches = sorted(r["batch_id"] for r in
                             df.select("batch_id").distinct().collect())
            print(f"    batch_ids: {len(batches)} distinct, "
                  f"{batches[:6]}{' ...' if len(batches) > 6 else ''} "
                  f"max {batches[-1] if batches else '-'}")

            # a restarted query re-uses batch ids from 0; the same id appearing with
            # two different wall times is the signature of two runs in one table
            if "written_at" in df.columns:
                per_batch = (df.groupBy("batch_id")
                             .agg(F.countDistinct(F.date_trunc("minute", "written_at"))
                                  .alias("distinct_minutes"))
                             .filter(F.col("distinct_minutes") > 1).count())
                if per_batch:
                    bad(f"{kind}: {per_batch} batch_id(s) written at more than one time",
                        "the table holds rows from MORE THAN ONE engine run. "
                        "`order by batch_id desc` then mixes runs. Reset the lake "
                        "between runs, or rely on written_at (the Day-5 fix).")
                else:
                    good(f"{kind}: batch_ids look like a single run")
            else:
                warn(f"{kind}: no written_at column — this table predates the Day-5 fix",
                     "cross-run contamination cannot be ruled out; reset_lake and re-run")

            if len(batches) > 1:
                gaps = [(a, b) for a, b in zip(batches, batches[1:]) if b - a > 1]
                if gaps:
                    warn(f"{kind}: gaps in batch_id: {gaps[:4]}",
                         "normal if some triggers produced no window updates")

            top = (df.groupBy("account_id")
                   .agg(F.sum("txn_count").alias("txns"))
                   .orderBy(F.col("txns").desc()).limit(3).collect())
            print(f"    txn_count summed per account (top 3): "
                  f"{[(r['account_id'], r['txns']) for r in top]}")
    finally:
        spark.stop()


def compare_against_source():
    head("4. How much of the stream did velocity actually see?")
    from pyspark.sql import functions as F

    from spark.utils.avro_deserializer import deserialize_stream
    from spark.utils.session import build_spark
    spark = build_spark(CFG, app_name="diagnose-velocity-src", streaming=True)
    try:
        raw = (spark.read.format("kafka")
               .option("kafka.bootstrap.servers", CFG["kafka_bootstrap"])
               .option("subscribe", CFG["topics"]["events"])
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest").load())
        ev = deserialize_stream(raw, CFG["schema_registry_url"])
        debits = ev.filter(F.col("event_type") == "DEBIT")
        n_debits = debits.count()
        frontier = ev.agg(F.max("event_ts")).first()[0]
        earliest = ev.agg(F.min("event_ts")).first()[0]
        print(f"  DEBIT events on the topic : {n_debits}")
        print(f"  event-time span           : {earliest} .. {frontier}")

        wm_ms = int(CFG["watermark_delay_ms"])
        print(f"  the stream's FINAL watermark is max(event_ts) - {wm_ms/1000:.0f}s")
        print(f"  {DIM}so a window is settled for the STREAM only when")
        print(f"  window_end <= max(event_ts) - watermark_delay, not <= max(event_ts).")
        print(f"  Using the looser bound marks ~one window's worth as settled that")
        print(f"  the stream has not finalised — a real flaw in the old check.{RESET}")
    finally:
        spark.stop()


def main():
    p = argparse.ArgumentParser(description="Velocity parity failure diagnosis")
    p.add_argument("--no-spark", action="store_true",
                   help="log and progress only, no Delta or Kafka reads")
    args = p.parse_args()

    print("Project 3 — velocity diagnosis")
    print("=" * 78)
    scan_engine_log()
    scan_progress()
    if not args.no_spark:
        scan_velocity_table()
        compare_against_source()

    print("\n" + "=" * 78)
    if FINDINGS:
        print(f"{RED}{len(FINDINGS)} issue(s){RESET}")
        for f in FINDINGS:
            print(f"  - {f}")
        sys.exit(1)
    print(f"{GREEN}No structural issue found in the velocity path.{RESET}")
    print(f"{DIM}If parity still fails, the remaining suspect is the settled-window")
    print(f"bound — re-run velocity_recompute.py, which now uses the watermark.{RESET}")
    sys.exit(0)


if __name__ == "__main__":
    main()
