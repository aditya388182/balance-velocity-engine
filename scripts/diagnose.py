#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
import sys
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from conf.config import CFG  # noqa: E402
from scripts.oracle import compute_oracle, read_delivery_log  # noqa: E402

GREEN, RED, YELLOW, BLUE, DIM, RESET = (
    "\033[92m", "\033[91m", "\033[93m", "\033[94m", "\033[2m", "\033[0m")

FINDINGS: List[str] = []


def head(title: str) -> None:
    print(f"\n{BLUE}{title}{RESET}\n" + "-" * len(title))


def note(msg: str) -> None:
    print(f"  {msg}")


def good(msg: str) -> None:
    print(f"  {GREEN}OK{RESET}    {msg}")


def bad(msg: str, fix: str = "") -> None:
    FINDINGS.append(msg + (f"  -> {fix}" if fix else ""))
    print(f"  {RED}ISSUE{RESET} {msg}")
    if fix:
        print(f"        {DIM}{fix}{RESET}")


def warn(msg: str, fix: str = "") -> None:
    print(f"  {YELLOW}NOTE{RESET}  {msg}")
    if fix:
        print(f"        {DIM}{fix}{RESET}")



def latest_log() -> str | None:
    logs = sorted(glob.glob(str(REPO_ROOT / "delivery_log_*.jsonl")),
                  key=lambda p: Path(p).stat().st_mtime)
    return logs[-1] if logs else None


def section_delivery(log_path: str | None) -> Dict[str, Any]:
    head("1. Delivery log — what was published, and what is therefore expected")
    if not log_path:
        bad("no delivery_log_*.jsonl found",
            "every assertion downstream compares against this; run the generator first")
        return {}
    rows = read_delivery_log(log_path)
    note(f"file            : {log_path}")
    note(f"published       : {len(rows)} events")
    accounts = Counter(r["account_id"] for r in rows)
    note(f"accounts        : {len(accounts)}  {dict(list(accounts.items())[:6])}"
         + (" ..." if len(accounts) > 6 else ""))

    oracle = compute_oracle(rows)
    total_gaps = sum(len(o["expected_gap_ranges"]) for o in oracle.values())
    total_dups = sum(len(o["expected_dup_dropped"]) for o in oracle.values())
    total_ovf = sum(len(o["expected_overflow_evicted"]) for o in oracle.values())
    note(f"oracle expects  : {total_gaps} gap range(s), {total_dups} dup seq(s), "
         f"{total_ovf} overflow eviction(s)")

    if total_gaps == 0:
        warn("this stream has NO withheld sequence, so NO SEQUENCE_GAP can fire",
             "asserting --expect-gaps on this run will always fail; use a --gap stream")
    if total_dups == 0 and total_ovf == 0 and total_gaps == 0:
        warn("this stream produces NO integrity events of any kind",
             "sink-replay detection is impossible here — add --dup to the generator")
    return {"rows": rows, "oracle": oracle, "path": log_path,
            "expect_gaps": total_gaps, "expect_dups": total_dups,
            "expect_ovf": total_ovf}


def count_topic(topic: str, timeout: float = 6.0) -> Dict[str, int]:
    from confluent_kafka import Consumer, KafkaError
    c = Consumer({"bootstrap.servers": CFG["kafka_bootstrap"],
                  "group.id": f"diag-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([topic])
    counts: Counter = Counter()
    total = 0
    idle = 0.0
    try:
        while idle < timeout:
            m = c.poll(1.0)
            if m is None:
                idle += 1.0
                continue
            if m.error():
                if m.error().code() == KafkaError._PARTITION_EOF:
                    idle += 1.0
                    continue
                raise RuntimeError(m.error())
            idle = 0.0
            total += 1
            try:
                rec = json.loads(m.value().decode("utf-8"))
                counts[rec.get("kind") or rec.get("reason") or "?"] += 1
            except Exception:
                counts["<binary>"] += 1
    finally:
        c.close()
    counts["__total__"] = total
    return dict(counts)


def section_kafka(dl: Dict[str, Any]) -> Dict[str, Any]:
    head("2. Kafka topics — what actually landed")
    out = {}
    for key in ("events", "signals", "dlq", "integrity"):
        topic = CFG["topics"][key]
        try:
            c = count_topic(topic)
        except Exception as exc:
            bad(f"could not read {topic}: {exc}", "is the stack up? docker compose ps")
            continue
        out[key] = c
        detail = {k: v for k, v in c.items() if k != "__total__"}
        note(f"{topic:<22} {c['__total__']:>7} record(s)  {detail if detail else ''}")

    if dl and out.get("events", {}).get("__total__", 0) == 0 and dl.get("rows"):
        bad("accounts.events is EMPTY but a delivery log exists",
            "the topics were reset AFTER the run (reset_lake.sh deletes them) — "
            "any topic-based assertion now measures a wiped stack, not the run")
    if dl and dl.get("expect_gaps", 0) > 0 and out.get("signals", {}).get("__total__", 0) == 0:
        bad("the delivery log withholds a sequence but accounts.signals is EMPTY",
            "either the gap never fired (watermark frozen? no --heartbeat-account?) "
            "or the topic was reset after the run")
    return out


def section_progress() -> Dict[str, Any]:
    head("3. logs/progress.jsonl — batches, watermark, state")
    p = REPO_ROOT / "logs" / "progress.jsonl"
    if not p.exists() or not p.read_text().strip():
        bad("progress.jsonl is missing or empty",
            "the engine never committed a batch, or the progress writer did not start; "
            "check for '[engine] progress' in logs/engine.log")
        return {}
    rows = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
    rows = [r for r in rows if r.get("batch_id") is not None]
    note(f"batches         : {len(rows)}  (ids {rows[0]['batch_id']}..{rows[-1]['batch_id']})")
    ids = [r["batch_id"] for r in rows]
    repeats = [b for b, n in Counter(ids).items() if n > 1]
    if repeats:
        good(f"batch id(s) {sorted(repeats)} appear more than once — a batch RE-EXECUTED")
    srows = [r.get("state_rows") for r in rows if r.get("state_rows") is not None]
    sbytes = [r.get("state_bytes") for r in rows if r.get("state_bytes") is not None]
    if srows:
        note(f"state rows      : peak {max(srows)}  final {srows[-1]}   "
             f"{DIM}(= number of ACCOUNTS in state, not buffered events){RESET}")
    if sbytes:
        note(f"state bytes     : peak {max(sbytes)/1024/1024:.1f} MB   "
             f"{DIM}(RocksDB overhead dominated; not a payload measure){RESET}")
    wms = [r.get("watermark_ms") for r in rows if r.get("watermark_ms")]
    if wms:
        note(f"watermark       : advanced {(max(wms)-min(wms))/1000:.0f}s across the run")
    else:
        warn("no watermark reported in any batch",
             "event time never advanced — no timeout can fire without it")
    if len(rows) < 4:
        warn(f"only {len(rows)} batch(es) committed",
             "the engine had very little time to work; raise --duration or --drain")
    return {"rows": rows}


def section_delta(dl: Dict[str, Any]) -> Dict[str, Any]:
    head("4. Delta tables — balances and the integrity trail")
    from spark.utils.session import build_spark
    spark = build_spark(CFG, app_name="diagnose", streaming=False)
    try:
        try:
            bal = (spark.read.format("delta").load(CFG["paths"]["balances"])
                   .select("account_id", "last_applied_seq", "balance_minor",
                           "buffer_size").collect())
        except Exception:
            bal = []
        try:
            integ = (spark.read.format("delta").load(CFG["paths"]["integrity"])
                     .select("account_id", "kind", "seq_no", "batch_id").collect())
        except Exception:
            integ = []
    finally:
        spark.stop()

    if not bal:
        bad("the balances table is empty or missing",
            "no batch ever wrote a BALANCE row; check logs/engine.log for a failing batch")
        return {}

    note(f"balances rows   : {len(bal)}")
    print(f"\n  {'account':<14}{'last_seq':>10}{'balance':>16}{'buffer_size':>13}")
    for r in sorted(bal, key=lambda r: r["account_id"])[:12]:
        flag = ""
        if r["buffer_size"] and r["buffer_size"] >= int(CFG["max_buffer_size"]):
            flag = f"  <- pinned at the cap ({CFG['max_buffer_size']})"
        print(f"  {r['account_id']:<14}{r['last_applied_seq']:>10}"
              f"{r['balance_minor']:>16}{r['buffer_size']:>13}{flag}")
    if len(bal) > 12:
        print(f"  ... {len(bal)-12} more")

    dupe_accounts = {a: n for a, n in Counter(r["account_id"] for r in bal).items() if n > 1}
    if dupe_accounts:
        bad(f"more than one balances row per account: {dupe_accounts}",
            "the MERGE key is wrong, or two engines wrote concurrently")
    else:
        good("exactly one balances row per account")

    print()
    by_kind = Counter(r["kind"] for r in integ)
    note(f"integrity rows  : {len(integ)}  {dict(by_kind) if by_kind else '(none)'}")
    exact = Counter((r["account_id"], r["kind"], r["seq_no"], r["batch_id"]) for r in integ)
    replayed = {k: n for k, n in exact.items() if n > 1}
    if replayed:
        good(f"{len(replayed)} (account, kind, seq, batch_id) tuple(s) written MORE THAN "
             f"ONCE — the foreachBatch body re-ran and the guards absorbed it")
    elif integ:
        warn("no duplicated (kind, seq, batch_id) tuples — no sink replay in this run",
             "the SIGKILL window for that is milliseconds wide; use "
             "scripts/sink_replay_drill.sh to induce it deterministically")

    if dl:
        for acct, o in sorted(dl["oracle"].items()):
            row = next((r for r in bal if r["account_id"] == acct), None)
            if row is None:
                bad(f"{acct}: in the delivery log but NOT in the balances table",
                    "the engine never saw it — check the topic and the key")
                continue
            if row["balance_minor"] != o["expected_balance_minor"]:
                bad(f"{acct}: balance {row['balance_minor']} != oracle "
                    f"{o['expected_balance_minor']} "
                    f"(diff {row['balance_minor'] - o['expected_balance_minor']:+d})",
                    "negative = the engine is behind (not drained?); "
                    "positive = a double-apply")
    return {"balances": bal, "integrity": integ}


def main() -> None:
    p = argparse.ArgumentParser(description="Full pipeline diagnostic snapshot")
    p.add_argument("--delivery-log", default=None)
    p.add_argument("--no-kafka", action="store_true")
    p.add_argument("--no-delta", action="store_true")
    args = p.parse_args()

    print("Project 3 — pipeline diagnostic")
    print("=" * 74)

    dl = section_delivery(args.delivery_log or latest_log())
    if not args.no_kafka:
        section_kafka(dl)
    section_progress()
    if not args.no_delta:
        section_delta(dl)

    print("\n" + "=" * 74)
    if FINDINGS:
        print(f"{RED}{len(FINDINGS)} issue(s) found{RESET}")
        for f in FINDINGS:
            print(f"  - {f}")
        sys.exit(1)
    print(f"{GREEN}No structural issues found{RESET} — the pipeline is internally "
          f"consistent with the delivery log.")
    sys.exit(0)


if __name__ == "__main__":
    main()
