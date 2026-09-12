#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import uuid
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from conf.config import CFG  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"


def dlq_count(timeout: float = 8.0):
    from confluent_kafka import Consumer, KafkaError
    c = Consumer({"bootstrap.servers": CFG["kafka_bootstrap"],
                  "group.id": f"bufproof-{uuid.uuid4().hex[:8]}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe([CFG["topics"]["dlq"]])
    seqs, total, idle = [], 0, 0.0
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
                if rec.get("seq_no") is not None:
                    seqs.append(int(rec["seq_no"]))
            except Exception:
                pass
    finally:
        c.close()
    return total, seqs


def main() -> None:
    p = argparse.ArgumentParser(description="Stage 5 buffer + DLQ proof")
    p.add_argument("--account", default="HOT-1")
    p.add_argument("--expect-buffer", type=int, default=None)
    p.add_argument("--expect-dlq", type=int, default=None)
    p.add_argument("--uncapped", action="store_true",
                   help="the contrast run: expect NO evictions and a buffer above the cap")
    p.add_argument("--save", default=None, help="write the measurements to JSON")
    args = p.parse_args()

    from spark.utils.session import build_spark
    spark = build_spark(CFG, app_name="buffer-proof", streaming=False)
    try:
        rows = (spark.read.format("delta").load(CFG["paths"]["balances"])
                .select("account_id", "last_applied_seq", "balance_minor", "buffer_size")
                .collect())
    finally:
        spark.stop()

    row = next((r for r in rows if r["account_id"] == args.account), None)
    if row is None:
        print(f"{RED}account {args.account} is not in the balances table{RESET}")
        print(f"  present: {sorted(r['account_id'] for r in rows)}")
        sys.exit(1)

    buf = int(row["buffer_size"])
    cap = int(CFG["max_buffer_size"])
    total, seqs = dlq_count()

    print(f"account          : {args.account}")
    print(f"configured cap   : {cap}")
    print(f"buffer_size      : {buf}")
    print(f"last_applied_seq : {row['last_applied_seq']}")
    print(f"DLQ records      : {total}"
          + (f"   seqs {min(seqs)}..{max(seqs)} distinct={len(set(seqs))}" if seqs else ""))
    print("=" * 78)

    failures = []

    if args.uncapped:
        if buf > cap:
            print(f"{GREEN}UNCAPPED CONFIRMED{RESET} — the buffer grew to {buf}, "
                  f"well past the {cap} the capped run held")
        else:
            failures.append(f"buffer_size {buf} did not exceed {cap} — was "
                            f"P3_MAX_BUFFER_SIZE echoed by [config] at startup?")
        if total == 0:
            print(f"{GREEN}NO EVICTIONS{RESET} — nothing was shed, which is exactly "
                  f"the failure mode the cap prevents")
        else:
            failures.append(f"{total} DLQ records on an uncapped run — the override "
                            f"did not take effect")
    else:
        if args.expect_buffer is not None and buf != args.expect_buffer:
            failures.append(f"buffer_size {buf} != expected {args.expect_buffer}")
        elif buf == cap:
            print(f"{GREEN}BUFFER PINNED AT THE CAP{RESET} — {buf} of a possible {cap}")
        elif buf < cap:
            print(f"{YELLOW}buffer below the cap{RESET} — {buf} of {cap}. Either the "
                  f"burst has not finished, or the head arrived and it drained.")

        if args.expect_dlq is not None and total != args.expect_dlq:
            failures.append(f"DLQ count {total} != expected {args.expect_dlq}")
        elif total > 0:
            ascending = seqs == sorted(seqs)
            print(f"{GREEN}EVICTIONS OBSERVED{RESET} — {total} records"
                  + (f", arrival-ordered (ascending={ascending})" if seqs else ""))
            print(f"  {DIM}min-first is a claim about WHICH events are evicted, not the "
                  f"order they leave in; with shuffled arrival the DLQ is not ascending"
                  f"{RESET}")

    if args.save:
        Path(args.save).write_text(json.dumps({
            "account": args.account, "cap": cap, "buffer_size": buf,
            "last_applied_seq": int(row["last_applied_seq"]),
            "dlq_records": total,
            "dlq_seq_lo": min(seqs) if seqs else None,
            "dlq_seq_hi": max(seqs) if seqs else None,
        }, indent=2))
        print(f"saved -> {args.save}")

    print("=" * 78)
    if failures:
        print(f"{RED}BUFFER PROOF FAIL{RESET}")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print(f"{GREEN}BUFFER PROOF PASS{RESET}")
    sys.exit(0)


if __name__ == "__main__":
    main()
