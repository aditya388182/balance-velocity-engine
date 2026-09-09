#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from conf.config import CFG  # noqa: E402

GREEN, RED, DIM, RESET = "\033[92m", "\033[91m", "\033[2m", "\033[0m"


def main() -> None:
    p = argparse.ArgumentParser(description="Print JSON records from a Kafka topic")
    p.add_argument("--topic", required=True)
    p.add_argument("--max", type=int, default=50, help="max records to PRINT (all are counted)")
    p.add_argument("--timeout", type=float, default=8.0, help="seconds of silence before stopping")
    p.add_argument("--latest", dest="from_beginning", action="store_false", default=True)
    p.add_argument("--count-only", action="store_true")
    p.add_argument("--filter-kind", default=None, help="only print records whose kind matches")
    p.add_argument("--check-min-first", action="store_true",
                   help="assert DLQ evictions arrive in ascending seq order (min-first)")
    p.add_argument("--group", default=None)
    args = p.parse_args()

    consumer = Consumer({
        "bootstrap.servers": CFG["kafka_bootstrap"],
        "group.id": args.group or f"inspect-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest" if args.from_beginning else "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])

    total = printed = 0
    by_kind: dict[str, int] = {}
    seqs: list[int] = []
    idle = 0.0
    poll_s = 1.0

    try:
        while idle < args.timeout:
            msg = consumer.poll(poll_s)
            if msg is None:
                idle += poll_s
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    idle += poll_s
                    continue
                raise RuntimeError(msg.error())
            idle = 0.0
            total += 1
            try:
                rec = json.loads(msg.value().decode("utf-8"))
            except Exception:
                rec = {"_raw": repr(msg.value()[:200])}

            kind = rec.get("kind") or rec.get("reason") or "?"
            by_kind[kind] = by_kind.get(kind, 0) + 1
            if rec.get("seq_no") is not None:
                try:
                    seqs.append(int(rec["seq_no"]))
                except (TypeError, ValueError):
                    pass

            if args.filter_kind and kind != args.filter_kind:
                continue
            if not args.count_only and printed < args.max:
                key = msg.key().decode("utf-8") if msg.key() else None
                print(f"[p{msg.partition()}@{msg.offset()}] key={key} {json.dumps(rec)}")
                printed += 1
    finally:
        consumer.close()

    print("-" * 72)
    print(f"topic    : {args.topic}")
    print(f"consumed : {total} record(s)")
    for kind, n in sorted(by_kind.items()):
        print(f"  {kind:<20} {n}")
    if seqs:
        print(f"seq range: {min(seqs)}..{max(seqs)}  distinct={len(set(seqs))}")

    if args.check_min_first:
        ascending = all(seqs[i] <= seqs[i + 1] for i in range(len(seqs) - 1))
        if ascending:
            print(f"{GREEN}MIN-FIRST OK{RESET} — evictions arrived in ascending seq order")
        else:
            firstbad = next(i for i in range(len(seqs) - 1) if seqs[i] > seqs[i + 1])
            print(f"{RED}MIN-FIRST VIOLATED{RESET} at position {firstbad}: "
                  f"{seqs[firstbad]} then {seqs[firstbad + 1]}")
            sys.exit(1)


if __name__ == "__main__":
    main()
