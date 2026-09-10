#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from conf.config import CFG  # noqa: E402

SCHEMA_FILE = REPO_ROOT / "schemas" / "account_event_v1.avsc"


def main() -> None:
    p = argparse.ArgumentParser(description="Out-of-order burst injector (Stage 5)")
    p.add_argument("--account", default="HOT-1")
    p.add_argument("--events", type=int, default=10_000)
    p.add_argument("--start-seq", type=int, default=2)
    p.add_argument("--never-send", type=int, action="append", default=[],
                   help="withhold this seq entirely (repeatable). Use 1 for pure buffer pressure.")
    p.add_argument("--amount", type=int, default=-1_000,
                   help="signed minor units per event; negative = DEBIT")
    p.add_argument("--shuffle", action="store_true", default=True,
                   help="publish in random order (default)")
    p.add_argument("--in-order", dest="shuffle", action="store_false")
    p.add_argument("--event-time-step-ms", type=int, default=10,
                   help="logical spacing between consecutive seqs in event time")
    p.add_argument("--seed", type=int, default=99)
    p.add_argument("--run-id", default=None)
    p.add_argument("--out-dir", default=str(REPO_ROOT))
    p.add_argument("--offline", action="store_true",
                   help="write the delivery log without Kafka (byte-reproducible)")
    p.add_argument("--append-to", default=None,
                   help="append to an existing delivery log instead of writing a new one")
    args = p.parse_args()

    rng = random.Random(args.seed)
    run_id = args.run_id or (f"burst{args.seed:04d}" if args.offline else uuid.uuid4().hex[:8])
    withheld = set(args.never_send)

    # Event time is assigned by LOGICAL position, before shuffling — the same rule
    # the generator follows, and for the same reason: publish order must not be
    # allowed to masquerade as event time, or nothing is ever late.
    base_ms = (int(CFG["generator"]["clock_start_ms"]) if args.offline
               else int(time.time() * 1000))
    plan: List[Dict[str, Any]] = []
    for i in range(args.events):
        seq = args.start_seq + i
        if seq in withheld:
            continue
        plan.append({
            "account_id": args.account,
            "seq_no": seq,
            "amount_minor": args.amount,
            "event_type": "DEBIT" if args.amount < 0 else "CREDIT",
            "event_ts_ms": base_ms + i * args.event_time_step_ms,
        })
    if args.shuffle:
        rng.shuffle(plan)

    producer = avro_ser = key_ser = ctx = None
    topic = CFG["topics"]["events"]
    errors: List[str] = []

    if not args.offline:
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import (MessageField, SerializationContext,
                                                   StringSerializer)
        sr = SchemaRegistryClient({"url": CFG["schema_registry_url"]})
        avro_ser = AvroSerializer(sr, SCHEMA_FILE.read_text(), lambda ev, _c: ev)
        key_ser = StringSerializer("utf_8")
        ctx = SerializationContext(topic, MessageField.VALUE)
        producer = Producer({
            "bootstrap.servers": CFG["kafka_bootstrap"],
            "linger.ms": 20, "compression.type": "zstd",
            "acks": "all", "enable.idempotence": True,
            "queue.buffering.max.messages": 200_000,
        })

    if args.append_to:
        log_path = Path(args.append_to)
        mode = "a"
        existing = sum(1 for line in log_path.read_text().splitlines() if line.strip())
    else:
        log_path = Path(args.out_dir) / f"delivery_log_{run_id}.jsonl"
        mode = "w"
        existing = 0

    print(f"account   : {args.account}")
    print(f"mode      : {'offline' if args.offline else 'kafka'}")
    print(f"planned   : {len(plan)} events, seqs {args.start_seq}..{args.start_seq + args.events - 1}")
    print(f"withheld  : {sorted(withheld) or 'none'}")
    print(f"log       : {log_path} ({'append' if mode == 'a' else 'new'})")

    cap = int(CFG["max_buffer_size"])
    seqs = sorted(e["seq_no"] for e in plan)
    if withheld and min(withheld) < min(seqs):
        keep = seqs[-cap:] if len(seqs) > cap else seqs
        evict = seqs[:-cap] if len(seqs) > cap else []
        print(f"predicted : buffer pins at {len(keep)} holding "
              f"{keep[0]}..{keep[-1]}; {len(evict)} evictions to the DLQ")

    published = 0
    t0 = time.time()
    with open(log_path, mode) as log:
        for i, e in enumerate(plan):
            if not args.offline:
                producer.produce(
                    topic=topic, key=key_ser(e["account_id"]),
                    value=avro_ser({
                        "account_id": e["account_id"], "seq_no": int(e["seq_no"]),
                        "amount_minor": int(e["amount_minor"]),
                        "event_type": e["event_type"], "event_ts": int(e["event_ts_ms"]),
                    }, ctx),
                    on_delivery=lambda err, _m: errors.append(str(err)) if err else None,
                )
                if i % 500 == 0:
                    producer.poll(0)
            log.write(json.dumps({
                "run_id": run_id, "publish_order": existing + i,
                "account_id": e["account_id"], "seq_no": int(e["seq_no"]),
                "amount_minor": int(e["amount_minor"]),
                "event_type": e["event_type"], "event_ts_ms": int(e["event_ts_ms"]),
            }, sort_keys=True) + "\n")
            published += 1

    if not args.offline:
        producer.flush(60)
        if errors:
            print(f"DELIVERY ERRORS: {len(errors)} (first: {errors[0]})", file=sys.stderr)
            sys.exit(1)

    print(f"published {published} events in {time.time() - t0:.1f}s")
    print(f"DELIVERY_LOG={log_path}")


if __name__ == "__main__":
    main()
