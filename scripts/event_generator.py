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

# Signed minor units: CREDIT positive, DEBIT negative.
CREDIT_RANGE = (5_000, 250_000)      # 50.00 .. 2500.00
DEBIT_RANGE = (1_000, 90_000)        # 10.00 ..  900.00
CREDIT_PROBABILITY = 0.42
HEARTBEAT_ACCOUNT = "HB-0000"


def build_plan(accounts: int, prefix: str, per_account: int, rng: random.Random
               ) -> List[Dict[str, Any]]:
    """Per-account logical event lists: seq 1..per_account, signed amounts."""
    plan: List[Dict[str, Any]] = []
    for a in range(accounts):
        account_id = f"{prefix}{a + 1:04d}"
        for seq in range(1, per_account + 1):
            if rng.random() < CREDIT_PROBABILITY:
                amount, etype = rng.randint(*CREDIT_RANGE), "CREDIT"
            else:
                amount, etype = -rng.randint(*DEBIT_RANGE), "DEBIT"
            plan.append({"account_id": account_id, "seq_no": seq,
                         "amount_minor": amount, "event_type": etype})
    return plan


def interleave_round_robin(plan: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Interleave accounts so they all progress together, as real traffic would."""
    by_acct: Dict[str, List[Dict[str, Any]]] = {}
    for e in plan:
        by_acct.setdefault(e["account_id"], []).append(e)
    out: List[Dict[str, Any]] = []
    idx = 0
    while True:
        progressed = False
        for acct in sorted(by_acct):
            evs = by_acct[acct]
            if idx < len(evs):
                out.append(evs[idx])
                progressed = True
        if not progressed:
            return out
        idx += 1


def stamp_logical_event_time(stream: List[Dict[str, Any]], base_ms: int, step_ms: int
                             ) -> List[Dict[str, Any]]:
    """Assign event_ts from LOGICAL position, before any delivery mangling."""
    for i, e in enumerate(stream):
        e["event_ts_ms"] = int(base_ms + i * step_ms)
    return stream


def apply_gaps_and_dups(stream: List[Dict[str, Any]], gaps: List[int], dups: List[int]
                        ) -> List[Dict[str, Any]]:
    """--gap removes an event from delivery entirely; --dup delivers it twice."""
    gap_set, dup_set = set(gaps), set(dups)
    out: List[Dict[str, Any]] = []
    for e in stream:
        if e["seq_no"] in gap_set:
            continue                       # never published — this is the hole
        out.append(e)
        if e["seq_no"] in dup_set:
            out.append(dict(e))            # same event_ts: same event, delivered twice
    return out


def shuffle_windows(stream: List[Dict[str, Any]], window: int, rng: random.Random
                    ) -> List[Dict[str, Any]]:
    """Permute publish order within fixed windows. Out-of-order, but bounded."""
    if not window or window <= 1:
        return stream
    out: List[Dict[str, Any]] = []
    for i in range(0, len(stream), window):
        chunk = stream[i:i + window]
        rng.shuffle(chunk)
        out.extend(chunk)
    return out


def apply_burst(stream: List[Dict[str, Any]], k: int) -> List[Dict[str, Any]]:
    """Move K late-sequence events for the first account to the front: pure early arrivals."""
    if not k:
        return stream
    first_account = stream[0]["account_id"]
    idxs = [i for i, e in enumerate(stream) if e["account_id"] == first_account][-k:]
    moved = {i for i in idxs}
    return [stream[i] for i in idxs] + [e for i, e in enumerate(stream) if i not in moved]


def main() -> None:
    p = argparse.ArgumentParser(description="AccountEvent generator + delivery log writer")
    p.add_argument("--accounts", type=int, default=1)
    p.add_argument("--account-prefix", default="ACCT-")
    p.add_argument("--rate", type=float, default=20.0, help="events/second (publish pacing)")
    p.add_argument("--duration", type=float, default=60.0, help="seconds of traffic")
    p.add_argument("--events-per-account", type=int, default=None,
                   help="override rate*duration/accounts")
    p.add_argument("--ordered", action="store_true", help="strict seq order (disables shuffle)")
    p.add_argument("--shuffle-window", type=int, default=0)
    p.add_argument("--gap", type=int, action="append", default=[])
    p.add_argument("--dup", type=int, action="append", default=[])
    p.add_argument("--burst", type=int, default=0)
    p.add_argument("--heartbeat-account", action="store_true")
    p.add_argument("--heartbeat-interval", type=float, default=1.0)
    p.add_argument("--trailing-heartbeats", type=int, default=45,
                   help="ticks after the main stream, to push the watermark past the tail")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--run-id", default=None)
    p.add_argument("--out-dir", default=str(REPO_ROOT))
    p.add_argument("--offline", action="store_true",
                   help="build the delivery log without Kafka (synthetic clock, byte-reproducible)")
    args = p.parse_args()

    rng = random.Random(args.seed)
    run_id = args.run_id or (f"offline{args.seed:04d}" if args.offline else uuid.uuid4().hex[:8])

    total = max(1, int(args.rate * args.duration))
    per_account = args.events_per_account or max(1, total // max(1, args.accounts))
    step_ms = max(1, int(round(1000.0 / args.rate))) if args.rate > 0 else 1

    stream = build_plan(args.accounts, args.account_prefix, per_account, rng)
    stream = interleave_round_robin(stream)

    base_ms = (int(CFG["generator"]["clock_start_ms"]) if args.offline
               else int(time.time() * 1000))
    stream = stamp_logical_event_time(stream, base_ms, step_ms)

    # Delivery mangling happens AFTER event times are fixed.
    stream = apply_gaps_and_dups(stream, args.gap, args.dup)
    stream = apply_burst(stream, args.burst)
    if args.shuffle_window and not args.ordered:
        stream = shuffle_windows(stream, args.shuffle_window, rng)

    log_path = Path(args.out_dir) / f"delivery_log_{run_id}.jsonl"

    producer = None
    avro_ser = None
    key_ser = None
    ctx = None
    topic = CFG["topics"]["events"]
    errors: List[str] = []

    if not args.offline:
        # Imported lazily so --offline works with neither confluent-kafka nor a broker.
        from confluent_kafka import Producer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import (MessageField, SerializationContext,
                                                   StringSerializer)

        sr = SchemaRegistryClient({"url": CFG["schema_registry_url"]})
        avro_ser = AvroSerializer(sr, SCHEMA_FILE.read_text(), lambda ev, _ctx: ev)
        key_ser = StringSerializer("utf_8")
        ctx = SerializationContext(topic, MessageField.VALUE)
        producer = Producer({
            "bootstrap.servers": CFG["kafka_bootstrap"],
            "linger.ms": 20,
            "compression.type": "zstd",
            "acks": "all",
            "enable.idempotence": True,
        })

        def _on_delivery(err, _msg):
            if err is not None:
                errors.append(str(err))
    else:
        _on_delivery = None  # type: ignore[assignment]

    print(f"run_id={run_id}  mode={'offline' if args.offline else 'kafka'}")
    print(f"delivery log -> {log_path}")
    print(f"planned publishes: {len(stream)} across {args.accounts} account(s)"
          f"{' + heartbeat' if args.heartbeat_account else ''}")

    publish_order = 0
    published = 0
    hb_seq = 0
    t0 = time.time()
    interval = (1.0 / args.rate) if (args.rate > 0 and not args.offline) else 0.0
    next_hb = t0

    with open(log_path, "w") as log:

        def publish(account_id: str, seq_no: int, amount_minor: int,
                    event_type: str, event_ts_ms: int) -> None:
            nonlocal publish_order, published
            if not args.offline:
                value = {
                    "account_id": account_id,
                    "seq_no": int(seq_no),
                    "amount_minor": int(amount_minor),
                    "event_type": event_type,
                    # fastavro passes plain ints straight through for timestamp-millis,
                    # so the wire bytes and the delivery log carry the identical number.
                    "event_ts": int(event_ts_ms),
                }
                # key = account_id. Without the key Kafka round-robins and one
                # account's events land on three partitions in arbitrary order:
                # disorder you did not ask for and cannot reason about.
                producer.produce(topic=topic, key=key_ser(account_id),
                                 value=avro_ser(value, ctx), on_delivery=_on_delivery)
                if published % 200 == 0:
                    producer.poll(0)
            log.write(json.dumps({
                "run_id": run_id,
                "publish_order": publish_order,
                "account_id": account_id,
                "seq_no": int(seq_no),
                "amount_minor": int(amount_minor),
                "event_type": event_type,
                "event_ts_ms": int(event_ts_ms),
            }, sort_keys=True) + "\n")
            publish_order += 1
            published += 1

        def heartbeat_ts(main_published: int, tick: int) -> int:
            """Heartbeats carry the event-time FRONTIER; they are what drives the watermark."""
            if args.offline:
                return int(base_ms + main_published * step_ms + tick * 1000)
            return int(base_ms + (time.time() - t0) * 1000)

        for i, ev in enumerate(stream):
            publish(ev["account_id"], ev["seq_no"], ev["amount_minor"],
                    ev["event_type"], ev["event_ts_ms"])

            if args.heartbeat_account and (args.offline or time.time() >= next_hb):
                if args.offline:
                    # deterministic cadence: one tick per heartbeat_interval of logical time
                    ticks_per_hb = max(1, int(args.heartbeat_interval * args.rate))
                    if i % ticks_per_hb != 0:
                        pass
                    else:
                        hb_seq += 1
                        publish(HEARTBEAT_ACCOUNT, hb_seq, 0, "CREDIT",
                                heartbeat_ts(i, 0))
                else:
                    hb_seq += 1
                    publish(HEARTBEAT_ACCOUNT, hb_seq, 0, "CREDIT", heartbeat_ts(i, 0))
                    next_hb = time.time() + args.heartbeat_interval

            if interval:
                time.sleep(interval)

        if args.heartbeat_account:
            for tick in range(1, args.trailing_heartbeats + 1):
                hb_seq += 1
                publish(HEARTBEAT_ACCOUNT, hb_seq, 0, "CREDIT",
                        heartbeat_ts(len(stream), tick))
                if not args.offline:
                    producer.poll(0)
                    time.sleep(args.heartbeat_interval)

    if not args.offline:
        producer.flush(30)
        if errors:
            print(f"DELIVERY ERRORS: {len(errors)} (first: {errors[0]})", file=sys.stderr)
            sys.exit(1)

    print(f"published {published} events")
    print(f"RUN_ID={run_id}")
    print(f"DELIVERY_LOG={log_path}")


if __name__ == "__main__":
    main()
