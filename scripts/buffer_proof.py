#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from conf.config import CFG  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"


def dlq_scan(timeout: float = 10.0):
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
    p = argparse.ArgumentParser(description="Stage 5 terminal-state proof")
    p.add_argument("--account", default="HOT-1")
    p.add_argument("--mode", choices=["capped", "uncapped", "report"], default="report")
    p.add_argument("--events", type=int, default=10_000,
                   help="how many events the burst published (for the expected figures)")
    p.add_argument("--amount", type=int, default=-1_000,
                   help="signed minor units per burst event")
    p.add_argument("--save", default=None)
    args = p.parse_args()

    cap = int(CFG["max_buffer_size"])
    n = args.events
    survivors = min(cap, n)
    expect_dlq = max(0, n - cap)
    expect_balance = survivors * args.amount
    expect_last = 1 + n              # start-seq 2 .. 2+n-1, head withheld then stepped over

    from spark.utils.session import build_spark
    spark = build_spark(CFG, app_name="buffer-proof", streaming=False)
    try:
        rows = (spark.read.format("delta").load(CFG["paths"]["balances"])
                .select("account_id", "last_applied_seq", "balance_minor", "buffer_size")
                .collect())
        try:
            integ = (spark.read.format("delta").load(CFG["paths"]["integrity"])
                     .select("kind").collect())
        except Exception:
            integ = []
    finally:
        spark.stop()

    row = next((r for r in rows if r["account_id"] == args.account), None)
    if row is None:
        print(f"{RED}account {args.account} is not in the balances table{RESET}")
        print(f"  present: {sorted(r['account_id'] for r in rows)}")
        sys.exit(1)

    gaps = sum(1 for r in integ if r["kind"] == "SEQUENCE_GAP")
    dlq_total, seqs = dlq_scan()
    last = int(row["last_applied_seq"])
    bal = int(row["balance_minor"])
    buf = int(row["buffer_size"])

    print(f"account            : {args.account}")
    print(f"configured cap     : {cap}   {DIM}(from CFG — pass P3_MAX_BUFFER_SIZE to "
          f"match the run){RESET}")
    print(f"last_applied_seq   : {last:<12} expected {expect_last}")
    print(f"balance_minor      : {bal:<12} expected {expect_balance}")
    print(f"buffer_size (term.): {buf:<12} {DIM}0 once the gap has fired and drained{RESET}")
    print(f"DLQ records        : {dlq_total:<12} expected {expect_dlq}"
          + (f"   seqs {min(seqs)}..{max(seqs)}" if seqs else ""))
    print(f"SEQUENCE_GAP rows  : {gaps}")
    print("=" * 78)

    failures = []

    if gaps == 0 and last < expect_last:
        print(f"{YELLOW}THE GAP HAS NOT FIRED YET{RESET} — this run has not reached its "
              f"terminal state.")
        print(f"  {DIM}The alarm sits at the EARLIEST buffered event_ts. Under the cap the")
        print(f"  buffer holds only the newest events, so that timestamp is far forward and")
        print(f"  the watermark has to travel further to reach it. Push event time with a")
        print(f"  tick run and re-read:")
        print(f"    python scripts/event_generator.py --accounts 1 --account-prefix TICK- \\")
        print(f"           --rate 2 --duration 90 --heartbeat-account --seed 3{RESET}")
        failures.append("terminal state not reached — gap has not fired")

    if last != expect_last:
        failures.append(f"last_applied_seq {last} != expected {expect_last}")
    if bal != expect_balance:
        failures.append(f"balance {bal} != expected {expect_balance} "
                        f"(diff {bal - expect_balance:+d})")
    if dlq_total != expect_dlq:
        failures.append(f"DLQ count {dlq_total} != expected {expect_dlq}")

    if args.mode == "capped" and dlq_total == 0:
        failures.append("a capped run shed nothing — is max_buffer_size really in effect?")
    if args.mode == "uncapped":
        if dlq_total > 0:
            failures.append(f"{dlq_total} DLQ records on an uncapped run — the "
                            f"P3_MAX_BUFFER_SIZE override did not take effect")
        elif not failures:
            print(f"{GREEN}NOTHING WAS SHED{RESET} — all {n} events survived and applied, "
                  f"which is exactly the unbounded behaviour the cap prevents")

    if args.mode == "capped" and not failures:
        print(f"{GREEN}THE CAP HELD{RESET} — {survivors} events survived, {expect_dlq} "
              f"were shed to the DLQ, and the balance is short by "
              f"{abs(expect_dlq * args.amount):,} minor units BY DESIGN")

    if args.save:
        Path(args.save).write_text(json.dumps({
            "account": args.account, "mode": args.mode, "cap": cap,
            "last_applied_seq": last, "balance_minor": bal,
            "terminal_buffer_size": buf, "dlq_records": dlq_total,
            "sequence_gap_rows": gaps,
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
