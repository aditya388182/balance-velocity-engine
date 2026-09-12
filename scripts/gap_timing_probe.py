#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
from conf.config import CFG  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"
PROGRESS_FILE = REPO_ROOT / "logs" / "progress.jsonl"


def read_progress(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    rows.sort(key=lambda r: (r.get("batch_id") if r.get("batch_id") is not None else -1))
    return rows


def consume_gaps(topic: str, timeout: float) -> List[Dict[str, Any]]:
    consumer = Consumer({
        "bootstrap.servers": CFG["kafka_bootstrap"],
        "group.id": f"gap-probe-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])
    gaps, idle, poll_s = [], 0.0, 1.0
    try:
        while idle < timeout:
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
            try:
                rec = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue
            if rec.get("kind") != "SEQUENCE_GAP":
                continue
            detail = rec.get("detail")
            if isinstance(detail, str):
                try:
                    detail = json.loads(detail)
                except json.JSONDecodeError:
                    detail = {}
            rec["_detail"] = detail or {}
            gaps.append(rec)
    finally:
        consumer.close()
    return gaps


def dedup_gaps(gaps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One entry per (account, lo).

    Under HOLD the same hole is deliberately re-asserted on a bounded cadence, and
    a replayed batch can append the same record twice. Both are by design, so the
    probe collapses them the same way the integrity table's readers do — keeping
    the EARLIEST firing, because that is the one the timing property is about.
    """
    best: Dict[tuple, Dict[str, Any]] = {}
    for g in gaps:
        key = (g.get("account_id"), g["_detail"].get("lo"))
        prev = best.get(key)
        if prev is None or (g.get("batch_id") or 0) < (prev.get("batch_id") or 0):
            best[key] = g
    return sorted(best.values(), key=lambda g: (g.get("account_id") or "",
                                                g["_detail"].get("lo") or 0))


def first_eligible_batch(progress: List[Dict[str, Any]], alarm_ts: int) -> Optional[int]:
    for row in progress:
        wm = row.get("watermark_ms")
        if wm is not None and wm >= alarm_ts:
            return row.get("batch_id")
    return None


def batch_wall_ms(progress: List[Dict[str, Any]], batch_id: int) -> Optional[int]:
    for row in progress:
        if row.get("batch_id") == batch_id:
            return row.get("wall_ms")
    return None


def main() -> None:
    p = argparse.ArgumentParser(description="Gap-detection timing probe")
    p.add_argument("--topic", default=None, help="default: topics.signals")
    p.add_argument("--timeout", type=float, default=10.0,
                   help="seconds of Kafka silence before giving up")
    p.add_argument("--slack-ms", type=int, default=15_000,
                   help="allowance over watermark+trigger for batch scheduling jitter")
    p.add_argument("--expect-gaps", type=int, default=None,
                   help="assert exactly this many distinct gap ranges")
    p.add_argument("--expect-no-gaps", action="store_true",
                   help="negative control 1: a reorder within the watermark must not alert")
    p.add_argument("--expect-false-positives", action="store_true",
                   help="negative control 2: too short a watermark MUST produce gaps")
    p.add_argument("--after-crash", action="store_true",
                   help="skip the (c) BOUNDED assertion. After a recovery drill the "
                        "wall-clock latency spans the outage AND the catch-up backlog, "
                        "so the bound is not a property of the detector. (a) not-before "
                        "and (b) not-never still hold and are still checked.")
    p.add_argument("--progress-file", default=str(PROGRESS_FILE))
    args = p.parse_args()

    topic = args.topic or CFG["topics"]["signals"]
    wm_ms = int(CFG["watermark_delay_ms"])
    trig_ms = int(CFG["trigger_interval_ms"])
    bound_ms = wm_ms + trig_ms + args.slack_ms

    progress = read_progress(Path(args.progress_file))
    raw_gaps = consume_gaps(topic, args.timeout)
    gaps = dedup_gaps(raw_gaps)

    print(f"topic            : {topic}")
    print(f"progress batches : {len(progress)}   "
          f"{'(none — is the engine running with the progress writer?)' if not progress else ''}")
    print(f"gap signals      : {len(raw_gaps)} raw, {len(gaps)} distinct range(s)")
    print(f"watermark_delay  : {wm_ms} ms      trigger: {trig_ms} ms")
    print(f"bound            : watermark + trigger + slack = {bound_ms} ms")
    print("=" * 100)

    failures: List[str] = []

    #  negative control 1 
    if args.expect_no_gaps:
        if gaps:
            for g in gaps:
                d = g["_detail"]
                print(f"  {RED}UNEXPECTED{RESET} {g.get('account_id')} "
                      f"lo={d.get('lo')} hi={d.get('hi')}")
            failures.append(f"expected zero gaps, found {len(gaps)} — a reorder "
                            f"within the watermark alerted, which is a false positive")
        else:
            print(f"{GREEN}ZERO GAPS{RESET} — reordering within the watermark is "
                  f"LATE, not LOST, and produced no alert")
        _finish(failures)

    #  negative control 2 
    if args.expect_false_positives:
        if not gaps:
            failures.append("expected false positives but saw none — is the "
                            "watermark override actually in effect? "
                            "(P3_WATERMARK_DELAY should be echoed by [config] at startup)")
        else:
            print(f"{YELLOW}FALSE POSITIVES REPRODUCED{RESET} — {len(gaps)} gap range(s) "
                  f"on a stream with no missing events.")
            for g in gaps:
                d = g["_detail"]
                print(f"  account={g.get('account_id')} lo={d.get('lo')} "
                      f"hi={d.get('hi')} count={d.get('count')}")
            print(f"  {DIM}Under FLAG_AND_CONTINUE each of these stepped over an event "
                  f"that was merely late. Parity will report the balance short by "
                  f"exactly those amounts — the cost is money, not noise.{RESET}")
        _finish(failures)

    #  the three-way property 
    if not gaps:
        failures.append("NOT-NEVER violated: no SEQUENCE_GAP signal at all")
        _finish(failures)

    for g in gaps:
        d = g["_detail"]
        acct = g.get("account_id")
        lo, hi = d.get("lo"), d.get("hi")
        alarm = d.get("alarm_event_ts")
        wm_at_fire = d.get("watermark_ms")
        batch = g.get("batch_id")

        if alarm is None or wm_at_fire is None:
            failures.append(f"{acct} lo={lo}: record is missing alarm_event_ts or "
                            f"watermark_ms — the sequencer did not populate the evidence")
            continue

        fired_wall = batch_wall_ms(progress, batch) if batch is not None else None
        latency = (fired_wall - alarm) if fired_wall is not None else None
        eligible = first_eligible_batch(progress, alarm)

        verdicts = []

        # (a) NOT-BEFORE
        if wm_at_fire < alarm:
            verdicts.append(f"{RED}NOT-BEFORE VIOLATED{RESET} "
                            f"(watermark {wm_at_fire} < alarm {alarm})")
            failures.append(f"{acct} lo={lo}: fired before the watermark passed the alarm")
        elif eligible is not None and batch is not None and batch < eligible:
            verdicts.append(f"{RED}NOT-BEFORE VIOLATED{RESET} "
                            f"(batch {batch} precedes first eligible batch {eligible})")
            failures.append(f"{acct} lo={lo}: fired in batch {batch}, before batch {eligible}")
        else:
            verdicts.append(f"{GREEN}not-before OK{RESET}")

        # (b) NOT-NEVER is satisfied by the record existing
        verdicts.append(f"{GREEN}not-never OK{RESET}")

        # (c) BOUNDED
        if args.after_crash:
            verdicts.append(f"{YELLOW}bound SKIPPED (after-crash){RESET}")
            if latency is not None:
                print(f"    {DIM}measured latency {latency} ms includes the outage and "
                      f"the catch-up backlog{RESET}")
        elif latency is None:
            verdicts.append(f"{YELLOW}bounded UNMEASURED{RESET} "
                            f"(no progress row for batch {batch})")
        elif latency <= bound_ms:
            verdicts.append(f"{GREEN}WITHIN BOUND{RESET}")
        else:
            verdicts.append(f"{RED}BOUND VIOLATED{RESET}")
            failures.append(f"{acct} lo={lo}: detection latency {latency} ms "
                            f"exceeds the {bound_ms} ms bound")

        print(f"GAP account={acct} missing_seq={lo}"
              + (f"..{hi}" if hi != lo else "")
              + f" count={d.get('count')} successor={d.get('successor_seq')}")
        print(f"    alarm_event_ts   = {alarm}")
        print(f"    watermark_at_fire= {wm_at_fire}   (+{wm_at_fire - alarm} ms past the alarm)")
        print(f"    fired_batch      = {batch}"
              + (f"   first_eligible_batch = {eligible}" if eligible is not None else ""))
        if latency is not None:
            print(f"    detection_latency= {latency} ms   bound = {bound_ms} ms")
        print(f"    {'  '.join(verdicts)}")
        print()

    if args.expect_gaps is not None and len(gaps) != args.expect_gaps:
        failures.append(f"expected exactly {args.expect_gaps} distinct gap range(s), "
                        f"found {len(gaps)}")

    _finish(failures)


def _finish(failures: List[str]) -> None:
    print("=" * 100)
    if failures:
        print(f"{RED}TIMING PROBE FAIL{RESET} — {len(failures)} violation(s):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print(f"{GREEN}TIMING PROBE PASS{RESET}")
    sys.exit(0)


if __name__ == "__main__":
    main()
