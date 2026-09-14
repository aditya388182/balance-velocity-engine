from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from spark.engine import gap_policy
from spark.engine.gap_policy import drain  # re-exported: one drain loop, one definition
from spark.engine.statecore import (KIND_BALANCE, KIND_DUP, KIND_OVERFLOW, KIND_TTL,
                                    empty_state, normalise_events,
                                    state_to_tuple, tuple_to_parts)

DUP_PATH_APPLIED = "applied"     # seq <= last: the event was already applied
DUP_PATH_BUFFERED = "buffered"   # seq > last but already sitting in the buffer

__all__ = ["step", "drain", "make_sequencer", "compute_alarm",
           "DUP_PATH_APPLIED", "DUP_PATH_BUFFERED"]


def compute_alarm(buf: Dict[int, Tuple[int, int]], watermark_ms: int,
                  *, defer_ms: int = 0, last_seen_ms: int = 0,
                  state_ttl_ms: int = 0) -> int | None:
    if not buf:
        if state_ttl_ms <= 0:
            return None
        # The clamp matters here too: an account already idle longer than the TTL
        # when its alarm is computed must fire on the next batch, not never.
        return int(max(last_seen_ms + state_ttl_ms, watermark_ms + 1))

    if defer_ms > 0:
        return int(watermark_ms + defer_ms)

    earliest_successor_ts = min(ts for _amt, ts in buf.values())
    # The clamp: setTimeoutTimestamp throws on a value at or below the watermark.
    return int(max(earliest_successor_ts, watermark_ms + 1))


def step(state_tuple, events_list: List[Dict[str, Any]], cfg: Dict[str, Any],
         *, timed_out: bool = False, watermark_ms: int = 0,
         opening: Tuple[int, int] | None = None, state_exists: bool = True
         ) -> Tuple[tuple, List[tuple]]:
    max_buffer = int(cfg["max_buffer_size"])
    policy = cfg.get("gap_policy", gap_policy.FLAG_AND_CONTINUE)
    realert_ms = int(cfg.get("gap_realert_ms", 60_000))
    state_ttl_ms = int(cfg.get("state_ttl_ms", 0))

    last, bal, buf, last_seen = tuple_to_parts(state_tuple)
    outputs: List[tuple] = []
    held_open = False
    ttl_flushed = False
    reseeded = False

    if not state_exists and opening is not None:
        o_seq, o_bal = int(opening[0]), int(opening[1])
        if o_seq > 0:
            last, bal = o_seq, o_bal
            reseeded = True

    #  the timeout branch 
    # ONE timeout slot, TWO alarms. Disambiguate by state, never by guessing:
    #   non-empty buffer          -> the gap is confirmed
    #   empty + stale last_seen   -> the account is idle, flush and release
    #   empty + fresh last_seen   -> a stale alarm; just re-arm
    if timed_out:
        if buf:
            alarm_event_ts = min(ts for _amt, ts in buf.values())
            outputs += gap_policy.on_gap(last, buf, watermark_ms=watermark_ms,
                                         alarm_event_ts=alarm_event_ts)
            last, bal, advanced = gap_policy.advance(last, bal, buf, policy)
            held_open = not advanced
        elif (state_ttl_ms > 0 and last_seen > 0
              and (watermark_ms - last_seen) >= state_ttl_ms):
            ttl_flushed = True

    # Sorting WITHIN the micro-batch is an OPTIMISATION, not the correctness
    # mechanism: a batch containing [5,4,3] applies directly instead of
    # round-tripping through the buffer. Order ACROSS batches is what the buffer
    # is for, and that is the part that actually matters.
    for ev in sorted(normalise_events(events_list), key=lambda e: e["seq_no"]):
        s = ev["seq_no"]
        amt = ev["amount_minor"]
        ts = ev["event_ts_ms"]
        if ts > last_seen:
            last_seen = ts

        #  branch 1: in-order -> apply, then drain 
        if s == last + 1:
            last, bal = s, bal + amt
            last, bal = drain(last, bal, buf)
            continue

        #  branch 2: early arrival -> buffer 
        if s > last + 1:
            if s in buf:
                #  branch 4b: re-delivery of a still-buffered event 
                outputs.append((KIND_DUP, s, {
                    "path": DUP_PATH_BUFFERED,
                    "amount_minor": amt,
                    "event_ts_ms": ts,
                }))
                continue

            if len(buf) >= max_buffer:
                #  branch 3: overflow -> min-first eviction to the DLQ 
                # min over the buffer AND the arrival: if the arrival is the
                # lowest, evicting min(buf) would discard a higher seq and admit
                # a lower one, breaking the "buffer holds the k largest" invariant
                # the oracle's independent DLQ prediction rests on.
                victim = s if not buf else min(min(buf), s)
                if victim == s:
                    outputs.append((KIND_OVERFLOW, s, {
                        "reason": "BUFFER_OVERFLOW",
                        "amount_minor": amt,
                        "event_ts_ms": ts,
                        "evicted_on_arrival": True,
                        "buffer_size": len(buf),
                    }))
                    continue
                v_amt, v_ts = buf.pop(victim)
                outputs.append((KIND_OVERFLOW, victim, {
                    "reason": "BUFFER_OVERFLOW",
                    "amount_minor": v_amt,
                    "event_ts_ms": v_ts,
                    "evicted_on_arrival": False,
                    "buffer_size": len(buf) + 1,
                }))

            buf[s] = (amt, ts)
            continue

        #  branch 4: seq <= last -> duplicate/replay, drop but COUNT 
        outputs.append((KIND_DUP, s, {
            "path": DUP_PATH_APPLIED,
            "amount_minor": amt,
            "event_ts_ms": ts,
        }))

    # A TTL flush that then received rows in the same invocation is not idle after
    # all — the rows are the account speaking again. Keep the state.
    if ttl_flushed and events_list:
        ttl_flushed = False

    #  (re)arm the alarm 
    # held_open means HOLD just fired and made no progress, so arming from the
    # buffer would put the alarm behind the watermark and fire again on the very
    # next trigger. Defer instead: a held gap is a live incident that should be
    # re-asserted on a bounded cadence, not repeated every five seconds.
    alarm_ts = None if ttl_flushed else compute_alarm(
        buf, watermark_ms,
        defer_ms=realert_ms if held_open else 0,
        last_seen_ms=last_seen, state_ttl_ms=state_ttl_ms)

    detail = {
        "pending": len(buf),
        "pending_lo": (min(buf) if buf else None),
        "pending_hi": (max(buf) if buf else None),
        "alarm_ts": alarm_ts,
        "watermark_ms": int(watermark_ms),
    }
    if reseeded:
        detail["reseeded_from"] = [int(opening[0]), int(opening[1])]

    if ttl_flushed:
        # The final balance goes to Delta through the same MERGE path a BALANCE row
        # takes, then the operator releases the state. idle_ms is in EVENT time.
        detail["idle_ms"] = int(watermark_ms - last_seen)
        detail["ttl_ms"] = state_ttl_ms
        outputs.append((KIND_TTL, last, detail))
    else:
        outputs.append((KIND_BALANCE, last, detail))

    return state_to_tuple(last, bal, buf, last_seen), outputs


def make_sequencer(cfg: Dict[str, Any]):
    import pandas as pd

    from spark.engine.state import OUTPUT_COLUMNS, OUTPUT_DTYPES

    def sequencer(key, pdf_iter, state):
        account_id = key[0]

        # A timed-out invocation receives NO ROWS: pdf_iter yields nothing and
        # this loop simply produces an empty list.
        events: List[Dict[str, Any]] = []
        opening = None
        for pdf in pdf_iter:
            for row in pdf.itertuples(index=False):
                events.append({
                    "seq_no": int(row.seq_no),
                    "amount_minor": int(row.amount_minor),
                    "event_ts_ms": int(row.event_ts_ms),
                })
                # The opening balance rides in on the rows via a stream-static join
                # with the balances table. Every row for a key carries the same
                # value, so the first non-null one is enough.
                if opening is None:
                    o_seq = getattr(row, "opening_seq", None)
                    o_bal = getattr(row, "opening_balance", None)
                    if o_seq is not None and o_bal is not None and o_seq == o_seq:
                        opening = (int(o_seq), int(o_bal))

        exists = state.exists
        prior = tuple(state.get) if exists else empty_state()
        watermark_ms = int(state.getCurrentWatermarkMs())

        new_state, outputs = step(prior, events, cfg,
                                  timed_out=state.hasTimedOut,
                                  watermark_ms=watermark_ms,
                                  opening=opening, state_exists=exists)

        final_kind = outputs[-1][0]
        if final_kind == KIND_TTL:
            # Release the state. No update, no alarm: the key is gone until it
            # speaks again, and when it does the re-seed above restores its opening
            # balance from Delta.
            state.remove()
        else:
            state.update(new_state)
            # The pure core decided WHEN; the operator only carries the decision
            # across the Spark boundary. The clamp already happened in compute_alarm.
            alarm_ts = outputs[-1][2].get("alarm_ts")
            if alarm_ts is not None:
                state.setTimeoutTimestamp(int(alarm_ts))

        last, bal, _blob, buf_size, _last_seen = new_state

        rows = []
        for kind, seq_no, detail in outputs:
            det = dict(detail) if detail else {}
            if kind != KIND_BALANCE:
                det["seq_no"] = int(seq_no)
            rows.append({
                "account_id": account_id,
                "last_applied_seq": int(last),
                "balance_minor": int(bal),
                "buffer_size": int(buf_size),
                "out_kind": kind,
                "detail": json.dumps(det, sort_keys=True),
            })

        yield pd.DataFrame(rows, columns=OUTPUT_COLUMNS).astype(OUTPUT_DTYPES)

    return sequencer
# STATE_SCHEMA = v3.5.1
