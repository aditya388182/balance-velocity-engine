from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from spark.engine.statecore import (KIND_BALANCE, KIND_DUP, KIND_OVERFLOW,
                                    empty_state, normalise_events,
                                    state_to_tuple, tuple_to_parts)

DUP_PATH_APPLIED = "applied"     # seq <= last: the event was already applied
DUP_PATH_BUFFERED = "buffered"   # seq > last but already sitting in the buffer


def drain(last: int, bal: int, buf: Dict[int, Tuple[int, int]]) -> Tuple[int, int]:
    """Apply the contiguous run last+1, last+2, ... sitting in the buffer.

    Shared helper: from Day 3 the FLAG_AND_CONTINUE gap policy calls this too,
    after stepping `last` over a confirmed hole.
    """
    while last + 1 in buf:
        amt, _ts = buf.pop(last + 1)
        last, bal = last + 1, bal + amt
    return last, bal


def step(state_tuple, events_list: List[Dict[str, Any]], cfg: Dict[str, Any],
         *, timed_out: bool = False, watermark_ms: int = 0
         ) -> Tuple[tuple, List[tuple]]:
    """Pure core. FINAL SIGNATURE — Days 3-5 add branches, never parameters.

    events_list : [{'seq_no', 'amount_minor', 'event_ts_ms'}, ...]
    returns     : (new_state_tuple, outputs)
                  outputs = [(out_kind, seq_no, detail_dict_or_None), ...]
                  The BALANCE row is always present and always last.
    """
    max_buffer = int(cfg["max_buffer_size"])
    last, bal, buf, last_seen = tuple_to_parts(state_tuple)
    outputs: List[tuple] = []

    if timed_out:
        pass

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
                victim = s if not buf else min(min(buf), s)
                if victim == s:
                    # the arrival itself is the minimum: evict it on arrival
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

    outputs.append((KIND_BALANCE, last, {
        "pending": len(buf),
        "pending_lo": (min(buf) if buf else None),
        "pending_hi": (max(buf) if buf else None),
    }))

    return state_to_tuple(last, bal, buf, last_seen), outputs


def make_sequencer(cfg: Dict[str, Any]):
    """Build the applyInPandasWithState function, bound to a config.

    pandas and the PySpark schemas are imported here rather than at module level
    so that `from spark.engine.sequencer import step` stays free of both.
    """
    import pandas as pd

    from spark.engine.state import OUTPUT_COLUMNS, OUTPUT_DTYPES

    def sequencer(key, pdf_iter, state):
        account_id = key[0]

        events: List[Dict[str, Any]] = []
        for pdf in pdf_iter:
            for row in pdf.itertuples(index=False):
                events.append({
                    "seq_no": int(row.seq_no),
                    "amount_minor": int(row.amount_minor),
                    "event_ts_ms": int(row.event_ts_ms),
                })

        prior = tuple(state.get) if state.exists else empty_state()
        watermark_ms = state.getCurrentWatermarkMs()

        new_state, outputs = step(prior, events, cfg,
                                  timed_out=state.hasTimedOut,
                                  watermark_ms=watermark_ms)
        state.update(new_state)

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
