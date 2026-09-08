from __future__ import annotations

import json
from typing import Any, Dict, List, Tuple

from spark.engine.statecore import (KIND_BALANCE, empty_state, normalise_events,
                                    state_to_tuple, tuple_to_parts)


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
    last, bal, buf, last_seen = tuple_to_parts(state_tuple)
    outputs: List[tuple] = []
    deferred: List[int] = []

    if timed_out:
        pass

    # Sorting WITHIN the micro-batch is an optimisation, not the correctness
    # mechanism: it means a batch containing [5,4,3] applies directly instead of
    # round-tripping through the buffer. Order ACROSS batches is what the buffer
    # is for, and that is the part that actually matters.
    for ev in sorted(normalise_events(events_list), key=lambda e: e["seq_no"]):
        s = ev["seq_no"]
        amt = ev["amount_minor"]
        ts = ev["event_ts_ms"]
        if ts > last_seen:
            last_seen = ts

        if s == last + 1:
            #  branch 1: in-order apply, then drain 
            last, bal = s, bal + amt
            last, bal = drain(last, bal, buf)
        else:
            deferred.append(s)

    detail: Dict[str, Any] = {"deferred": len(deferred), "pending": len(buf)}
    if deferred:
        detail["deferred_seqs"] = sorted(deferred)[:20]

    outputs.append((KIND_BALANCE, last, detail))
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
