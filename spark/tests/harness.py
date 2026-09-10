from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from spark.engine.sequencer import step
from spark.engine.statecore import KIND_BALANCE, empty_state, tuple_to_parts


class Recorder:
    """Everything the harness saw, in the form the timing probe would see it."""

    def __init__(self) -> None:
        self.batches: List[Dict[str, Any]] = []      # one per micro-batch
        self.outputs: List[Tuple[int, str, int, Dict[str, Any]]] = []  # (batch_id, kind, seq, detail)

    def of_kind(self, kind: str):
        return [(b, s, d) for b, k, s, d in self.outputs if k == kind]

    def gaps(self):
        return self.of_kind("SEQUENCE_GAP")

    def dups(self):
        return self.of_kind("DUP_DROPPED")

    def overflows(self):
        return self.of_kind("BUFFER_OVERFLOW")

    def watermark_at(self, batch_id: int) -> int:
        return self.batches[batch_id]["watermark_ms"]

    def first_batch_with_watermark_at_or_past(self, ts: int) -> Optional[int]:
        for b in self.batches:
            if b["watermark_ms"] >= ts:
                return b["batch_id"]
        return None


def run_stream(events: Iterable[Dict[str, Any]], cfg: Dict[str, Any],
               *, batch_size: int = 50, watermark_delay_ms: int | None = None,
               trailing_idle_batches: int = 0,
               idle_event_time_step_ms: int = 0) -> Tuple[tuple, Recorder]:
    events = list(events)
    if watermark_delay_ms is None:
        watermark_delay_ms = int(cfg["watermark_delay_ms"])

    state = empty_state()
    rec = Recorder()
    max_event_ts = 0
    watermark = 0
    armed: Optional[int] = None
    batch_id = 0

    def invoke(rows: List[Dict[str, Any]], timed_out: bool) -> None:
        nonlocal state, armed
        state, outputs = step(state, rows, cfg, timed_out=timed_out,
                              watermark_ms=watermark)
        for kind, seq, detail in outputs:
            if kind == KIND_BALANCE:
                armed = detail.get("alarm_ts")
            else:
                rec.outputs.append((batch_id, kind, seq, detail))

    chunks: List[List[Dict[str, Any]]] = [
        events[i:i + batch_size] for i in range(0, len(events), batch_size)
    ] or [[]]
    chunks += [[] for _ in range(trailing_idle_batches)]

    for chunk in chunks:
        # 1. timeout check happens BEFORE the batch's rows are processed
        fired = armed is not None and armed <= watermark
        if fired:
            invoke([], timed_out=True)

        # 2. the batch's rows
        if chunk:
            invoke(chunk, timed_out=False)

        last, bal, buf, _seen = tuple_to_parts(state)
        rec.batches.append({
            "batch_id": batch_id,
            "watermark_ms": watermark,
            "num_rows": len(chunk),
            "timed_out": fired,
            "last_applied_seq": last,
            "balance_minor": bal,
            "buffer_size": len(buf),
            "armed_alarm_ts": armed,
        })

        # 3. advance event time, then recompute the watermark for the NEXT batch
        for e in chunk:
            max_event_ts = max(max_event_ts, int(e["event_ts_ms"]))
        if not chunk and idle_event_time_step_ms:
            max_event_ts += idle_event_time_step_ms     # the heartbeat account
        watermark = max(watermark, max_event_ts - watermark_delay_ms)
        batch_id += 1

    return state, rec


def run_stream_with_crash(events, cfg, *, crash_before_batch: int,
                          batch_size: int = 50, watermark_delay_ms: int | None = None,
                          trailing_idle_batches: int = 0,
                          idle_event_time_step_ms: int = 0):
    import copy
    import pickle

    events = list(events)
    if watermark_delay_ms is None:
        watermark_delay_ms = int(cfg["watermark_delay_ms"])

    chunks = [events[i:i + batch_size] for i in range(0, len(events), batch_size)] or [[]]
    chunks += [[] for _ in range(trailing_idle_batches)]
    if not 0 <= crash_before_batch < len(chunks):
        raise ValueError(f"crash_before_batch {crash_before_batch} out of range "
                         f"(0..{len(chunks) - 1})")

    state = empty_state()
    rec = Recorder()
    lost = Recorder()          # what the doomed attempt emitted to the sink
    max_event_ts = 0
    watermark = 0
    armed: Optional[int] = None
    batch_id = 0
    checkpoint: Optional[bytes] = None
    checkpoint_meta = None

    def invoke(rows, timed_out, into: Recorder):
        nonlocal state, armed
        state, outputs = step(state, rows, cfg, timed_out=timed_out, watermark_ms=watermark)
        for kind, seq, detail in outputs:
            if kind == KIND_BALANCE:
                armed = detail.get("alarm_ts")
            else:
                into.outputs.append((batch_id, kind, seq, detail))

    def run_batch(chunk, into: Recorder):
        nonlocal max_event_ts, watermark, batch_id
        fired = armed is not None and armed <= watermark
        if fired:
            invoke([], True, into)
        if chunk:
            invoke(chunk, False, into)
        last, bal, buf, _seen = tuple_to_parts(state)
        into.batches.append({
            "batch_id": batch_id, "watermark_ms": watermark, "num_rows": len(chunk),
            "timed_out": fired, "last_applied_seq": last, "balance_minor": bal,
            "buffer_size": len(buf), "armed_alarm_ts": armed,
        })

    for idx, chunk in enumerate(chunks):
        if idx == crash_before_batch:
            #  the checkpoint state as of the END of batch idx-1 
            checkpoint = pickle.dumps(state)
            checkpoint_meta = (max_event_ts, watermark, armed, batch_id)

            #  the doomed attempt: batch idx runs, its sink writes, then SIGKILL
            saved_state = copy.deepcopy(state)
            run_batch(chunk, lost)
            state = saved_state

            #  restart: state rolls back to the checkpoint, offsets roll back too
            state = pickle.loads(checkpoint)
            max_event_ts, watermark, armed, batch_id = checkpoint_meta
            # batch idx is re-read from offsets/idx and re-executed
            run_batch(chunk, rec)
        else:
            run_batch(chunk, rec)

        for e in chunk:
            max_event_ts = max(max_event_ts, int(e["event_ts_ms"]))
        if not chunk and idle_event_time_step_ms:
            max_event_ts += idle_event_time_step_ms
        watermark = max(watermark, max_event_ts - watermark_delay_ms)
        batch_id += 1

    return state, rec, lost


def events_from_delivery_log(rows: Iterable[Dict[str, Any]], account_id: str
                             ) -> List[Dict[str, Any]]:
    """Project one account's events out of a delivery log, preserving publish order."""
    return [{"seq_no": int(r["seq_no"]), "amount_minor": int(r["amount_minor"]),
             "event_ts_ms": int(r["event_ts_ms"])}
            for r in rows if r["account_id"] == account_id]
