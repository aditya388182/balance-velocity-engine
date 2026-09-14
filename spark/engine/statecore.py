from __future__ import annotations

import pickle
from typing import Any, Dict, List, Tuple

#  out_kind vocabulary (contract 4) 
KIND_BALANCE = "BALANCE"
KIND_GAP = "SEQUENCE_GAP"       
KIND_DUP = "DUP_DROPPED"         
KIND_OVERFLOW = "BUFFER_OVERFLOW"  
KIND_TTL = "TTL_FLUSH"           

OUTPUT_COLUMNS = [
    "account_id",
    "last_applied_seq",
    "balance_minor",
    "buffer_size",
    "out_kind",
    "detail",
]

PICKLE_PROTOCOL = 4              # fixed: the restart test round-trips through this exact path
EMPTY_BUFFER_BYTES = pickle.dumps({}, protocol=PICKLE_PROTOCOL)

StateTuple = Tuple[int, int, bytes, int, int]


def empty_state(now_ms: int = 0) -> StateTuple:
    """A never-before-seen account. Field order MUST match STATE_SCHEMA."""
    return (0, 0, EMPTY_BUFFER_BYTES, 0, int(now_ms))


def pack_buffer(buf: Dict[int, Tuple[int, int]]) -> bytes:
    """Serialise the pending buffer with NATIVE PYTHON INT keys and values.

    numpy.int64 keys pickle and unpickle without complaint, and then compare
    strangely against `last + 1` after a pandas round-trip — producing a buffer
    that never drains and a gap that never closes, with no error anywhere. The
    cast happens here, once, at the boundary, rather than being hoped for at
    every call site.
    """
    clean = {int(k): (int(v[0]), int(v[1])) for k, v in buf.items()}
    return pickle.dumps(clean, protocol=PICKLE_PROTOCOL)


def unpack_buffer(blob: Any) -> Dict[int, Tuple[int, int]]:
    if not blob:
        return {}
    raw = pickle.loads(bytes(blob))
    return {int(k): (int(v[0]), int(v[1])) for k, v in raw.items()}


def state_to_tuple(last_applied_seq: int, balance_minor: int,
                   buf: Dict[int, Tuple[int, int]], last_seen_ms: int) -> StateTuple:
    return (int(last_applied_seq), int(balance_minor), pack_buffer(buf),
            int(len(buf)), int(last_seen_ms))


def tuple_to_parts(state_tuple) -> Tuple[int, int, Dict[int, Tuple[int, int]], int]:
    last, bal, blob, _size, last_seen = state_tuple
    return int(last), int(bal), unpack_buffer(blob), int(last_seen)


def normalise_events(events: List[Dict[str, Any]]) -> List[Dict[str, int]]:
    """Coerce an event list to native ints once, at the edge of the pure core."""
    return [{
        "seq_no": int(e["seq_no"]),
        "amount_minor": int(e["amount_minor"]),
        "event_ts_ms": int(e["event_ts_ms"]),
    } for e in events]
