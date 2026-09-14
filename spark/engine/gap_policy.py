from __future__ import annotations

from typing import Any, Dict, List, Tuple

from spark.engine.statecore import KIND_GAP

FLAG_AND_CONTINUE = "FLAG_AND_CONTINUE"
HOLD = "HOLD"
VALID_POLICIES = (FLAG_AND_CONTINUE, HOLD)


def drain(last: int, bal: int, buf: Dict[int, Tuple[int, int]]) -> Tuple[int, int]:
    """Apply the contiguous run last+1, last+2, ... sitting in the buffer.

    Defined here and imported by the sequencer so that BOTH callers — normal
    in-order application and post-gap advancement — use the identical loop. A
    second copy of this five-line function is a second place for the reordering
    mechanism to be subtly wrong.
    """
    while last + 1 in buf:
        amt, _ts = buf.pop(last + 1)
        last, bal = last + 1, bal + amt
    return last, bal


def gap_range(last: int, buf: Dict[int, Tuple[int, int]]) -> Tuple[int, int]:
    """(lo, hi) of the confirmed hole. Caller guarantees buf is non-empty.

    lo is the first seq we are still waiting for; hi is one below the earliest
    thing we hold. Everything between is missing by construction — there is no
    need to enumerate it, and enumerating it is what produces alert storms.
    """
    lo = last + 1
    hi = min(buf) - 1
    return lo, hi


def on_gap(last: int, buf: Dict[int, Tuple[int, int]], *,
           watermark_ms: int = 0, alarm_event_ts: int | None = None
           ) -> List[Tuple[str, int, Dict[str, Any]]]:
    lo, hi = gap_range(last, buf)
    successor = min(buf)
    return [(KIND_GAP, lo, {
        "lo": lo,
        "hi": hi,
        "count": hi - lo + 1,
        "successor_seq": successor,
        "alarm_event_ts": int(alarm_event_ts) if alarm_event_ts is not None
                          else int(buf[successor][1]),
        "watermark_ms": int(watermark_ms),
    })]


def advance(last: int, bal: int, buf: Dict[int, Tuple[int, int]], policy: str
            ) -> Tuple[int, int, bool]:
    """Apply the policy. Returns (last, bal, advanced).

    `advanced` tells the caller whether progress was made, which is what decides
    between re-arming normally and re-arming on the bounded HOLD cadence.
    """
    if policy == HOLD:
        return last, bal, False

    if policy != FLAG_AND_CONTINUE:
        raise ValueError(f"unknown gap_policy: {policy!r} (expected one of {VALID_POLICIES})")

    # Step over the hole, then let the shared drain loop apply the buffered run
    # that was waiting behind it.
    last = min(buf) - 1
    last, bal = drain(last, bal, buf)
    return last, bal, True
