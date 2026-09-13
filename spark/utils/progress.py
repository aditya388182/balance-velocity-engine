# spark/utils/progress.py
"""Write the streaming query's per-batch progress to logs/progress.jsonl.

WHY THIS EXISTS ON DAY 3
------------------------
The gap-timing probe has to answer "what was the watermark when the gap fired?".
That number lives in query.lastProgress, which only the driver process can see —
and the probe is a separate process consuming Kafka. Something has to carry the
watermark across that boundary.

A background thread polling lastProgress is the smallest thing that works. It is
deliberately chosen over a StreamingQueryListener: the listener API is the right
long-term shape, but a polling thread has no callback-registration failure mode
and no risk of a slow callback back-pressuring the query, and on Day 3 the point
is to measure the engine, not to debug the measuring apparatus.

Day 5's metrics.py pushes these same numbers to the Prometheus pushgateway. The
field names here are the ones the runbooks will reference, so they are fixed now.

Each line:
    {"wall_ms", "batch_id", "watermark_ms", "num_input_rows",
     "state_rows", "state_bytes", "duration_ms", "timestamp"}
"""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

PROGRESS_FILE_DEFAULT = "logs/progress.jsonl"


def _parse_watermark(progress: Dict[str, Any]) -> Optional[int]:
    """lastProgress reports the watermark as an ISO-8601 string, or omits it."""
    et = progress.get("eventTime") or {}
    raw = et.get("watermark")
    if not raw:
        return None
    try:
        from datetime import datetime, timezone
        cleaned = raw.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _extract(progress: Dict[str, Any]) -> Dict[str, Any]:
    ops = progress.get("stateOperators") or []
    op = ops[0] if ops else {}
    # The AUTHORITATIVE caught-up signal. endOffset is what this batch consumed up
    # to; latestOffset is what the source had available. Equal means the query has
    # read everything that existed. Inferring the same thing from "no new batches"
    # is a guess; this is the query's own answer.
    srcs = progress.get("sources") or []
    src = srcs[0] if srcs else {}
    return {
        "source_end_offset": json.dumps(src.get("endOffset"), sort_keys=True),
        "source_latest_offset": json.dumps(src.get("latestOffset"), sort_keys=True),
        "wall_ms": int(time.time() * 1000),
        "batch_id": progress.get("batchId"),
        "timestamp": progress.get("timestamp"),
        "watermark_ms": _parse_watermark(progress),
        "num_input_rows": progress.get("numInputRows"),
        "duration_ms": (progress.get("durationMs") or {}).get("triggerExecution"),
        "state_rows": op.get("numRowsTotal"),
        "state_bytes": op.get("memoryUsedBytes"),
    }


def start_progress_writer(query_or_queries, path: str = PROGRESS_FILE_DEFAULT,
                          poll_seconds: float = 1.0) -> threading.Thread:
    """Poll lastProgress for EVERY query and append one JSONL line per new batch.

    It used to poll a single query — the sequencer — which meant the velocity
    queries had no health signal at all. When one of them died, the only evidence
    was rows missing from a Delta table two steps downstream, which reads like a
    windowing bug rather than a dead query. An unobserved query is an unfalsifiable
    one; each line now carries query_name.

    Daemon thread: it must never keep the JVM alive after the query stops, and it
    must never be able to fail the job. Every exception inside the loop is
    swallowed on purpose — a broken observability side-channel is not a reason to
    stop processing money.
    """
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)

    queries = (list(query_or_queries)
               if isinstance(query_or_queries, (list, tuple))
               else [query_or_queries])

    def loop():
        seen = set()
        while True:
            try:
                if not any(q.isActive for q in queries):
                    return
                for q in queries:
                    name = getattr(q, "name", None) or "unnamed"
                    try:
                        if not q.isActive:
                            # Record the death ONCE. A query that stops while the
                            # others run is the failure mode that was invisible.
                            if (name, "STOPPED") not in seen:
                                seen.add((name, "STOPPED"))
                                exc = None
                                try:
                                    exc = str(q.exception()) if q.exception() else None
                                except Exception:
                                    pass
                                with open(out, "a") as fh:
                                    fh.write(json.dumps({
                                        "wall_ms": int(time.time() * 1000),
                                        "query_name": name, "event": "STOPPED",
                                        "exception": (exc or "")[:500]}) + "\n")
                            continue
                        progress = q.lastProgress
                        if not progress:
                            continue
                        batch_id = progress.get("batchId")
                        if batch_id is None or (name, batch_id) in seen:
                            continue
                        seen.add((name, batch_id))
                        row = _extract(progress)
                        row["query_name"] = name
                        with open(out, "a") as fh:
                            fh.write(json.dumps(row) + "\n")
                    except Exception:
                        continue
            except Exception:
                pass
            time.sleep(poll_seconds)

    t = threading.Thread(target=loop, name="progress-writer", daemon=True)
    t.start()
    return t
