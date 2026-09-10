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
    return {
        "wall_ms": int(time.time() * 1000),
        "batch_id": progress.get("batchId"),
        "timestamp": progress.get("timestamp"),
        "watermark_ms": _parse_watermark(progress),
        "num_input_rows": progress.get("numInputRows"),
        "duration_ms": (progress.get("durationMs") or {}).get("triggerExecution"),
        "state_rows": op.get("numRowsTotal"),
        "state_bytes": op.get("memoryUsedBytes"),
    }


def start_progress_writer(query, path: str = PROGRESS_FILE_DEFAULT,
                          poll_seconds: float = 1.0) -> threading.Thread:
    """Poll query.lastProgress and append one JSONL line per NEW batch.

    Daemon thread: it must never keep the JVM alive after the query stops, and it
    must never be able to fail the job. Every exception inside the loop is
    swallowed on purpose — a broken observability side-channel is not a reason to
    stop processing money.
    """
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)

    def loop():
        seen = set()
        while True:
            try:
                if not query.isActive:
                    return
                progress = query.lastProgress
                if progress:
                    batch_id = progress.get("batchId")
                    if batch_id is not None and batch_id not in seen:
                        seen.add(batch_id)
                        with open(out, "a") as fh:
                            fh.write(json.dumps(_extract(progress)) + "\n")
            except Exception:
                pass
            time.sleep(poll_seconds)

    t = threading.Thread(target=loop, name="progress-writer", daemon=True)
    t.start()
    return t
