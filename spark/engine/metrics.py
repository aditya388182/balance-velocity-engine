from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict

METRIC_STATE_ROWS = "p3_state_rows"
METRIC_STATE_BYTES = "p3_state_bytes"
METRIC_BUFFER_P99 = "p3_buffer_p99"
METRIC_BUFFER_MAX = "p3_buffer_max"
METRIC_ACCOUNTS_AT_CAP = "p3_accounts_at_cap"
METRIC_GAP_RATE = "p3_gap_rate"
METRIC_DUP_DROPPED = "p3_dup_dropped"
METRIC_OVERFLOW_RATE = "p3_overflow_rate"
METRIC_TTL_FLUSH = "p3_ttl_flush"
METRIC_SNAPSHOT_LAG = "p3_snapshot_lag_seconds"

ALL_METRICS = [
    METRIC_STATE_ROWS, METRIC_STATE_BYTES, METRIC_BUFFER_P99, METRIC_BUFFER_MAX,
    METRIC_ACCOUNTS_AT_CAP, METRIC_GAP_RATE, METRIC_DUP_DROPPED,
    METRIC_OVERFLOW_RATE, METRIC_TTL_FLUSH, METRIC_SNAPSHOT_LAG,
]

METRICS_FILE = Path(os.environ.get("P3_METRICS_FILE", "logs/metrics.jsonl"))
PUSHGATEWAY = os.environ.get("P3_PUSHGATEWAY", "http://localhost:9091")
JOB_NAME = os.environ.get("P3_METRICS_JOB", "balance_engine")


def emit(metrics: Dict[str, Any], *, job: str = JOB_NAME) -> None:
    """Append to the JSONL file, then try the pushgateway. Never raises."""
    try:
        METRICS_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(METRICS_FILE, "a") as fh:
            fh.write(json.dumps({"wall_ms": int(time.time() * 1000),
                                 "job": job, **metrics}) + "\n")
    except Exception:
        pass

    try:
        import urllib.request
        body = "".join(
            f"# TYPE {k} gauge\n{k} {float(v)}\n"
            for k, v in metrics.items()
            if isinstance(v, (int, float)) and k in ALL_METRICS)
        if not body:
            return
        req = urllib.request.Request(
            f"{PUSHGATEWAY}/metrics/job/{job}",
            data=body.encode(), method="POST",
            headers={"Content-Type": "text/plain"})
        urllib.request.urlopen(req, timeout=2)
    except Exception:
        pass   


def push_batch_metrics(batch_df, batch_id: int, cfg: Dict[str, Any]) -> None:
    """Derive the per-batch numbers from the rows the operator just emitted.

    buffer_size only exists on these rows — it is not in lastProgress and not in
    the state store metrics, because the buffer is a pickled blob inside one state
    row per account. This is the only place the number is observable.
    """
    from pyspark.sql import functions as F

    cap = int(cfg["max_buffer_size"])
    balance_rows = batch_df.filter(F.col("out_kind").isin(["BALANCE", "TTL_FLUSH"]))

    buf_p99 = buf_max = at_cap = 0
    if not balance_rows.isEmpty():
        agg = balance_rows.agg(
            F.expr("percentile_approx(buffer_size, 0.99)").alias("p99"),
            F.max("buffer_size").alias("mx"),
            F.sum(F.when(F.col("buffer_size") >= cap, 1).otherwise(0)).alias("at_cap"),
        ).first()
        buf_p99 = int(agg["p99"] or 0)
        buf_max = int(agg["mx"] or 0)
        at_cap = int(agg["at_cap"] or 0)

    counts = {r["out_kind"]: r["n"] for r in
              batch_df.groupBy("out_kind").agg(F.count("*").alias("n")).collect()}

    emit({
        "batch_id": batch_id,
        METRIC_BUFFER_P99: buf_p99,
        METRIC_BUFFER_MAX: buf_max,
        METRIC_ACCOUNTS_AT_CAP: at_cap,
        METRIC_GAP_RATE: int(counts.get("SEQUENCE_GAP", 0)),
        METRIC_DUP_DROPPED: int(counts.get("DUP_DROPPED", 0)),
        METRIC_OVERFLOW_RATE: int(counts.get("BUFFER_OVERFLOW", 0)),
        METRIC_TTL_FLUSH: int(counts.get("TTL_FLUSH", 0)),
    })
