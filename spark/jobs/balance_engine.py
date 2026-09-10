#!/usr/bin/env python3
from __future__ import annotations

import os
import signal
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql.streaming.state import GroupStateTimeout  # noqa: E402

from conf.config import CFG, checkpoint_path  # noqa: E402
from spark.engine.sequencer import make_sequencer  # noqa: E402
from spark.engine.sinks import make_foreach_batch  # noqa: E402
from spark.engine.state import OUTPUT_SCHEMA, STATE_SCHEMA  # noqa: E402
from spark.utils.avro_deserializer import deserialize_stream  # noqa: E402
from spark.utils.progress import start_progress_writer  # noqa: E402
from spark.utils.session import assert_state_store_configured, build_spark  # noqa: E402

PID_FILE = REPO_ROOT / "run" / "engine.pid"


def _cleanup_pid(*_args):
    try:
        PID_FILE.unlink()
    except FileNotFoundError:
        pass
    sys.exit(0)


def main() -> None:
    spark = build_spark(CFG, app_name="balance-engine", streaming=True)
    assert_state_store_configured(spark)

    ckpt = checkpoint_path(CFG, "balance_engine")
    print(f"[engine] checkpoint      : {ckpt}")
    print(f"[engine] balances        : {CFG['paths']['balances']}")
    print(f"[engine] watermark       : {CFG['watermark_delay']}")
    print(f"[engine] trigger         : {CFG['trigger_interval']}")
    print(f"[engine] max_buffer_size : {CFG['max_buffer_size']}")
    print(f"[engine] gap_policy      : {CFG['gap_policy']}")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", CFG["kafka_bootstrap"])
           .option("subscribe", CFG["topics"]["events"])
           # earliest is only deterministic because reset_lake.sh deletes and
           # recreates the topics. Without that, every re-run replays the previous
           # run's events and parity quietly lies to you.
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .option("maxOffsetsPerTrigger", str(CFG["spark"]["max_offsets_per_trigger"]))
           .load())

    events = deserialize_stream(raw, CFG["schema_registry_url"])

    #  checkpoint-identity decision: the watermark 
    events = events.withWatermark("event_ts", CFG["watermark_delay"])

    seq_out = (events.groupBy("account_id")
               .applyInPandasWithState(
                   make_sequencer(CFG),
                   OUTPUT_SCHEMA,
                   STATE_SCHEMA,
                   "append",
                   GroupStateTimeout.EventTimeTimeout,
               ))

    query = (seq_out.writeStream
             .queryName("balance_engine")
             .foreachBatch(make_foreach_batch(CFG))
             .option("checkpointLocation", ckpt)
             .trigger(processingTime=CFG["trigger_interval"])
             .start())

    progress_path = REPO_ROOT / "logs" / "progress.jsonl"
    start_progress_writer(query, str(progress_path), poll_seconds=1.0)
    print(f"[engine] progress    : {progress_path}")

    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()))
    print(f"[engine] pid {os.getpid()} -> {PID_FILE}")
    print("[engine] streaming; Ctrl-C or SIGTERM to stop")

    signal.signal(signal.SIGTERM, _cleanup_pid)

    try:
        query.awaitTermination()
    finally:
        try:
            PID_FILE.unlink()
        except FileNotFoundError:
            pass


if __name__ == "__main__":
    main()
