from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

KIND_BALANCE = "BALANCE"
KIND_DUP = "DUP_DROPPED"
KIND_OVERFLOW = "BUFFER_OVERFLOW"
KIND_GAP = "SEQUENCE_GAP"
KIND_TTL = "TTL_FLUSH"

BALANCE_KINDS = (KIND_BALANCE, KIND_TTL)
SINK_FAIL_ONCE = os.environ.get("P3_SINK_FAIL_ONCE") == "1"
SINK_FAIL_MARKER = Path(os.environ.get("P3_SINK_FAIL_MARKER", "run/sink_failed_once"))


def _write_kafka(df: DataFrame, bootstrap: str, topic: str) -> None:
    """Batch-write a DataFrame to a Kafka topic as JSON, keyed by account_id."""
    payload = (df
               .withColumn("value", F.to_json(F.struct(*df.columns)))
               .select(F.col("account_id").cast("string").alias("key"), "value"))
    (payload.write
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", topic)
        .save())


def make_foreach_batch(cfg: Dict[str, Any]):
    balances_path = cfg["paths"]["balances"]
    integrity_path = cfg["paths"]["integrity"]
    bootstrap = cfg["kafka_bootstrap"]
    topic_signals = cfg["topics"]["signals"]
    topic_dlq = cfg["topics"]["dlq"]
    topic_integrity = cfg["topics"]["integrity"]

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        spark = batch_df.sparkSession
        batch_df.persist()
        try:
            if batch_df.isEmpty():
                return

            #  1. balances: seq-guarded MERGE 
            balance_rows = batch_df.filter(F.col("out_kind").isin(list(BALANCE_KINDS)))
            if not balance_rows.isEmpty():
                # The operator emits one row per key per batch, so a second row
                # for one account cannot occur — but a multi-match Delta MERGE is
                # a hard failure, and three lines of insurance costs less than
                # the incident.
                w = Window.partitionBy("account_id").orderBy(F.col("last_applied_seq").desc())
                latest = (balance_rows
                          .withColumn("_rn", F.row_number().over(w))
                          .filter(F.col("_rn") == 1)
                          .select("account_id", "last_applied_seq", "balance_minor",
                                  "buffer_size", "detail")
                          .withColumn("batch_id", F.lit(batch_id).cast("long"))
                          .withColumn("updated_at", F.current_timestamp()))

                if not DeltaTable.isDeltaTable(spark, balances_path):
                    latest.write.format("delta").mode("overwrite").save(balances_path)
                else:
                    (DeltaTable.forPath(spark, balances_path).alias("t")
                        .merge(latest.alias("s"), "t.account_id = s.account_id")
                        # strict > : a replayed batch carries the same
                        # last_applied_seq and is therefore a no-op.
                        .whenMatchedUpdateAll("s.last_applied_seq > t.last_applied_seq")
                        .whenNotMatchedInsertAll()
                        .execute())

            #  2. integrity events 
            integrity_rows = batch_df.filter(~F.col("out_kind").isin(list(BALANCE_KINDS)))
            if integrity_rows.isEmpty():
                return

            integrity = integrity_rows.select(
                F.col("account_id"),
                F.col("out_kind").alias("kind"),
                F.get_json_object("detail", "$.seq_no").cast("long").alias("seq_no"),
                (F.get_json_object("detail", "$.event_ts_ms").cast("long") / 1000)
                    .cast("timestamp").alias("event_ts"),
                F.lit(batch_id).cast("long").alias("batch_id"),
                F.col("detail"),
            )
            integrity.persist()
            try:
                integrity.write.format("delta").mode("append").save(integrity_path)
                _write_kafka(integrity, bootstrap, topic_integrity)

                overflow = integrity.filter(F.col("kind") == KIND_OVERFLOW)
                if not overflow.isEmpty():
                    _write_kafka(overflow.withColumn("reason", F.lit(KIND_OVERFLOW)),
                                 bootstrap, topic_dlq)

                gaps = integrity.filter(F.col("kind") == KIND_GAP)
                if not gaps.isEmpty():
                    _write_kafka(gaps, bootstrap, topic_signals)
            finally:
                integrity.unpersist()

            _maybe_fail_once(batch_id)

        finally:
            batch_df.unpersist()

    return write_batch


def _maybe_fail_once(batch_id: int) -> None:
    """Raise after the writes have landed, once, when the drill asks for it."""
    if not SINK_FAIL_ONCE or SINK_FAIL_MARKER.exists():
        return
    SINK_FAIL_MARKER.parent.mkdir(parents=True, exist_ok=True)
    SINK_FAIL_MARKER.write_text(str(batch_id))
    raise RuntimeError(
        f"P3_SINK_FAIL_ONCE: deliberate failure AFTER the sink wrote batch {batch_id}. "
        f"The restart must re-execute this batch; the strict-> MERGE guard should make "
        f"the balances update a no-op, and the append-only integrity table should show "
        f"the same (kind, seq_no, batch_id) rows twice.")
