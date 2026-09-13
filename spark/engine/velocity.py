from __future__ import annotations

from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

WINDOW_SHORT = "short"
WINDOW_LONG = "long"


def velocity_frame(events: DataFrame, cfg: Dict[str, Any], which: str) -> DataFrame:
    """One native sliding-window aggregation. No custom state anywhere in here."""
    if which == WINDOW_SHORT:
        size, slide, limit = cfg["vel_window"], cfg["vel_slide"], int(cfg["vel_limit"])
    else:
        size = cfg.get("vel_window_long", cfg["vel_window"])
        slide = cfg.get("vel_slide_long", cfg["vel_slide"])
        # the long window covers proportionally more time, so the cap scales with it
        limit = int(cfg["vel_limit"]) * 5

    return (events
            .filter(F.col("event_type") == "DEBIT")
            .groupBy(F.col("account_id"),
                     F.window(F.col("event_ts"), size, slide))
            .agg(F.sum(F.abs(F.col("amount_minor"))).alias("spend_minor"),
                 F.count("*").alias("txn_count"))
            .withColumn("window_kind", F.lit(which))
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .withColumn("vel_limit", F.lit(limit))
            .withColumn("limit_breached", F.col("spend_minor") > F.lit(limit))
            .drop("window"))


def make_velocity_sink(cfg: Dict[str, Any], which: str):
    """Delta for the parity read, Kafka signals for the fraud service."""
    velocity_path = cfg["paths"]["velocity"]
    bootstrap = cfg["kafka_bootstrap"]
    topic_signals = cfg["topics"]["signals"]

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df.persist()
        try:
            if batch_df.isEmpty():
                return
            rows = batch_df.withColumn("batch_id", F.lit(batch_id).cast("long"))

            # Append-only, like the integrity trail: update output mode re-emits a
            # window every time it changes, so the table holds a history of each
            # window's evolution. The parity read takes the LAST value per
            # (account, window_kind, window_start) — the value at the moment the
            # window closed.
            rows.write.format("delta").mode("append") \
                .partitionBy("window_kind").save(velocity_path)

            breached = rows.filter(F.col("limit_breached"))
            if not breached.isEmpty():
                payload = (breached
                           .withColumn("kind", F.lit("VELOCITY_BREACH"))
                           .select(F.col("account_id").cast("string").alias("key"),
                                   F.to_json(F.struct(
                                       "account_id", "kind", "window_kind",
                                       "window_start", "window_end", "spend_minor",
                                       "txn_count", "vel_limit", "batch_id"
                                   )).alias("value")))
                (payload.write.format("kafka")
                    .option("kafka.bootstrap.servers", bootstrap)
                    .option("topic", topic_signals)
                    .save())
        finally:
            batch_df.unpersist()

    return write_batch


def start_velocity_queries(events: DataFrame, cfg: Dict[str, Any],
                           checkpoint_for) -> List[Any]:
    """Start both window queries. Returns the StreamingQuery handles."""
    queries = []
    for which in (WINDOW_SHORT, WINDOW_LONG):
        frame = velocity_frame(events, cfg, which)
        q = (frame.writeStream
             .queryName(f"velocity_{which}")
             .outputMode("update")
             .foreachBatch(make_velocity_sink(cfg, which))
             .option("checkpointLocation", checkpoint_for(f"velocity_{which}"))
             .trigger(processingTime=cfg["trigger_interval"])
             .start())
        queries.append(q)
    return queries
