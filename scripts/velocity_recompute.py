#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.window import Window  # noqa: E402

from conf.config import CFG  # noqa: E402
from spark.utils.avro_deserializer import deserialize_stream  # noqa: E402
from spark.utils.session import build_spark  # noqa: E402

GREEN, RED, YELLOW, DIM, RESET = "\033[92m", "\033[91m", "\033[93m", "\033[2m", "\033[0m"


def window_params(cfg, which):
    if which == "short":
        return cfg["vel_window"], cfg["vel_slide"], int(cfg["vel_limit"])
    return (cfg.get("vel_window_long", cfg["vel_window"]),
            cfg.get("vel_slide_long", cfg["vel_slide"]),
            int(cfg["vel_limit"]) * 5)


def main() -> None:
    p = argparse.ArgumentParser(description="Stage 4 velocity parity")
    p.add_argument("--window-kind", choices=["short", "long", "both"], default="both")
    p.add_argument("--max-open-windows-skipped", type=int, default=10_000)
    args = p.parse_args()

    spark = build_spark(CFG, app_name="velocity-recompute", streaming=True)
    failures = []
    try:
        #  the batch side: the SAME records, bounded offsets 
        raw = (spark.read.format("kafka")
               .option("kafka.bootstrap.servers", CFG["kafka_bootstrap"])
               .option("subscribe", CFG["topics"]["events"])
               .option("startingOffsets", "earliest")
               .option("endingOffsets", "latest")
               .load())
        events = deserialize_stream(raw, CFG["schema_registry_url"])
        events.persist()
        n_events = events.count()
        print(f"batch events read : {n_events}")
        if n_events == 0:
            print(f"{RED}nothing on the topic — run the engine and generator first{RESET}")
            sys.exit(1)

        try:
            stream_all = spark.read.format("delta").load(CFG["paths"]["velocity"])
        except Exception:
            print(f"{RED}the velocity Delta table does not exist{RESET}")
            print(f"  {DIM}the velocity queries never wrote — check logs/engine.log for "
                  f"'velocity_short' and 'velocity_long'{RESET}")
            sys.exit(1)

        kinds = ["short", "long"] if args.window_kind == "both" else [args.window_kind]
        for which in kinds:
            size, slide, limit = window_params(CFG, which)
            print()
            print(f"--- window '{which}': size={size} slide={slide} limit={limit} ---")

            batch = (events
                     .filter(F.col("event_type") == "DEBIT")
                     .groupBy(F.col("account_id"),
                              F.window(F.col("event_ts"), size, slide))
                     .agg(F.sum(F.abs(F.col("amount_minor"))).alias("batch_spend"),
                          F.count("*").alias("batch_txns"))
                     .select("account_id",
                             F.col("window.start").alias("window_start"),
                             F.col("window.end").alias("window_end"),
                             "batch_spend", "batch_txns"))

            # update mode re-emits a window whenever it changes, so take the LAST
            # row the stream wrote for each window.
            w = (Window.partitionBy("account_id", "window_start")
                 .orderBy(F.col("batch_id").desc()))
            stream = (stream_all
                      .filter(F.col("window_kind") == which)
                      .withColumn("_rn", F.row_number().over(w))
                      .filter(F.col("_rn") == 1)
                      .select("account_id", "window_start", "window_end",
                              F.col("spend_minor").alias("stream_spend"),
                              F.col("txn_count").alias("stream_txns")))

            joined = stream.join(batch, ["account_id", "window_start", "window_end"],
                                 "full_outer")

            # A window is SETTLED once the whole stream has moved past its end.
            # max(event_ts) is the frontier the engine ever saw.
            frontier = events.select(F.max("event_ts")).first()[0]
            settled = joined.filter(F.col("window_end") <= F.lit(frontier))
            open_windows = joined.count() - settled.count()

            total = settled.count()
            mismatched = settled.filter(
                (F.col("stream_spend").isNull())
                | (F.col("batch_spend").isNull())
                | (F.col("stream_spend") != F.col("batch_spend"))
                | (F.col("stream_txns") != F.col("batch_txns")))
            n_bad = mismatched.count()

            print(f"settled windows   : {total}")
            print(f"open windows      : {open_windows}  "
                  f"{DIM}(excluded — a partial sum is not a disagreement){RESET}")
            print(f"mismatched        : {n_bad}")

            if open_windows > args.max_open_windows_skipped:
                failures.append(f"{which}: {open_windows} open windows skipped — "
                                f"the run may not have drained")
            if total == 0:
                failures.append(f"{which}: no settled windows to compare — let the "
                                f"stream run past at least one full window")
            if n_bad:
                mismatched.orderBy("account_id", "window_start").show(10, truncate=False)
                failures.append(f"{which}: {n_bad} settled window(s) disagree")
            else:
                print(f"{GREEN}EXACT MATCH{RESET} — native windowed aggregation == "
                      f"independent batch recomputation on every settled window")
        events.unpersist()
    finally:
        spark.stop()

    print()
    print("=" * 78)
    if failures:
        print(f"{RED}VELOCITY PARITY FAIL{RESET}")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print(f"{GREEN}VELOCITY PARITY PASS{RESET} — the native path needed no custom state, "
          f"and it is right.")
    sys.exit(0)


if __name__ == "__main__":
    main()
