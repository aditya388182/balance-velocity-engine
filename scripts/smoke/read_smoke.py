#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from conf.config import CFG  # noqa: E402
from spark.utils.avro_deserializer import deserialize_stream  # noqa: E402
from spark.utils.session import assert_state_store_configured, build_spark  # noqa: E402


def main() -> None:
    spark = build_spark(CFG, "read-smoke", streaming=True)
    assert_state_store_configured(spark)

    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", CFG["kafka_bootstrap"])
           .option("subscribe", CFG["topics"]["events"])
           .option("startingOffsets", "earliest")
           .load())
    n_raw = raw.count()
    print(f"raw kafka rows: {n_raw}")
    if n_raw == 0:
        print("nothing on the topic — run the generator first")
        spark.stop()
        sys.exit(1)

    events = deserialize_stream(raw, CFG["schema_registry_url"])
    events.orderBy("account_id", "seq_no").show(5, truncate=False)
    events.printSchema()
    print(f"decoded rows  : {events.count()}")
    print(f"accounts      : {sorted(r[0] for r in events.select('account_id').distinct().collect())}")

    # A cheap but meaningful assertion: per account, seq_no must start at 1 and be dense.
    import pyspark.sql.functions as F
    agg = (events.groupBy("account_id")
           .agg(F.min("seq_no").alias("lo"), F.max("seq_no").alias("hi"),
                F.countDistinct("seq_no").alias("n"))
           .collect())
    for r in agg:
        dense = (r["lo"] == 1 and r["hi"] == r["n"])
        print(f"  {r['account_id']}: seq {r['lo']}..{r['hi']} distinct={r['n']} "
              f"{'dense ✓' if dense else 'NOT DENSE ✗'}")

    spark.stop()


if __name__ == "__main__":
    main()
