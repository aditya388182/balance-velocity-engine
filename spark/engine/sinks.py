from __future__ import annotations

from typing import Any, Dict

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

BALANCE_KINDS = ("BALANCE", "TTL_FLUSH")


def make_foreach_batch(cfg: Dict[str, Any]):
    balances_path = cfg["paths"]["balances"]

    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        spark = batch_df.sparkSession
        batch_df.persist()
        try:
            balance_rows = batch_df.filter(F.col("out_kind").isin(list(BALANCE_KINDS)))
            if balance_rows.isEmpty():
                return

            # The operator emits one row per key per batch, so a second row for the
            # same account cannot occur — but a multi-match Delta MERGE is a hard
            # failure, and three lines of insurance costs less than the incident.
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
                return

            (DeltaTable.forPath(spark, balances_path).alias("t")
                .merge(latest.alias("s"), "t.account_id = s.account_id")
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
        finally:
            batch_df.unpersist()

    return write_batch
