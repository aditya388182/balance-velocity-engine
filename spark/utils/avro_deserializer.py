from __future__ import annotations

import datetime as _dt
import io
import json
import struct
from typing import Dict

import fastavro
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

MAGIC_BYTE = 0

# The projection this UDF produces.
#
# event_ts_ms is carried as a LONG alongside the timestamp column on purpose: the
# pandas side of applyInPandasWithState needs raw epoch millis, and converting
# pandas Timestamps back to millis inside the operator is a reliable source of
# off-by-timezone bugs that surface as a buffer that never drains.
EVENT_STRUCT = StructType([
    StructField("account_id", StringType()),
    StructField("seq_no", LongType()),
    StructField("amount_minor", LongType()),
    StructField("event_type", StringType()),
    StructField("event_ts_ms", LongType()),
])

_SCHEMA_CACHE: Dict[int, dict] = {}
_SR_CLIENT = None


def _sr_client(url: str):
    """Lazy per-worker singleton. Executors cannot inherit a client from the driver."""
    global _SR_CLIENT
    if _SR_CLIENT is None:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        _SR_CLIENT = SchemaRegistryClient({"url": url})
    return _SR_CLIENT


def _writer_schema(schema_id: int, url: str) -> dict:
    if schema_id not in _SCHEMA_CACHE:
        registered = _sr_client(url).get_schema(schema_id)
        _SCHEMA_CACHE[schema_id] = fastavro.parse_schema(json.loads(registered.schema_str))
    return _SCHEMA_CACHE[schema_id]


def _to_millis(value) -> int:
    """fastavro decodes timestamp-millis into tz-aware datetimes; normalise back to millis."""
    if isinstance(value, _dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=_dt.timezone.utc)
        return int(value.timestamp() * 1000)
    return int(value)


def make_decoder(schema_registry_url: str):
    """Return a pandas UDF bound to a registry URL (captured so executors get it)."""

    @F.pandas_udf(EVENT_STRUCT)
    def decode(payloads: pd.Series) -> pd.DataFrame:
        account_id, seq_no, amount_minor, event_type, event_ts_ms = [], [], [], [], []
        for raw in payloads:
            if raw is None or len(raw) < 5:
                account_id.append(None)
                seq_no.append(None)
                amount_minor.append(None)
                event_type.append(None)
                event_ts_ms.append(None)
                continue
            buf = bytes(raw)
            magic, schema_id = struct.unpack(">bI", buf[:5])
            if magic != MAGIC_BYTE:
                raise ValueError(f"not Confluent wire format: magic byte {magic}")
            rec = fastavro.schemaless_reader(io.BytesIO(buf[5:]),
                                             _writer_schema(schema_id, schema_registry_url))
            account_id.append(rec["account_id"])
            seq_no.append(int(rec["seq_no"]))
            amount_minor.append(int(rec["amount_minor"]))
            event_type.append(str(rec["event_type"]))
            event_ts_ms.append(_to_millis(rec["event_ts"]))
        return pd.DataFrame({
            "account_id": account_id,
            "seq_no": pd.array(seq_no, dtype="Int64"),
            "amount_minor": pd.array(amount_minor, dtype="Int64"),
            "event_type": event_type,
            "event_ts_ms": pd.array(event_ts_ms, dtype="Int64"),
        })

    return decode


def deserialize_stream(raw_df, schema_registry_url: str):
    """Kafka source DF -> (account_id, seq_no, amount_minor, event_type, event_ts, event_ts_ms)."""
    decode = make_decoder(schema_registry_url)
    return (raw_df
            .select(decode(F.col("value")).alias("e"))
            .select("e.*")
            .filter(F.col("account_id").isNotNull())
            .withColumn("event_ts", (F.col("event_ts_ms") / 1000).cast("timestamp")))
