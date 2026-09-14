from __future__ import annotations

from pyspark.sql.types import (BinaryType, IntegerType, LongType, StringType,
                               StructField, StructType)

from spark.engine.statecore import (EMPTY_BUFFER_BYTES, KIND_BALANCE, KIND_DUP,  # noqa: F401
                                    KIND_GAP, KIND_OVERFLOW, KIND_TTL,
                                    OUTPUT_COLUMNS, PICKLE_PROTOCOL, empty_state,
                                    normalise_events, pack_buffer, state_to_tuple,
                                    tuple_to_parts, unpack_buffer)

# Field order MUST match statecore.state_to_tuple positionally.
STATE_SCHEMA = StructType([
    StructField("last_applied_seq", LongType()),
    StructField("balance_minor", LongType()),
    StructField("pending_buffer", BinaryType()),   # pickled {int seq: (int amount, int event_ts_ms)}
    StructField("buffer_size", IntegerType()),
    StructField("last_seen_ms", LongType()),
])

OUTPUT_SCHEMA = StructType([
    StructField("account_id", StringType()),
    StructField("last_applied_seq", LongType()),
    StructField("balance_minor", LongType()),
    StructField("buffer_size", IntegerType()),
    StructField("out_kind", StringType()),         # BALANCE|SEQUENCE_GAP|DUP_DROPPED|BUFFER_OVERFLOW|TTL_FLUSH
    StructField("detail", StringType()),           # JSON, or null
])

# Pandas dtypes matching OUTPUT_SCHEMA exactly. Handing Arrow an int64 where the
# schema says IntegerType produces a cryptic java.lang.IllegalStateException from
# deep inside the Arrow writer, with nothing in the message pointing at the column.
# Casting at the boundary is cheaper than reading that stack trace.
OUTPUT_DTYPES = {
    "account_id": "object",
    "last_applied_seq": "int64",
    "balance_minor": "int64",
    "buffer_size": "int32",
    "out_kind": "object",
    "detail": "object",
}

assert [f.name for f in OUTPUT_SCHEMA.fields] == OUTPUT_COLUMNS, \
    "OUTPUT_SCHEMA and statecore.OUTPUT_COLUMNS have drifted apart"
assert list(OUTPUT_DTYPES) == OUTPUT_COLUMNS, \
    "OUTPUT_DTYPES and OUTPUT_COLUMNS have drifted apart"
