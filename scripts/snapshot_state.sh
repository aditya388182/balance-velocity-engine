#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
MC="${MC_CONTAINER:-p3-mc}"

TAG="$(python3 -c "from conf.config import CFG; print(CFG['spark_version_tag'])")"
SRC="local/balance-lake/checkpoints/${TAG}/"
DEST_ROOT="local/state-snapshots/balance_engine"

if [[ "${1:-}" == "--list" ]]; then
  docker exec "$MC" mc ls "$DEST_ROOT/" | tail -10
  exit 0
fi

TS="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="${DEST_ROOT}/${TS}/"

echo "==> snapshotting ${SRC} -> ${DEST}"
docker exec "$MC" mc mirror --overwrite "$SRC" "$DEST" >/dev/null 2>&1 || {
  echo "ERROR: nothing to snapshot — has the engine written a checkpoint yet?" >&2
  exit 1
}
SIZE="$(docker exec "$MC" mc du "$DEST" 2>/dev/null | tail -1 || echo "?")"
echo "    done: ${SIZE}"

echo "==> newest snapshots:"
docker exec "$MC" mc ls "$DEST_ROOT/" | tail -5

# snapshot_lag_seconds is 0 the instant a snapshot lands; the DAG pushes the
# growing value between runs.
python3 - <<PY
import sys; sys.path.insert(0, ".")
from spark.engine.metrics import emit, METRIC_SNAPSHOT_LAG
emit({METRIC_SNAPSHOT_LAG: 0, "snapshot_ts": "${TS}"}, job="state_snapshot")
print("    snapshot_lag_seconds = 0")
PY
