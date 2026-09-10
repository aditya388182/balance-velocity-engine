#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"
mkdir -p logs run

PID=""
if [[ -f run/engine.pid ]]; then PID="$(cat run/engine.pid)"; fi
if [[ -z "$PID" ]] || ! kill -0 "$PID" 2>/dev/null; then
  PID="$(pgrep -f 'spark/jobs/balance_engine.py' | head -1 || true)"
fi
if [[ -z "$PID" ]]; then
  echo "ERROR: no running engine found (run/engine.pid absent and no matching process)" >&2
  exit 1
fi

LAST_BATCH="$(python3 - <<'PY' 2>/dev/null || echo "unknown"
import json, pathlib
p = pathlib.Path("logs/progress.jsonl")
ids = [json.loads(l).get("batch_id") for l in p.read_text().splitlines() if l.strip()] if p.exists() else []
ids = [i for i in ids if i is not None]
print(max(ids) if ids else "none")
PY
)"

KILL_TS="$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ 2>/dev/null || date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "==> SIGKILL pid=$PID at $KILL_TS (last committed batch: $LAST_BATCH)"
kill -9 "$PID" 2>/dev/null || true
for _ in $(seq 1 20); do kill -0 "$PID" 2>/dev/null || break; sleep 0.5; done
pkill -9 -f 'spark/jobs/balance_engine.py' 2>/dev/null || true
rm -f run/engine.pid run/engine.launcher.pid

printf '%s\n' "{\"event\":\"SIGKILL\",\"pid\":$PID,\"wall\":\"$KILL_TS\",\"last_committed_batch\":\"$LAST_BATCH\"}" \
  >> logs/chaos.jsonl
echo "==> killed. Recorded in logs/chaos.jsonl"
