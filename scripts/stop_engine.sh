#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -f run/engine.pid ]]; then
  PID="$(cat run/engine.pid)"
  if kill -0 "$PID" 2>/dev/null; then
    echo "==> SIGTERM to engine pid $PID"
    kill -TERM "$PID" 2>/dev/null || true
    for _ in $(seq 1 30); do kill -0 "$PID" 2>/dev/null || break; sleep 1; done
    kill -0 "$PID" 2>/dev/null && { echo "    still alive, SIGKILL"; kill -9 "$PID"; } || true
  fi
  rm -f run/engine.pid
fi
pkill -f "spark/jobs/balance_engine.py" 2>/dev/null || true
rm -f run/engine.launcher.pid
echo "==> engine stopped"
