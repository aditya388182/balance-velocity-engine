#!/usr/bin/env bash
# scripts/run_engine.sh — start the engine, foreground or background.
#
# The background path writes run/engine.pid. Day 4's kill_engine.sh needs a
# reliable PID, and hunting for it at 09:45 on Thursday is worse than writing it
# today for free.
#
#   ./scripts/run_engine.sh --fg     run in the foreground (Day 1 default: you want to watch it)
#   ./scripts/run_engine.sh          run in the background, logging to logs/engine.log
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate
mkdir -p logs run

if [[ "${1:-}" == "--fg" ]]; then
  exec env PYTHONUNBUFFERED=1 python spark/jobs/balance_engine.py
fi

rm -f run/engine.pid
PYTHONUNBUFFERED=1 nohup python spark/jobs/balance_engine.py > logs/engine.log 2>&1 &
echo $! > run/engine.launcher.pid
echo "engine launching, launcher pid $(cat run/engine.launcher.pid)"
echo "follow with:  tail -f logs/engine.log"
