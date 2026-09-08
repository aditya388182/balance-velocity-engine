#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate
mkdir -p logs run

if [[ "${1:-}" == "--fg" ]]; then
  exec python spark/jobs/balance_engine.py
fi

rm -f run/engine.pid
nohup python spark/jobs/balance_engine.py > logs/engine.log 2>&1 &
echo $! > run/engine.launcher.pid
echo "engine launching, launcher pid $(cat run/engine.launcher.pid)"
echo "follow with:  tail -f logs/engine.log"
