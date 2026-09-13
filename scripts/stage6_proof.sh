#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ACCOUNTS=1000
RATE=200
DURATION=60
TICK=200
while [[ $# -gt 0 ]]; do
  case "$1" in
    --accounts) ACCOUNTS="$2"; shift 2 ;;
    --rate)     RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --tick)     TICK="$2"; shift 2 ;;
    -h|--help)  sed -n '2,18p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate
mkdir -p logs run

stop_engine () {
  [[ -f run/engine.pid ]] && { kill -TERM "$(cat run/engine.pid)" 2>/dev/null || true; sleep 3; }
  pkill -f "spark/jobs/balance_engine.py" 2>/dev/null || true
  rm -f run/engine.pid run/engine.launcher.pid
}
trap stop_engine EXIT

start_and_wait () {
  rm -f run/engine.pid
  nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
  for _ in $(seq 1 90); do [[ -f run/engine.pid ]] && break; sleep 2; done
  [[ -f run/engine.pid ]] || { echo "engine never became ready" >&2; tail -30 logs/engine.log >&2; exit 1; }
  for _ in $(seq 1 60); do [[ -s logs/progress.jsonl ]] && return 0; sleep 2; done
  echo "    WARNING: no batch committed yet" >&2
}

echo "==> reset"
stop_engine
./scripts/reset_lake.sh >/dev/null
: > logs/engine.log; : > logs/progress.jsonl; : > logs/metrics.jsonl

echo "==> start engine"
start_and_wait
grep -m1 "state_ttl\|rejoin" logs/engine.log || true

echo "==> ${ACCOUNTS} accounts, rate ${RATE}, ${DURATION}s"
python scripts/event_generator.py --accounts "$ACCOUNTS" --rate "$RATE" \
       --duration "$DURATION" --seed 5 >/dev/null
sleep 30

echo "==> snapshot ACCT-0001 before it goes idle"
python scripts/rejoin_proof.py --account ACCT-0001 --snapshot logs/before_ttl.json

echo "==> idling: the tick run advances EVENT time so the TTL can be reached"
python scripts/event_generator.py --accounts 1 --account-prefix TICK- \
       --rate 2 --duration "$TICK" --heartbeat-account --seed 3 >/dev/null
sleep 45

echo "==> the eviction curve"
OK=1
python scripts/state_growth.py --check logs/progress.jsonl --expect-eviction || OK=0

echo "==> ACCT-0001 returns — one event, continuing its sequence"
BEFORE_SEQ=$(python -c "import json;print(json.load(open('logs/before_ttl.json'))['last_applied_seq'])")
NEXT_SEQ=$((BEFORE_SEQ + 1))
python scripts/inject_burst.py --account ACCT-0001 --events 1 --start-seq "$NEXT_SEQ" \
       --amount -4500 --run-id rejoin_probe
sleep 45
stop_engine
trap - EXIT

echo "==> rejoin proof"
python scripts/rejoin_proof.py --account ACCT-0001 --compare logs/before_ttl.json \
       --expect-delta -4500 || OK=0

[[ "$OK" -eq 1 ]] && { echo "==> STAGE 6 PROOF: PASS"; exit 0; }
echo "==> STAGE 6 PROOF: FAIL" >&2; exit 1
