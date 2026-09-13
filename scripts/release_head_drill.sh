#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

EVENTS=10000
TICK_DURATION=90
RUN_ID="burst_release"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --events) EVENTS="$2"; shift 2 ;;
    --tick)   TICK_DURATION="$2"; shift 2 ;;
    -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate
mkdir -p logs run

# Fail fast rather than half-run. A missing helper five minutes in wastes the
# whole drill and reports FAIL for a reason that has nothing to do with the engine.
MISSING=0
for f in scripts/inject_burst.py scripts/buffer_proof.py scripts/late_arrival_proof.py \
         scripts/parity_balance.py scripts/event_generator.py scripts/reset_lake.sh; do
  [[ -f "$f" ]] || { echo "MISSING: $f" >&2; MISSING=1; }
done
if [[ "$MISSING" -eq 1 ]]; then
  echo "" >&2
  echo "Install the Day-4 corrections before running this drill:" >&2
  echo "  unzip -o ~/balance-velocity-engine-day4-releasehead.zip" >&2
  echo "  chmod +x scripts/*.py scripts/*.sh" >&2
  exit 2
fi

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
  # and wait for a COMMITTED batch, not merely a live process
  local before; before=$(wc -l < logs/progress.jsonl 2>/dev/null || echo 0)
  for _ in $(seq 1 60); do
    [[ "$(wc -l < logs/progress.jsonl 2>/dev/null || echo 0)" -gt "$before" ]] && return 0
    sleep 2
  done
  echo "    WARNING: no new batch committed — events may not be consumed" >&2
}

LOG="delivery_log_${RUN_ID}.jsonl"

echo "==> reset (a CAPPED run — the demo is meaningless after an uncapped one)"
stop_engine
./scripts/reset_lake.sh >/dev/null
: > logs/engine.log; : > logs/progress.jsonl

echo "==> start engine"
start_and_wait

echo "==> burst: ${EVENTS} events, head (seq 1) withheld"
python scripts/inject_burst.py --account HOT-1 --events "$EVENTS" --start-seq 2 \
       --never-send 1 --event-time-step-ms 2 --seed 99 --run-id "$RUN_ID"
sleep 30

echo "==> pushing event time forward so the gap is CONFIRMED"
python scripts/event_generator.py --accounts 1 --account-prefix TICK- \
       --rate 2 --duration "$TICK_DURATION" --heartbeat-account --seed 3 >/dev/null
sleep 45

echo "==> full parity BEFORE the release, while the oracle can still model the run"
# Ordering matters. With the head withheld the oracle is DETERMINATE: min-first
# eviction retains the k largest, so it predicts the survivors exactly. The moment
# seq 1 is appended, head_withheld becomes false, the oracle switches to its
# "appliable head" branch, and its balance is computed as though nothing had been
# evicted. Comparing after the append turns the oracle's own admission of
# uncertainty into a FAIL. So compare first, release second.
OK=1
python scripts/parity_balance.py --delivery-log "$LOG" --only-account HOT-1 || OK=0

echo "==> snapshot BEFORE releasing the head"
python scripts/buffer_proof.py --account HOT-1 --mode capped --events "$EVENTS" \
       --save logs/proof_before_release.json
BEFORE_BAL=$(python -c "import json;print(json.load(open('logs/proof_before_release.json'))['balance_minor'])")
BEFORE_LAST=$(python -c "import json;print(json.load(open('logs/proof_before_release.json'))['last_applied_seq'])")
echo "    balance=${BEFORE_BAL}  last_applied_seq=${BEFORE_LAST}"

echo "==> releasing the head: seq 1, APPENDED to the same delivery log"
python scripts/inject_burst.py --account HOT-1 --events 1 --start-seq 1 \
       --append-to "$LOG"

echo "==> draining"
sleep 45
stop_engine
trap - EXIT

echo "==> late-arrival proof (the oracle cannot model a post-gap arrival; this can)"
python scripts/late_arrival_proof.py --account HOT-1 --seq 1 \
       --expect-balance "$BEFORE_BAL" --expect-last "$BEFORE_LAST" \
       --save logs/proof_release_head.json || OK=0

[[ "$OK" -eq 1 ]] && { echo "==> RELEASE HEAD DRILL: PASS"; exit 0; }
echo "==> RELEASE HEAD DRILL: FAIL" >&2; exit 1
