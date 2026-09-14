#!/usr/bin/env bash
# scripts/stage6_proof.sh — Stage 6: TTL eviction and the rejoin round trip.
#
#   accounts arrive -> state rows climb -> traffic stops -> event time keeps moving
#   -> state rows FALL as idle keys are released -> one account returns and its
#   balance CONTINUES instead of restarting from zero
#
# THREE THINGS THE FIRST VERSION GOT WRONG
# ----------------------------------------
# 1. It ran velocity alongside. Two extra windowed aggregations over the same
#    stream on local[4] pushed batches to ~33s against a 5s trigger, so the engine
#    never caught up: the tick events were never consumed, the watermark never
#    passed the TTL, and the injected event was never read. Velocity is not under
#    test here, so it is switched off. Isolating the variable is a measurement
#    decision, not a production one.
#
# 2. It used blind sleeps. A sleep asserts a duration; the drill needs a condition.
#    Every phase now waits for the query to actually drain, and fails loudly with
#    the batch durations if it does not.
#
# 3. The load was too heavy to observe anything. 1000 accounts x rate 200 says
#    nothing that 300 x rate 100 does not, and the smaller run finishes.
#
#   ./scripts/stage6_proof.sh
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ACCOUNTS=300
RATE=100
DURATION=45
TICK=240
while [[ $# -gt 0 ]]; do
  case "$1" in
    --accounts) ACCOUNTS="$2"; shift 2 ;;
    --rate)     RATE="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --tick)     TICK="$2"; shift 2 ;;
    -h|--help)  sed -n '2,26p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate

BUILD="$(cat "$REPO_ROOT/BUILD" 2>/dev/null || echo "UNKNOWN")"
echo "### $(basename "$0")  build ${BUILD}"
if [[ "$BUILD" == "UNKNOWN" ]]; then
  echo "### WARNING: no BUILD file — this tree predates build tagging" >&2
fi

mkdir -p logs run

stop_engine () {
  [[ -f run/engine.pid ]] && { kill -TERM "$(cat run/engine.pid)" 2>/dev/null || true; sleep 3; }
  pkill -f "spark/jobs/balance_engine.py" 2>/dev/null || true
  rm -f run/engine.pid run/engine.launcher.pid
}
trap stop_engine EXIT

start_and_wait () {
  rm -f run/engine.pid
  # Velocity OFF: this drill measures TTL, and the extra queries make it measure
  # resource contention instead.
  PYTHONUNBUFFERED=1 P3_VELOCITY_ENABLED=0 nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
  for _ in $(seq 1 90); do [[ -f run/engine.pid ]] && break; sleep 2; done
  [[ -f run/engine.pid ]] || { echo "engine never became ready" >&2; tail -30 logs/engine.log >&2; exit 1; }
  for _ in $(seq 1 60); do [[ -s logs/progress.jsonl ]] && return 0; sleep 2; done
  echo "    WARNING: no batch committed yet" >&2
}

drain () {
  # stable-seconds 25 == five triggers of silence. Spark skips a trigger when there
  # is nothing to do, so silence IS the drained signal; the offset check short-
  # circuits this whenever the progress file carries source offsets.
  python scripts/wait_for_drain.py --query balance_engine \
         --timeout "${1:-300}" --stable-seconds 25
}

# A stale helper produces a failure that looks like an engine problem. Four drills
# have now been lost to that, so this refuses to start rather than find out late.
if ! python scripts/check_install.py >/dev/null 2>&1; then
  echo "" >&2
  echo "INSTALL IS STALE OR INCOMPLETE — not running the drill." >&2
  python scripts/check_install.py >&2 || true
  exit 2
fi

echo "==> reset"
stop_engine
./scripts/reset_lake.sh >/dev/null
: > logs/engine.log; : > logs/progress.jsonl; : > logs/metrics.jsonl

echo "==> start engine (velocity disabled — TTL is the variable under test)"
start_and_wait
grep -E "velocity|rejoin|balances" logs/engine.log || true

echo "==> ${ACCOUNTS} accounts, rate ${RATE}, ${DURATION}s"
python scripts/event_generator.py --accounts "$ACCOUNTS" --rate "$RATE" \
       --duration "$DURATION" --seed 5 >/dev/null
echo "==> waiting for the engine to consume it"
drain 300

echo "==> snapshot ACCT-0001 before it goes idle"
python scripts/rejoin_proof.py --account ACCT-0001 --snapshot logs/before_ttl.json

echo "==> idling: the tick run advances EVENT time so the TTL can be reached"
echo "    (a frozen watermark can never pass last_seen + TTL, however long you wait)"
python scripts/event_generator.py --accounts 1 --account-prefix TICK- \
       --rate 2 --duration "$TICK" --heartbeat-account --seed 3 >/dev/null
drain 300

OK=1
echo "==> the eviction curve (sequencer only — progress.jsonl holds every query)"
python scripts/state_growth.py --check logs/progress.jsonl \
       --query balance_engine --expect-eviction || OK=0

echo "==> ACCT-0001 returns — one event, continuing its sequence"
BEFORE_SEQ=$(python -c "import json;print(json.load(open('logs/before_ttl.json'))['last_applied_seq'])")
NEXT_SEQ=$((BEFORE_SEQ + 1))
python scripts/inject_burst.py --account ACCT-0001 --events 1 --start-seq "$NEXT_SEQ" \
       --amount -4500 --run-id rejoin_probe

# A single event must still produce a NEW batch. The waiter refuses to answer from
# the batch that ran before the publish, which is how this returned in 0s and let
# the proof run against an engine that had not read the event.
if ! drain 180; then
  echo "" >&2
  echo "the rejoin event was never consumed — tracing it before giving up:" >&2
  python scripts/trace_event.py --account ACCT-0001 --seq "$NEXT_SEQ" >&2 || true
  OK=0
fi
stop_engine
trap - EXIT

echo "==> rejoin proof"
python scripts/rejoin_proof.py --account ACCT-0001 --compare logs/before_ttl.json \
       --expect-delta -4500 || OK=0

[[ "$OK" -eq 1 ]] && { echo "==> STAGE 6 PROOF: PASS"; exit 0; }
echo "==> STAGE 6 PROOF: FAIL" >&2; exit 1
