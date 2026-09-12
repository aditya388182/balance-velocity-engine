#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

GEN_ARGS="--accounts 3 --rate 40 --duration 45 --shuffle-window 20 \
--dup 50 --dup 100 --dup 150 --dup 200 --dup 250 --dup 300 --seed 7"
DRAIN=60
while [[ $# -gt 0 ]]; do
  case "$1" in
    --gen)   GEN_ARGS="$2"; shift 2 ;;
    --drain) DRAIN="$2"; shift 2 ;;
    -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
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

wait_ready () {
  for _ in $(seq 1 90); do
    [[ -f run/engine.pid ]] && return 0
    sleep 2
  done
  echo "engine never became ready" >&2; tail -30 logs/engine.log >&2; return 1
}

echo "==> reset"
stop_engine
./scripts/reset_lake.sh >/dev/null
rm -f run/sink_failed_once
: > logs/engine.log; : > logs/progress.jsonl

echo "==> lifetime 1: engine WITH the one-shot sink failure armed"
P3_SINK_FAIL_ONCE=1 nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
wait_ready
echo "    pid $(cat run/engine.pid), P3_SINK_FAIL_ONCE=1"

echo "==> waiting for the first committed batch"
for _ in $(seq 1 60); do [[ -s logs/progress.jsonl ]] && break; sleep 2; done

echo "==> publish: $GEN_ARGS"
# shellcheck disable=SC2086
python scripts/event_generator.py $GEN_ARGS

echo "==> waiting for the deliberate sink failure"
FAILED=0
for _ in $(seq 1 60); do
  if [[ -f run/sink_failed_once ]]; then FAILED=1; break; fi
  sleep 2
done
if [[ "$FAILED" -eq 1 ]]; then
  echo "    sink raised after writing batch $(cat run/sink_failed_once)"
else
  echo "    WARNING: the sink never failed — was P3_SINK_FAIL_ONCE picked up?" >&2
fi
sleep 10
stop_engine

echo "==> lifetime 2: restart WITHOUT the failure armed; batch re-executes"
nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
wait_ready
echo "==> draining ${DRAIN}s"
sleep "$DRAIN"
stop_engine
trap - EXIT

echo "==> parity"
OK=1
python scripts/parity_balance.py --expect-empty-buffer || OK=0

echo "==> replay evidence"
python scripts/replay_evidence.py --require-sink-replay || OK=0

if [[ "$OK" -eq 1 ]]; then
  echo "==> SINK REPLAY DRILL: PASS — layer 3 demonstrated, not argued"
  exit 0
fi
echo "==> SINK REPLAY DRILL: FAIL" >&2
exit 1
