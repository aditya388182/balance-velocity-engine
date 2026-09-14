#!/usr/bin/env bash
# scripts/sink_replay_drill.sh — demonstrate exactly-once LAYER 3, deterministically.
#
# WHY A SEPARATE DRILL
# --------------------
# recovery_drill.sh proves layers 1 and 2: state and Kafka offsets restore
# atomically, so the balance still equals the oracle after a SIGKILL. It cannot
# reliably prove layer 3, because the window in which foreachBatch has WRITTEN but
# the batch is UNCOMMITTED is milliseconds wide — commits/N is written the moment
# foreachBatch returns. Three SIGKILL attempts found it zero times, which is the
# expected outcome, not a defect.
#
# So this drill manufactures it. P3_SINK_FAIL_ONCE=1 makes the sink complete all of
# its writes and then raise, once. The query dies with the batch uncommitted; the
# restart re-executes it from offsets/N and writes the same rows again.
#
# That is the exact scenario the guards exist for, and the same discipline as Day 6's
# corrupt_checkpoint.sh: if the disaster is too rare to wait for, cause it.
#
#   ./scripts/sink_replay_drill.sh
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
PYTHONUNBUFFERED=1 P3_SINK_FAIL_ONCE=1 nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
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
PYTHONUNBUFFERED=1 nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
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
