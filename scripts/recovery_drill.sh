#!/usr/bin/env bash
# scripts/recovery_drill.sh — the exactly-once recovery drill, one command.
#
#   reset -> start engine -> publish -> SIGKILL mid-stream -> restart -> drain
#         -> parity -> replay evidence
#
# WHAT MUST HOLD, AND WHY (the log paragraph writes itself from this chain)
# ------------------------------------------------------------------------
# RocksDB state and Kafka offsets live in the SAME checkpoint and restore
# atomically, so the restarted job resumes with last_applied_seq exactly
# consistent with the offsets it will re-read. Batch N re-executes against state
# version N-1 and produces the same result. The replayed foreachBatch body hits
# the strict-> MERGE guard as a no-op. Three independent layers said no.
#
# Note what this does NOT mean: during a clean recovery the state machine's
# duplicate branch does not engage, because the state rolled back with the
# offsets. Expecting recovery-caused DUP_DROPPED records is a misreading of how
# Spark checkpoints — see scripts/replay_evidence.py.
#
#   ./scripts/recovery_drill.sh --gen "--accounts 5 --rate 40 --duration 90 --shuffle-window 20 --seed 7"
#
# Flags:
#   --gen        "..."  generator args (required)
#   --parity     "..."  extra parity args
#   --kill-after N      seconds after publishing starts before the SIGKILL (default 25)
#   --drain      N      seconds to settle after restart (default 60)
#   --attempts   N      retry the whole drill until a SINK replay is observed (default 1)
#   --no-reset          keep the existing lake/topics
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

GEN_ARGS=""; PARITY_ARGS=""; KILL_AFTER=25; DRAIN=60; ATTEMPTS=1; DO_RESET=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --gen)        GEN_ARGS="$2"; shift 2 ;;
    --parity)     PARITY_ARGS="$2"; shift 2 ;;
    --kill-after) KILL_AFTER="$2"; shift 2 ;;
    --drain)      DRAIN="$2"; shift 2 ;;
    --attempts)   ATTEMPTS="$2"; shift 2 ;;
    --no-reset)   DO_RESET=0; shift ;;
    -h|--help)    sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done
[[ -z "$GEN_ARGS" ]] && { echo "--gen is required" >&2; exit 2; }

# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate

BUILD="$(cat "$REPO_ROOT/BUILD" 2>/dev/null || echo "UNKNOWN")"
echo "### $(basename "$0")  build ${BUILD}"
if [[ "$BUILD" == "UNKNOWN" ]]; then
  echo "### WARNING: no BUILD file — this tree predates build tagging" >&2
fi

mkdir -p logs run

start_engine () {
  rm -f run/engine.pid
  PYTHONUNBUFFERED=1 nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
  local launcher=$!
  for _ in $(seq 1 90); do
    [[ -f run/engine.pid ]] && return 0
    kill -0 "$launcher" 2>/dev/null || { echo "engine died on startup" >&2; tail -30 logs/engine.log >&2; return 1; }
    sleep 2
  done
  echo "engine never became ready" >&2; return 1
}

stop_engine () {
  [[ -f run/engine.pid ]] && { kill -TERM "$(cat run/engine.pid)" 2>/dev/null || true; sleep 3; }
  pkill -f "spark/jobs/balance_engine.py" 2>/dev/null || true
  rm -f run/engine.pid run/engine.launcher.pid
}
trap stop_engine EXIT

OVERALL=1
for attempt in $(seq 1 "$ATTEMPTS"); do
  echo "############ RECOVERY DRILL — attempt ${attempt}/${ATTEMPTS} ############"
  stop_engine
  if [[ "$DO_RESET" -eq 1 ]]; then
    echo "==> reset"; ./scripts/reset_lake.sh >/dev/null
  fi
  # progress.jsonl is NOT truncated between the two engine lifetimes: the timeline
  # across the restart is part of the evidence.
  : > logs/engine.log; : > logs/progress.jsonl

  echo "==> start engine (lifetime 1)"; start_engine
  # Wait for the engine to COMMIT a batch, not merely to exist. The pid file is
  # written when query.start() returns; the first micro-batch can be a further
  # 30-60s away on a cold JVM. Publishing before then means the SIGKILL lands
  # while almost nothing has been consumed, which is why "last committed batch: 0"
  # keeps appearing and why no replay is ever observed.
  echo "==> waiting for the first committed batch"
  for _ in $(seq 1 60); do
    [[ -s logs/progress.jsonl ]] && break
    sleep 2
  done
  if [[ -s logs/progress.jsonl ]]; then
    echo "    first batch committed"
  else
    echo "    WARNING: no batch committed within 120s — the kill may land too early"
  fi

  echo "==> publish (background): $GEN_ARGS"
  # shellcheck disable=SC2086
  python scripts/event_generator.py $GEN_ARGS > logs/generator.log 2>&1 &
  GEN_PID=$!

  echo "==> waiting ${KILL_AFTER}s, then SIGKILL mid-stream"
  sleep "$KILL_AFTER"
  ./scripts/chaos/kill_engine.sh

  echo "==> letting the generator finish publishing into a DEAD engine"
  wait "$GEN_PID" || true

  echo "==> restart engine (lifetime 2)"; start_engine
  echo "==> draining ${DRAIN}s"; sleep "$DRAIN"
  stop_engine

  echo "==> parity"
  ATTEMPT_OK=1
  # shellcheck disable=SC2086
  python scripts/parity_balance.py $PARITY_ARGS || ATTEMPT_OK=0

  echo "==> replay evidence"
  if python scripts/replay_evidence.py --require-sink-replay; then
    SINK_REPLAY=1
  else
    SINK_REPLAY=0
  fi

  if [[ "$ATTEMPT_OK" -eq 0 ]]; then
    echo "==> DRILL RESULT: FAIL (parity)" >&2
    exit 1
  fi
  if [[ "$SINK_REPLAY" -eq 1 ]]; then
    echo "==> DRILL RESULT: PASS (parity + sink replay observed)"
    OVERALL=0
    break
  fi
  echo "==> parity PASS but no sink replay observed on this attempt"
  OVERALL=0
done

if [[ "$OVERALL" -eq 0 ]]; then
  echo "==> DRILL COMPLETE"
  exit 0
fi
exit 1
