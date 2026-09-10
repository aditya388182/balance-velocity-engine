#!/usr/bin/env bash
#   reset -> start engine -> wait ready -> publish -> drain -> stop engine -> parity
#
#   ./scripts/stage_run.sh --gen "--accounts 3 --ordered --rate 30 --duration 60 --seed 7" \
#                          --parity "--expect-empty-buffer" --drain 45
#
# Flags:
#   --gen     "..."   passthrough args for event_generator.py   (required)
#   --parity  "..."   passthrough args for parity_balance.py    (default: none)
#   --drain   N       seconds to let the engine settle after the generator ends (default 45)
#   --no-reset        keep the existing lake/topics
#   --no-parity       stop before parity (mechanism-only exercises, e.g. Block 2.5)
#   --probe   "..."   run gap_timing_probe.py after parity with these args
#   --keep-running    leave the engine up after the run
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

GEN_ARGS=""
PARITY_ARGS=""
PROBE_ARGS=""
RUN_PROBE=0
DRAIN=45
DO_RESET=1
DO_PARITY=1
KEEP_RUNNING=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gen)          GEN_ARGS="$2"; shift 2 ;;
    --parity)       PARITY_ARGS="$2"; shift 2 ;;
    --probe)        PROBE_ARGS="$2"; RUN_PROBE=1; shift 2 ;;
    --drain)        DRAIN="$2"; shift 2 ;;
    --no-reset)     DO_RESET=0; shift ;;
    --no-parity)    DO_PARITY=0; shift ;;
    --keep-running) KEEP_RUNNING=1; shift ;;
    -h|--help)      sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$GEN_ARGS" ]]; then
  echo "--gen is required, e.g. --gen \"--accounts 3 --ordered --rate 30 --duration 60 --seed 7\"" >&2
  exit 2
fi

# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate
mkdir -p logs run

stop_engine () {
  if [[ -f run/engine.pid ]]; then
    local pid; pid="$(cat run/engine.pid)"
    if kill -0 "$pid" 2>/dev/null; then
      echo "==> stopping engine gracefully (SIGTERM) pid=$pid"
      kill -TERM "$pid" 2>/dev/null || true
      for _ in $(seq 1 30); do kill -0 "$pid" 2>/dev/null || break; sleep 1; done
      kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
    fi
    rm -f run/engine.pid
  fi
  pkill -f "spark/jobs/balance_engine.py" 2>/dev/null || true
  rm -f run/engine.launcher.pid
}
trap stop_engine EXIT

stop_engine

if [[ "$DO_RESET" -eq 1 ]]; then
  echo "==> resetting lake + topics"
  ./scripts/reset_lake.sh >/dev/null
  echo "    clean"
fi

echo "==> starting engine"
rm -f run/engine.pid
: > logs/engine.log
# The probe reads logs/progress.jsonl. A stale file from a previous run would let
# it resolve a batch_id against the wrong timeline, so it is truncated with the log.
: > logs/progress.jsonl
nohup python spark/jobs/balance_engine.py > logs/engine.log 2>&1 &
LAUNCHER=$!
echo "$LAUNCHER" > run/engine.launcher.pid

echo "==> waiting for engine readiness (run/engine.pid)"
for _ in $(seq 1 90); do
  [[ -f run/engine.pid ]] && break
  if ! kill -0 "$LAUNCHER" 2>/dev/null; then
    echo "ERROR: engine died during startup. Tail of logs/engine.log:" >&2
    tail -40 logs/engine.log >&2
    exit 1
  fi
  sleep 2
done
if [[ ! -f run/engine.pid ]]; then
  echo "ERROR: engine never became ready in 180s" >&2
  tail -40 logs/engine.log >&2
  exit 1
fi
echo "    engine pid $(cat run/engine.pid)"
sleep 8   # let the first (empty) micro-batch commit before publishing

echo "==> publishing: $GEN_ARGS"
# shellcheck disable=SC2086
python scripts/event_generator.py $GEN_ARGS

echo "==> draining for ${DRAIN}s"
sleep "$DRAIN"

if [[ "$KEEP_RUNNING" -eq 0 ]]; then
  stop_engine
  trap - EXIT
fi

STAGE_OK=1

if [[ "$DO_PARITY" -eq 1 ]]; then
  echo "==> parity"
  # shellcheck disable=SC2086
  python scripts/parity_balance.py $PARITY_ARGS || STAGE_OK=0
else
  echo "==> parity skipped (--no-parity)"
fi

if [[ "$RUN_PROBE" -eq 1 ]]; then
  echo "==> gap timing probe"
  # shellcheck disable=SC2086
  python scripts/gap_timing_probe.py $PROBE_ARGS || STAGE_OK=0
fi

if [[ "$STAGE_OK" -eq 1 ]]; then
  echo "==> STAGE RESULT: PASS"
else
  echo "==> STAGE RESULT: FAIL" >&2
  exit 1
fi
