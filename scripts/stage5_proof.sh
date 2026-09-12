#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

EVENTS=10000
ONLY="both"
TICK_DURATION=90
while [[ $# -gt 0 ]]; do
  case "$1" in
    --events) EVENTS="$2"; shift 2 ;;
    --only)   ONLY="$2"; shift 2 ;;
    --tick)   TICK_DURATION="$2"; shift 2 ;;
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
  for _ in $(seq 1 90); do [[ -f run/engine.pid ]] && return 0; sleep 2; done
  echo "engine never became ready" >&2; tail -30 logs/engine.log >&2; return 1
}

run_one () {
  local mode="$1" capenv="$2"
  echo ""
  echo "############ STAGE 5 — ${mode} ############"
  stop_engine
  ./scripts/reset_lake.sh >/dev/null
  : > logs/engine.log; : > logs/progress.jsonl

  echo "==> start engine (${capenv:-default cap})"
  if [[ -n "$capenv" ]]; then
    env "$capenv" nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
  else
    nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
  fi
  wait_ready
  grep -m1 "max_buffer_size" logs/engine.log || true

  echo "==> waiting for the first committed batch"
  for _ in $(seq 1 60); do [[ -s logs/progress.jsonl ]] && break; sleep 2; done

  echo "==> burst: ${EVENTS} out-of-order events, head withheld"
  python scripts/inject_burst.py --account HOT-1 --events "$EVENTS" --start-seq 2 \
         --never-send 1 --event-time-step-ms 2 --seed 99 --run-id "burst_${mode}"
  sleep 30

  echo "==> pushing event time forward so the gap can be CONFIRMED in both runs"
  python scripts/event_generator.py --accounts 1 --account-prefix TICK- \
         --rate 2 --duration "$TICK_DURATION" --heartbeat-account --seed 3 >/dev/null
  echo "==> draining"
  sleep 45
  stop_engine

  echo "==> proof (${mode})"
  if [[ -n "$capenv" ]]; then
    env "$capenv" python scripts/buffer_proof.py --account HOT-1 --mode "$mode" \
        --events "$EVENTS" --save "logs/proof_${mode}.json"
  else
    python scripts/buffer_proof.py --account HOT-1 --mode "$mode" \
        --events "$EVENTS" --save "logs/proof_${mode}.json"
  fi
}

OK=1
if [[ "$ONLY" == "both" || "$ONLY" == "capped" ]]; then
  run_one capped "" || OK=0
fi
if [[ "$ONLY" == "both" || "$ONLY" == "uncapped" ]]; then
  run_one uncapped "P3_MAX_BUFFER_SIZE=200000" || OK=0
fi

if [[ "$ONLY" == "both" && -f logs/proof_capped.json && -f logs/proof_uncapped.json ]]; then
  echo ""
  echo "############ THE CONTRAST ############"
  python - <<'PY'
import json
a = json.load(open("logs/proof_capped.json"))
b = json.load(open("logs/proof_uncapped.json"))
print(f"{'':<22}{'capped':>16}{'uncapped':>16}")
for k in ("cap", "last_applied_seq", "balance_minor", "dlq_records", "sequence_gap_rows"):
    print(f"{k:<22}{a[k]:>16,}{b[k]:>16,}")
shed = b["balance_minor"] - a["balance_minor"]
print()
print(f"Same input. The cap shed {a['dlq_records']:,} events to the DLQ and cost")
print(f"{abs(shed):,} minor units of balance, deliberately, to bound memory.")
PY
fi

trap - EXIT
stop_engine
[[ "$OK" -eq 1 ]] && { echo "==> STAGE 5 PROOF: PASS"; exit 0; }
echo "==> STAGE 5 PROOF: FAIL" >&2; exit 1
