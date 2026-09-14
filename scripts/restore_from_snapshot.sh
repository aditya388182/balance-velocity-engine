#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate
BUILD="$(cat "$REPO_ROOT/BUILD" 2>/dev/null || echo UNKNOWN)"
echo "### restore_from_snapshot.sh  build ${BUILD}"
MC="${MC_CONTAINER:-p3-mc}"
mkdir -p logs run

MEASURE_BASELINE=0
[[ "${1:-}" == "--with-baseline" ]] && MEASURE_BASELINE=1

TAG="$(python3 -c "from conf.config import CFG; print(CFG['spark_version_tag'])")"
SNAP_ROOT="local/state-snapshots/balance_engine"

stop_engine () {
  [[ -f run/engine.pid ]] && { kill -TERM "$(cat run/engine.pid)" 2>/dev/null || true; sleep 3; }
  pkill -f "spark/jobs/balance_engine.py" 2>/dev/null || true
  rm -f run/engine.pid run/engine.launcher.pid
}
start_engine () {
  rm -f run/engine.pid
  P3_VELOCITY_ENABLED=0 nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
  for _ in $(seq 1 90); do [[ -f run/engine.pid ]] && return 0; sleep 2; done
  return 1
}

echo "==> latest snapshot:"
LATEST="$(docker exec "$MC" mc ls "$SNAP_ROOT/" | awk '{print $NF}' | tr -d '/' | sort | tail -1)"
[[ -z "$LATEST" ]] && { echo "ERROR: no snapshots. Run ./scripts/snapshot_state.sh first." >&2; exit 1; }
echo "    $LATEST"

echo "==> corrupting the live checkpoint"
./scripts/corrupt_checkpoint.sh

echo "==> waiting for the engine to die on it"
DIED=0
for _ in $(seq 1 60); do
  if ! pgrep -f "spark/jobs/balance_engine.py" >/dev/null; then DIED=1; break; fi
  if grep -qiE "state.*(corrupt|checksum|EOFException|Invalid|Failed to read)" logs/engine.log; then
    DIED=1; break
  fi
  sleep 2
done
[[ "$DIED" -eq 1 ]] && echo "    the engine failed, as designed" \
                    || echo "    WARNING: the engine has not failed yet; continuing"
stop_engine

T_START=$(date +%s)

echo "==> restoring ${LATEST} to a FRESH checkpoint path (never over the corpse)"
RESTORE_TAG="${TAG}-restored-$(date -u +%H%M%S)"
docker exec "$MC" mc mirror --overwrite \
  "${SNAP_ROOT}/${LATEST}/" "local/balance-lake/checkpoints/${RESTORE_TAG}/" >/dev/null
echo "    restored to checkpoints/${RESTORE_TAG}/"

echo "==> restarting against the restored path; Kafka replay is bounded by the"
echo "    offsets inside the snapshot, not by 'earliest'"
LOG_MARK=$(wc -l < logs/engine.log 2>/dev/null || echo 0)
P3_SPARK_VERSION_TAG="$RESTORE_TAG" start_engine || {
  echo "ERROR: the engine did not restart" >&2; tail -30 logs/engine.log >&2; exit 1; }
# Read only this lifetime's lines — the log is appended across every restart.
RESTORED_CKPT=$(tail -n +$((LOG_MARK+1)) logs/engine.log | grep -m1 "\[engine\] checkpoint" || true)
echo "    ${RESTORED_CKPT:-<no checkpoint line yet>}"
if [[ "$RESTORED_CKPT" != *"$RESTORE_TAG"* ]]; then
  echo "ERROR: the engine is not on the RESTORED path checkpoints/${RESTORE_TAG}/." >&2
  echo "       It may have resumed the corrupt checkpoint instead." >&2
  exit 1
fi

python scripts/wait_for_drain.py --query balance_engine --timeout 300 --stable-seconds 25 \
  || echo "    (drain wait did not confirm; continuing to parity)"
stop_engine
T_RECOVER=$(( $(date +%s) - T_START ))

echo "==> parity after recovery"
# Same as the upgrade drill: restoring a snapshot re-reads Kafka from the
# snapshot's offsets, so the drop branch fires on every re-read event.
OK=1
python scripts/parity_balance.py --expect-replay || OK=0
echo "==> T_recover with snapshot: ${T_RECOVER}s"

if [[ "$MEASURE_BASELINE" -eq 1 ]]; then
  echo "==> CONTRAST: the same recovery with NO snapshot (replay from earliest)"
  T2=$(date +%s)
  docker exec "$MC" mc rm -r --force "local/balance-lake/checkpoints/" >/dev/null 2>&1 || true
  P3_SPARK_VERSION_TAG="${TAG}-scratch" start_engine || true
  python scripts/wait_for_drain.py --query balance_engine --timeout 900 --stable-seconds 25 || true
  stop_engine
  T_SCRATCH=$(( $(date +%s) - T2 ))
  echo "==> T_recover from earliest: ${T_SCRATCH}s   vs   with snapshot: ${T_RECOVER}s"
  printf '%s\n' "{\"drill\":\"corruption\",\"t_snapshot_s\":$T_RECOVER,\"t_scratch_s\":$T_SCRATCH}" \
    >> logs/chaos.jsonl
else
  printf '%s\n' "{\"drill\":\"corruption\",\"t_snapshot_s\":$T_RECOVER}" >> logs/chaos.jsonl
fi

[[ "$OK" -eq 1 ]] && { echo "==> CORRUPTION DRILL: PASS"; exit 0; }
echo "==> CORRUPTION DRILL: FAIL" >&2; exit 1
