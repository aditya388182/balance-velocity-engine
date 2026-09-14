#!/usr/bin/env bash
# scripts/restore_from_snapshot.sh — Drill 1: corruption -> restore -> bounded replay.
#
# THE CHOREOGRAPHY, AND WHY EACH STEP IS WHAT IT IS
# -------------------------------------------------
# 1. note the wall clock, corrupt the live checkpoint, watch the engine die on its
#    next batch. The drill starts from a REAL error, not a hypothetical.
# 2. restore the latest snapshot to a FRESH checkpoint path. Never over the corpse:
#    half the value of an immutable snapshot is that the broken state is still
#    there to look at afterwards, and restoring onto it destroys the evidence and
#    risks mixing good files with bad.
# 3. restart against the fresh path. Kafka replay is BOUNDED — only offsets after
#    the snapshot's committed ones are re-read, because the snapshot contains those
#    offsets. That is the entire reason a snapshot is a copy of the durable
#    checkpoint rather than of the executor's local RocksDB.
# 4. the overlap is absorbed by the same three layers as Day 4: atomic state+offset
#    restore, the state machine's dup branch, and the strict-> MERGE guard.
# 5. measure T_recover, and measure the alternative once for contrast.
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate
BUILD="$(cat "$REPO_ROOT/BUILD" 2>/dev/null || echo UNKNOWN)"

# The assertion must WAIT for the line, not sample once. Even unbuffered, the
# banner and the pid file are written by different code paths and there is no
# ordering guarantee across a filesystem. Sampling once turns a 2-second race into
# a failed drill.
wait_for_ckpt_line () {   # $1 = log line count before this job started
  local mark="$1" line=""
  for _ in $(seq 1 40); do
    line=$(tail -n +$((mark+1)) logs/engine.log 2>/dev/null \
           | grep -m1 "\[engine\] checkpoint" || true)
    [[ -n "$line" ]] && { printf '%s' "$line"; return 0; }
    sleep 2
  done
  return 1
}

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
  PYTHONUNBUFFERED=1 P3_VELOCITY_ENABLED=0 nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
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
RESTORED_CKPT=$(wait_for_ckpt_line "$LOG_MARK" || true)
if [[ -z "$RESTORED_CKPT" ]]; then
  echo "ERROR: the engine never logged a checkpoint path within 80s." >&2
  echo "       It is running (the pid file exists) but has not reached query.start()." >&2
  tail -n +$((LOG_MARK+1)) logs/engine.log | tail -20 >&2
  exit 1
fi
echo "    $RESTORED_CKPT"
if [[ "$RESTORED_CKPT" != *"$RESTORE_TAG"* ]]; then
  echo "ERROR: the engine is not on the RESTORED path checkpoints/${RESTORE_TAG}/." >&2
  echo "       It may have resumed the corrupt checkpoint instead." >&2
  exit 1
fi
echo "    confirmed: running on the restored path"

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
