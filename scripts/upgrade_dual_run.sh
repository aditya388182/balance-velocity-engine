#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
# shellcheck disable=SC1091
[ -d .venv ] && source .venv/bin/activate
BUILD="$(cat "$REPO_ROOT/BUILD" 2>/dev/null || echo UNKNOWN)"
echo "### upgrade_dual_run.sh  build ${BUILD}"
MC="${MC_CONTAINER:-p3-mc}"
mkdir -p logs run

OLD_TAG="$(python3 -c "from conf.config import CFG; print(CFG['spark_version_tag'])")"
NEW_TAG="${1:-v3.6.0-drill}"
PARITY_WINDOW="${PARITY_WINDOW:-60}"
SNAP_ROOT="local/state-snapshots/balance_engine"

stop_all () { pkill -f "spark/jobs/balance_engine.py" 2>/dev/null || true; rm -f run/engine.pid; }

echo "==> old checkpoint path : checkpoints/${OLD_TAG}/"
echo "==> new checkpoint path : checkpoints/${NEW_TAG}/"
echo "    the paths are versioned because they have been since the FIRST run."
echo "    A drill that introduces versioning on the day of the upgrade proves nothing."

echo "==> snapshotting the old job's state"
./scripts/snapshot_state.sh >/dev/null
LATEST="$(docker exec "$MC" mc ls "$SNAP_ROOT/" | awk '{print $NF}' | tr -d '/' | sort | tail -1)"
echo "    $LATEST"

echo "==> seeding the NEW path from the snapshot + bounded replay"
echo "    (NOT from the old checkpoint — that is the format-coupled thing you must"
echo "     never depend on)"
docker exec "$MC" mc mirror --overwrite \
  "${SNAP_ROOT}/${LATEST}/" "local/balance-lake/checkpoints/${NEW_TAG}/" >/dev/null

echo "==> draining the old job gracefully (SIGTERM — the one place graceful is right)"
if [[ -f run/engine.pid ]]; then
  kill -TERM "$(cat run/engine.pid)" 2>/dev/null || true
  for _ in $(seq 1 30); do kill -0 "$(cat run/engine.pid 2>/dev/null)" 2>/dev/null || break; sleep 1; done
fi
stop_all
rm -f run/engine.pid

echo "==> starting the NEW job on the new versioned path"
# Mark where this job's output begins. logs/engine.log is APPENDED across every
# lifetime, so `grep -m1` returns the FIRST match in the whole file — the OLD
# job's startup line from minutes earlier. Reporting that as the new job's
# checkpoint path is how a working drill reads as a broken one.
LOG_MARK=$(wc -l < logs/engine.log 2>/dev/null || echo 0)

P3_SPARK_VERSION_TAG="$NEW_TAG" P3_VELOCITY_ENABLED=0 \
  nohup python spark/jobs/balance_engine.py >> logs/engine.log 2>&1 &
for _ in $(seq 1 90); do [[ -f run/engine.pid ]] && break; sleep 2; done
[[ -f run/engine.pid ]] || { echo "ERROR: new job did not start" >&2; tail -30 logs/engine.log >&2; exit 1; }

# ...and read only the lines THIS job wrote.
NEW_CKPT=$(tail -n +$((LOG_MARK+1)) logs/engine.log | grep -m1 "\[engine\] checkpoint" || true)
echo "    ${NEW_CKPT:-<no checkpoint line yet>}"
if [[ "$NEW_CKPT" != *"$NEW_TAG"* ]]; then
  echo "ERROR: the new job is NOT on checkpoints/${NEW_TAG}/." >&2
  echo "       P3_SPARK_VERSION_TAG did not reach the process. Without that, the" >&2
  echo "       drill is silently re-running the old job and proves nothing." >&2
  tail -n +$((LOG_MARK+1)) logs/engine.log | head -20 >&2
  exit 1
fi
echo "    confirmed: the new job is on the NEW versioned path"

echo "==> parity window (${PARITY_WINDOW}s; production: 24h)"
python scripts/wait_for_drain.py --query balance_engine --timeout 300 --stable-seconds 25 \
  || echo "    (drain wait did not confirm)"
sleep "$PARITY_WINDOW"
stop_all

echo "==> validating the new path against the oracle"
# --expect-replay: seeding from a snapshot means re-reading Kafka from the
# snapshot's offsets, so every re-read event hits seq <= last and is dropped WITH
# A RECORD. The delivery log contains no duplicates, so it cannot predict those.
# Balance, last_applied_seq and applied-count are still compared exactly — a real
# double-apply would still fail here.
OK=1
python scripts/parity_balance.py --expect-replay || OK=0

echo "==> cutover"
echo "    new job is primary; the old path is retired to cold storage and deleted"
echo "    after the parity window. In production this is 24 hours of BOTH paths"
echo "    compared against a batch recomputation before anything is deleted."
docker exec "$MC" mc ls "local/balance-lake/checkpoints/" | tail -5

printf '%s\n' "{\"drill\":\"upgrade\",\"old\":\"$OLD_TAG\",\"new\":\"$NEW_TAG\",\"parity\":$OK}" \
  >> logs/chaos.jsonl

[[ "$OK" -eq 1 ]] && { echo "==> UPGRADE DRILL: PASS — migration choreography validated"; exit 0; }
echo "==> UPGRADE DRILL: FAIL" >&2; exit 1
