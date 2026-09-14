#!/usr/bin/env bash
set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
MC="${MC_CONTAINER:-p3-mc}"

TAG="$(python3 -c "from conf.config import CFG; print(CFG['spark_version_tag'])")"
STATE="local/balance-lake/checkpoints/${TAG}/balance_engine/state"

echo "==> files under ${STATE} before:"
docker exec "$MC" mc ls -r "$STATE/" 2>/dev/null | tail -5 || {
  echo "ERROR: no checkpoint state to corrupt — has the engine run?" >&2; exit 1; }

VICTIMS="$(docker exec "$MC" mc ls -r "$STATE/" 2>/dev/null \
          | awk '{print $NF}' | grep -E '\.(delta|zip|sst|changelog)$' | head -5 || true)"
if [[ -z "$VICTIMS" ]]; then
  VICTIMS="$(docker exec "$MC" mc ls -r "$STATE/" | awk '{print $NF}' | head -5)"
fi
[[ -z "$VICTIMS" ]] && { echo "ERROR: nothing to corrupt under $STATE" >&2; exit 1; }

COUNT=0
while read -r f; do
  [[ -z "$f" ]] && continue
  # overwrite with garbage rather than deleting: the file still EXISTS, so Spark
  # opens it and fails parsing it, which is what a partial write looks like.
  echo "CORRUPTED-BY-DRILL" | docker exec -i "$MC" mc pipe "${STATE}/${f}" >/dev/null
  echo "    truncated ${f}"
  COUNT=$((COUNT+1))
done <<< "$VICTIMS"

printf '%s\n' "{\"event\":\"CHECKPOINT_CORRUPTED\",\"files\":$COUNT,\"wall\":\"$(date -u +%FT%TZ)\"}" \
  >> logs/chaos.jsonl
echo "==> corrupted ${COUNT} state file(s). The engine will fail on its next batch."
