#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-p3-kafka}"
MC_CONTAINER="${MC_CONTAINER:-p3-mc}"
BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "==> deleting Kafka topics"
for t in accounts.events accounts.signals accounts.dlq accounts.integrity; do
  docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --delete --topic "$t" >/dev/null 2>&1 || echo "    ($t absent)"
done
sleep 4   # topic deletion is asynchronous

echo "==> recreating topics"
"${REPO_ROOT}/infra/kafka/topics.sh" >/dev/null
echo "    topics recreated"

echo "==> wiping MinIO prefixes"
docker exec "$MC_CONTAINER" mc rm -r --force local/balance-lake/ >/dev/null 2>&1 || true
docker exec "$MC_CONTAINER" mc rm -r --force local/state-snapshots/ >/dev/null 2>&1 || true
docker exec "$MC_CONTAINER" mc mb --ignore-existing local/balance-lake >/dev/null
docker exec "$MC_CONTAINER" mc mb --ignore-existing local/state-snapshots >/dev/null
echo "    buckets emptied"

echo "==> removing local delivery logs + run artifacts"
rm -f "${REPO_ROOT}"/delivery_log_*.jsonl
rm -f "${REPO_ROOT}"/run/*.pid
mkdir -p "${REPO_ROOT}/logs"
: > "${REPO_ROOT}/logs/engine.log"

echo "==> RESET COMPLETE"
