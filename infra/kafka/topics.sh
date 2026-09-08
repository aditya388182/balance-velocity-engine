#!/usr/bin/env bash
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-p3-kafka}"
BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
SR_URL="${SR_URL:-http://localhost:8081}"

echo "==> waiting for broker on ${BOOTSTRAP}"
for i in $(seq 1 40); do
  if docker exec "$KAFKA_CONTAINER" kafka-broker-api-versions \
        --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
    echo "    broker up"; break
  fi
  [ "$i" -eq 40 ] && { echo "ERROR: broker never came up"; exit 1; }
  sleep 3
done

create_topic () {
  local name="$1" parts="$2"
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$parts" \
    --replication-factor 1 \
    --config compression.type=zstd \
    --config retention.ms=604800000 >/dev/null
  echo "    topic ready: ${name} (${parts}p, zstd)"
}

echo "==> creating topics"
create_topic accounts.events    3
create_topic accounts.signals   1
create_topic accounts.dlq       1
create_topic accounts.integrity 1

echo "==> waiting for schema registry on ${SR_URL}"
for i in $(seq 1 40); do
  if curl -sf "${SR_URL}/subjects" >/dev/null; then echo "    registry up"; break; fi
  [ "$i" -eq 40 ] && { echo "ERROR: schema registry never came up"; exit 1; }
  sleep 3
done

echo "==> setting global compatibility to BACKWARD_TRANSITIVE"
curl -sf -X PUT "${SR_URL}/config" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"compatibility":"BACKWARD_TRANSITIVE"}'
echo

echo "==> topics now present:"
docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server "$BOOTSTRAP" --list
