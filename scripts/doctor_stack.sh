#!/usr/bin/env bash
set -uo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
COMPOSE="infra/docker-compose.yml"
G="\033[92m"; R="\033[91m"; Y="\033[93m"; D="\033[2m"; N="\033[0m"

echo "### stack doctor"
echo "=============================================================================="

echo ""
echo "1. Host resources"
echo "-----------------"
if command -v free >/dev/null; then
  free -m | awk 'NR<=2{printf "  %s\n", $0}'
  AVAIL=$(free -m | awk '/^Mem:/{print $7}')
  echo "  available: ${AVAIL} MB"
  if [ "${AVAIL:-0}" -lt 2000 ]; then
    echo -e "  ${R}LOW${N} — under 2 GB free. The core stack alone reserves ~3.4 GB of limits."
    echo -e "  ${D}Bring the stack up WITHOUT the observability profile, and stop the${N}"
    echo -e "  ${D}Spark driver before running drills.${N}"
  fi
else
  echo "  (no free(1) on this host)"
fi
echo "  docker limits declared in $COMPOSE:"
grep -E "^\s+(mem_limit|container_name):" "$COMPOSE" | paste - - 2>/dev/null \
  | sed 's/^/    /' || true

echo ""
echo "2. Container status"
echo "-------------------"
docker compose -f "$COMPOSE" ps --format "  {{.Name}}\t{{.State}}\t{{.Status}}" 2>/dev/null \
  || docker compose -f "$COMPOSE" ps

echo ""
echo "3. Was Kafka OOM-killed?"
echo "------------------------"
OOM=$(docker inspect p3-kafka --format '{{.State.OOMKilled}}' 2>/dev/null || echo "n/a")
CODE=$(docker inspect p3-kafka --format '{{.State.ExitCode}}' 2>/dev/null || echo "n/a")
RESTARTS=$(docker inspect p3-kafka --format '{{.RestartCount}}' 2>/dev/null || echo "n/a")
echo "  OOMKilled : $OOM"
echo "  ExitCode  : $CODE   (137 = SIGKILL, almost always the OOM killer)"
echo "  Restarts  : $RESTARTS"
if [ "$OOM" = "true" ] || [ "$CODE" = "137" ]; then
  echo -e "  ${R}THE BROKER WAS KILLED FOR MEMORY.${N}"
  echo -e "  ${D}mem_limit must be heap + ~500 MB. A JVM's RSS is heap PLUS metaspace,${N}"
  echo -e "  ${D}thread stacks, direct buffers and code cache. Current settings:${N}"
  grep -E "KAFKA_HEAP_OPTS|mem_limit: " "$COMPOSE" | head -2 | sed 's/^/    /'
fi

echo ""
echo "4. Did the broker actually start?"
echo "---------------------------------"
if docker logs p3-kafka 2>&1 | grep -q "Kafka Server started"; then
  echo -e "  ${G}YES — 'Kafka Server started' is in the log.${N}"
  echo -e "  ${Y}So the BROKER is fine and the HEALTHCHECK is what is failing.${N}"
  echo -e "  ${D}kafka-broker-api-versions launches a fresh JVM per probe; under CPU${N}"
  echo -e "  ${D}contention that exceeds a short timeout. Fix: start_period + a longer${N}"
  echo -e "  ${D}timeout, and run the observability containers behind a profile.${N}"
  echo ""
  echo "  proving it by hand:"
  docker exec p3-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 \
    >/dev/null 2>&1 \
    && echo -e "    ${G}the broker answers the API call — it is healthy${N}" \
    || echo -e "    ${R}the API call fails too — this is not just the probe${N}"
else
  echo -e "  ${R}NO — the broker never reported 'Kafka Server started'.${N}"
fi

echo ""
echo "5. The decisive log lines"
echo "-------------------------"
docker logs p3-kafka 2>&1 | grep -iE \
  "ERROR|FATAL|Exception|Cluster ID|InconsistentClusterId|Address already in use|OutOfMemory|Shutdown" \
  | tail -12 | sed 's/^/  /'
if [ -z "$(docker logs p3-kafka 2>&1 | grep -iE 'ERROR|FATAL|Exception' | head -1)" ]; then
  echo -e "  ${G}no errors, exceptions or fatals in the broker log${N}"
fi
echo ""
echo -e "  ${D}InconsistentClusterId  -> the volume was formatted with a different${N}"
echo -e "  ${D}                          CLUSTER_ID. docker compose down -v, then up.${N}"
echo -e "  ${D}Address already in use  -> something else holds 29092 or 9092 on the host.${N}"

echo ""
echo "6. Host ports"
echo "-------------"
for p in 29092 8081 9000 9001 9090 9091 3000; do
  if command -v ss >/dev/null && ss -ltn 2>/dev/null | grep -q ":$p "; then
    echo "  $p  in use"
  else
    echo "  $p  free"
  fi
done

echo ""
echo "=============================================================================="
echo "Recommended, in order:"
echo "  1. docker compose -f $COMPOSE down -v"
echo "  2. docker compose -f $COMPOSE up -d              # CORE ONLY"
echo "  3. ./infra/kafka/topics.sh"
echo "  4. add observability only when you need the dashboards:"
echo "     docker compose -f $COMPOSE --profile observability up -d"
