#!/bin/bash
# Consumer capacity test: queue a backlog with the consumer stopped, then time how fast it drains.
# Usage: loadtest/drain.sh [events=60000]
set -euo pipefail
EVENTS=${1:-60000}
METRICS=http://localhost:9100/metrics
processed() { curl -s $METRICS | awk '/^consumer_events_processed_total\{/ {s+=$2} END {print s+0}'; }

docker compose stop consumer >/dev/null
node "$(dirname "$0")/send-only.mjs" "$EVENTS" 100 10
docker compose start consumer >/dev/null
until curl -s -o /dev/null $METRICS; do sleep 0.5; done
START_COUNT=$(processed)
until [ "$(processed)" -gt "$START_COUNT" ]; do sleep 0.2; done
T0=$(date +%s.%N)
until [ "$(processed)" -ge $((START_COUNT + EVENTS)) ]; do sleep 0.2; done
T1=$(date +%s.%N)
echo "{\"events\": $EVENTS, \"drain_seconds\": $(echo "$T1 - $T0" | bc), \"consumer_events_per_sec\": $(echo "$EVENTS / ($T1 - $T0)" | bc)}"
