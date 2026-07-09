#!/bin/bash
set -euo pipefail
BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"
KAFKA_BIN="/opt/kafka/bin"

echo "[kafka-init] Waiting for controller quorum to settle..."
sleep 5

echo "[kafka-init] Checking topic '${KAFKA_TOPIC}'..."
if $KAFKA_BIN/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$KAFKA_TOPIC"; then
  echo "[kafka-init] Topic already exists, skipping."
else
  echo "[kafka-init] Creating topic (partitions=${KAFKA_PARTITIONS}, rf=${KAFKA_REPLICATION_FACTOR})..."
  $KAFKA_BIN/kafka-topics.sh --create \
    --bootstrap-server "$BOOTSTRAP" \
    --topic "$KAFKA_TOPIC" \
    --partitions "$KAFKA_PARTITIONS" \
    --replication-factor "$KAFKA_REPLICATION_FACTOR" \
    --config min.insync.replicas=2
fi

echo "[kafka-init] Topic details:"
$KAFKA_BIN/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$KAFKA_TOPIC"
echo "[kafka-init] Done."
