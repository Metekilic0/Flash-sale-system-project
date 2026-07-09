#!/bin/bash
# =============================================================================
# Fault-injection helper for the Systems Track "scalability, connectivity and
# fault-tolerance" evaluation. Run sub-commands individually during a live
# demo, or run `./scripts/fault_injection.sh full-scenario` for a scripted,
# timed run-through you can paste straight into the project report.
#
# Usage:
#   ./scripts/fault_injection.sh kill-kafka-broker 2
#   ./scripts/fault_injection.sh restart-kafka-broker 2
#   ./scripts/fault_injection.sh kill-citus-worker 1
#   ./scripts/fault_injection.sh restart-citus-worker 1
#   ./scripts/fault_injection.sh scale-spark-workers 3
#   ./scripts/fault_injection.sh full-scenario
# =============================================================================
set -euo pipefail

ts() { date "+%Y-%m-%dT%H:%M:%S"; }
log() { echo "[$(ts)] $*"; }

kafka_topic_describe() {
  docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
    --describe --topic "${KAFKA_TOPIC:-clickstream-events}" || true
}

citus_node_status() {
  docker compose exec -T citus-coordinator psql -U "${CITUS_USER:-citus}" -d "${CITUS_DB:-citus}" \
    -c "SELECT * FROM v_cluster_nodes;" || true
}

recent_metrics() {
  docker compose exec -T citus-coordinator psql -U "${CITUS_USER:-citus}" -d "${CITUS_DB:-citus}" \
    -c "SELECT batch_id, batch_ts, total_events, legit_events, flagged_events, processing_time_ms FROM pipeline_metrics ORDER BY batch_ts DESC LIMIT 5;" || true
}

cmd_kill_kafka_broker() {
  local n="$1"
  log "Killing kafka-${n} to simulate a broker failure..."
  docker compose stop "kafka-${n}"
  log "Broker kafka-${n} stopped. Topic state (replicas should show fewer in-sync replicas):"
  kafka_topic_describe
}

cmd_restart_kafka_broker() {
  local n="$1"
  log "Restarting kafka-${n}..."
  docker compose start "kafka-${n}"
  log "Waiting for kafka-${n} to rejoin the cluster..."
  sleep 15
  kafka_topic_describe
}

cmd_kill_citus_worker() {
  local n="$1"
  log "Killing citus-worker-${n} to simulate a database node failure..."
  docker compose stop "citus-worker-${n}"
  log "Worker stopped. Coordinator's view of cluster nodes:"
  citus_node_status
  log "NOTE: queries touching shards on the dead worker will fail/timeout until it is restarted."
  log "      this is the expected behaviour to discuss in the report (no automatic shard failover"
  log "      without replication factor > 1 / citus.shard_replication_factor configured)."
}

cmd_restart_citus_worker() {
  local n="$1"
  log "Restarting citus-worker-${n}..."
  docker compose start "citus-worker-${n}"
  sleep 10
  citus_node_status
}

cmd_scale_spark_workers() {
  local n="$1"
  log "Scaling spark-worker to ${n} replicas..."
  docker compose up -d --scale "spark-worker=${n}" --no-recreate
  log "Check http://localhost:8080 to confirm ${n} workers are registered with the master."
}

cmd_full_scenario() {
  log "================= FULL FAULT-TOLERANCE SCENARIO START ================="

  log "--- Baseline ---"
  recent_metrics

  log "--- Step 1: kill one Kafka broker (out of 3) while traffic keeps flowing ---"
  cmd_kill_kafka_broker 3
  sleep 20
  log "Pipeline metrics 20s after broker loss (should still be producing batches):"
  recent_metrics

  log "--- Step 2: restart the broker, confirm it rejoins ---"
  cmd_restart_kafka_broker 3

  log "--- Step 3: kill one Citus worker (out of 2) ---"
  cmd_kill_citus_worker 2
  sleep 15
  log "Pipeline metrics while a worker is down (writes touching that worker's shards will start failing):"
  recent_metrics || true

  log "--- Step 4: restart the Citus worker ---"
  cmd_restart_citus_worker 2
  sleep 10
  recent_metrics

  log "--- Step 5: scale Spark out to 3 workers under load ---"
  cmd_scale_spark_workers 3

  log "================= FULL FAULT-TOLERANCE SCENARIO END ================="
  log "Tip: keep this terminal's output - paste it (trimmed) into the report's"
  log "     'fault tolerance / scalability evaluation' section."
}

main() {
  local sub="${1:-}"
  shift || true
  case "$sub" in
    kill-kafka-broker) cmd_kill_kafka_broker "$1" ;;
    restart-kafka-broker) cmd_restart_kafka_broker "$1" ;;
    kill-citus-worker) cmd_kill_citus_worker "$1" ;;
    restart-citus-worker) cmd_restart_citus_worker "$1" ;;
    scale-spark-workers) cmd_scale_spark_workers "$1" ;;
    full-scenario) cmd_full_scenario ;;
    *)
      echo "Usage: $0 {kill-kafka-broker N|restart-kafka-broker N|kill-citus-worker N|restart-citus-worker N|scale-spark-workers N|full-scenario}"
      exit 1
      ;;
  esac
}

main "$@"
