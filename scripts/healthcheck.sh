#!/bin/bash
# Quick smoke test: confirms every component is healthy and data is actually
# flowing end-to-end. Run after `docker compose up -d` once everything has
# had a minute or two to start.
set -uo pipefail

echo "============================================================"
echo " Flash Sale Pipeline - Health Check"
echo "============================================================"

echo
echo "--- Container status ---------------------------------------"
docker compose ps

echo
echo "--- Kafka: topic description ---------------------------------"
docker compose exec -T kafka-1 kafka-topics.sh \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --describe --topic "${KAFKA_TOPIC:-clickstream-events}"

echo
echo "--- Citus: registered cluster nodes ----------------------------"
docker compose exec -T citus-coordinator psql -U "${CITUS_USER:-citus}" -d "${CITUS_DB:-citus}" \
  -c "SELECT * FROM v_cluster_nodes;"

echo
echo "--- Citus: row counts per table --------------------------------"
docker compose exec -T citus-coordinator psql -U "${CITUS_USER:-citus}" -d "${CITUS_DB:-citus}" -c "
SELECT 'events_clean' AS table_name, count(*) FROM events_clean
UNION ALL SELECT 'events_flagged', count(*) FROM events_flagged
UNION ALL SELECT 'events_agg', count(*) FROM events_agg
UNION ALL SELECT 'pipeline_metrics', count(*) FROM pipeline_metrics;
"

echo
echo "--- Latest 5 pipeline_metrics rows -------------------------------"
docker compose exec -T citus-coordinator psql -U "${CITUS_USER:-citus}" -d "${CITUS_DB:-citus}" -c "
SELECT batch_id, batch_ts, total_events, legit_events, flagged_events, processing_time_ms
FROM pipeline_metrics ORDER BY batch_ts DESC LIMIT 5;
"

echo
echo "--- Spark master UI -----------------------------------------------"
echo "Open http://localhost:8080 to see registered workers and running applications."

echo
echo "--- Grafana / Prometheus ----------------------------------------"
echo "Grafana:    http://localhost:3000  (dashboard: 'Flash Sale Pipeline - Overview')"
echo "Prometheus: http://localhost:9090/targets  (all targets should be 'UP')"

echo
echo "============================================================"
echo " If 'pipeline_metrics' has rows and table counts are growing"
echo " when you re-run this script, the end-to-end pipeline is healthy."
echo "============================================================"
