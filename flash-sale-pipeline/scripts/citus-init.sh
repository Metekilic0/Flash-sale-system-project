#!/bin/bash
# Idempotent Citus cluster bootstrap. Safe to re-run.
set -euo pipefail

echo "[citus-init] Enabling the citus extension on each worker..."
PGPASSWORD="${PGPASSWORD}" psql -v ON_ERROR_STOP=1 -h citus-worker-1 -U "${PGUSER}" -d "${PGDATABASE}" -f /sql/00-worker-extension.sql
PGPASSWORD="${PGPASSWORD}" psql -v ON_ERROR_STOP=1 -h citus-worker-2 -U "${PGUSER}" -d "${PGDATABASE}" -f /sql/00-worker-extension.sql
PGPASSWORD="${PGPASSWORD}" psql -v ON_ERROR_STOP=1 -h citus-worker-3 -U "${PGUSER}" -d "${PGDATABASE}" -f /sql/00-worker-extension.sql

echo "[citus-init] Registering workers and creating the distributed schema on the coordinator..."
PGPASSWORD="${PGPASSWORD}" psql -v ON_ERROR_STOP=1 -h citus-coordinator -U "${PGUSER}" -d "${PGDATABASE}" -f /sql/01-coordinator-bootstrap.sql

echo "[citus-init] Cluster nodes:"
PGPASSWORD="${PGPASSWORD}" psql -h citus-coordinator -U "${PGUSER}" -d "${PGDATABASE}" -c "SELECT * FROM v_cluster_nodes;"

echo "[citus-init] Done."
