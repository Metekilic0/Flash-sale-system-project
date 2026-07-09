#!/bin/bash
# Manually triggers one flash-sale burst cycle in the producer when
# PRODUCER_MODE=manual (in your .env). Useful for live demos: stay in calm
# NORMAL traffic, then trigger a burst exactly when you want to show it off.
#
# Usage: ./scripts/trigger_burst.sh
set -euo pipefail

CONTROL_DIR="./control"
TRIGGER_FILE="${CONTROL_DIR}/burst_trigger"

mkdir -p "${CONTROL_DIR}"
touch "${TRIGGER_FILE}"

echo "Burst trigger created at ${TRIGGER_FILE}."
echo "The producer container polls for this file and will consume (delete) it"
echo "within a few seconds, then run one FLASH_SALE burst phase."
echo
echo "Watch it happen with: docker compose logs -f producer"
