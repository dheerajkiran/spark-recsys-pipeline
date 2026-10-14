#!/usr/bin/env bash
# Run pipeline stages in order on the cluster with spark-submit.
#
# Usage: ./scripts/run_pipeline.sh [stage ...]   (default: all stages)
#   e.g. ./scripts/run_pipeline.sh clean
#
# Each stage reads one layer and writes the next, so any stage can be re-run
# on its own. A failed quality check stops the run.

set -euo pipefail

if [[ $# -eq 0 ]]; then
  set -- ingest clean
fi

# HDFS is read-only (safe mode) for ~30 s after the cluster starts.
docker exec namenode hdfs dfsadmin -safemode wait >/dev/null

for stage in "$@"; do
  echo "=== $stage ==="
  start=$(date +%s)
  docker exec spark-master spark-submit "/opt/project/pipeline/$stage.py"
  echo "=== $stage done in $(( $(date +%s) - start ))s ==="
done
