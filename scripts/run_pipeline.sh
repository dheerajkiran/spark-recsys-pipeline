#!/usr/bin/env bash
# Run pipeline stages in order on the cluster with spark-submit.
#
# Usage: ./scripts/run_pipeline.sh [stage ...]   (default: all stages)
#   e.g. ./scripts/run_pipeline.sh clean
# PIPELINE_CONFIG picks another config (repo-relative), e.g. config/pipeline-1k.yaml.
#
# Each stage reads one layer and writes the next, so any stage can be re-run
# on its own. A failed quality check stops the run.

set -euo pipefail

if [[ $# -eq 0 ]]; then
  set -- ingest clean features build_dataset
fi

# HDFS is read-only (safe mode) for ~30 s after the cluster starts.
docker exec namenode hdfs dfsadmin -safemode wait >/dev/null

for stage in "$@"; do
  echo "=== $stage ==="
  start=$(date +%s)
  docker exec spark-master spark-submit "/opt/project/pipeline/$stage.py" \
    --config "/opt/project/${PIPELINE_CONFIG:-config/pipeline.yaml}"
  echo "=== $stage done in $(( $(date +%s) - start ))s ==="
done
