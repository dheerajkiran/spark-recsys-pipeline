#!/usr/bin/env bash
# Copy the gold dataset (all splits + _metadata) from HDFS to data/gold/ for training.
#
# Usage: ./scripts/export_gold.sh
#
# Copies into a temporary folder first and swaps it in only when complete, so
# a failed export never leaves a half-copied data/gold/ behind. Then checks
# that every split's row count matches gold/_metadata/feature_spec.json.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$REPO_ROOT/data/gold"
TMP="$REPO_ROOT/data/gold.tmp"
PY="$REPO_ROOT/.venv/bin/python"

docker exec namenode hdfs dfsadmin -safemode wait >/dev/null
if ! docker exec namenode hdfs dfs -test -e /gold/_metadata/feature_spec.json; then
  echo "No gold dataset on HDFS. Run ./scripts/run_pipeline.sh first." >&2
  exit 1
fi

echo "Copying hdfs:///gold out of HDFS"
docker exec namenode bash -c "rm -rf /tmp/gold_export && hdfs dfs -get /gold /tmp/gold_export"
rm -rf "$TMP"
docker cp namenode:/tmp/gold_export "$TMP" >/dev/null
docker exec namenode rm -rf /tmp/gold_export
find "$TMP" -name "_SUCCESS" -delete

rm -rf "$DEST"
mv "$TMP" "$DEST"
echo "Exported to $DEST ($(du -sh "$DEST" | cut -f1))"

if [[ ! -x "$PY" ]]; then
  echo "No .venv found; skipping the row-count check (see README section 17)." >&2
  exit 0
fi

"$PY" - "$DEST" <<'EOF'
import json, sys
from pathlib import Path
import pyarrow.parquet as pq

gold = Path(sys.argv[1])
spec = json.loads((gold / "_metadata" / "feature_spec.json").read_text())
ok = True
for name, info in spec["splits"].items():
    files = sorted((gold / name).glob("*.parquet"))
    rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    match = rows == info["rows"]
    ok &= match
    print(f"[{'PASS' if match else 'FAIL'}] {name}: {rows:,} rows in {len(files)} files "
          f"(feature_spec says {info['rows']:,})")
sys.exit(0 if ok else 1)
EOF
