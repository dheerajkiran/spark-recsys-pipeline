#!/usr/bin/env bash
# Create the HDFS layer directories and upload a downloaded KuaiRand version.
#
# Usage: ./scripts/load_to_hdfs.sh [pure|1k|27k]   (default: pure)
#
# Runs `hdfs` inside the namenode container, which mounts ./data/raw read-only
# at /data/raw (see docker-compose.yml). Re-running overwrites the raw files.

set -euo pipefail

VERSION="${1:-pure}"
case "$VERSION" in
  pure) NAME="KuaiRand-Pure" ;;
  1k)   NAME="KuaiRand-1K" ;;
  27k)  NAME="KuaiRand-27K" ;;
  *) echo "Unknown version '$VERSION'. Use one of: pure, 1k, 27k" >&2; exit 1 ;;
esac

SRC="/data/raw/$NAME/data"
DEST="/raw/kuairand/$VERSION"

hdfs() { docker exec namenode hdfs "$@"; }

if ! docker exec namenode test -d "$SRC"; then
  echo "$SRC not found in the namenode container. Run ./scripts/download_data.sh $VERSION first." >&2
  exit 1
fi

# After a (re)start the namenode stays read-only (safe mode) until the
# datanodes have reported their blocks, usually ~30 s.
hdfs dfsadmin -safemode wait

echo "Creating layer directories"
hdfs dfs -mkdir -p "$DEST" /bronze /silver /features /gold/_metadata

echo "Uploading $NAME CSVs to $DEST"
# The glob must expand inside the container, where the files live.
docker exec namenode bash -c "hdfs dfs -put -f $SRC/*.csv $DEST/"

echo "Done:"
hdfs dfs -ls -h "$DEST"
