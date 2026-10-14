#!/usr/bin/env bash
# Start one HDFS daemon in the foreground: namenode or datanode.
set -euo pipefail

ROLE="${1:-}"

case "$ROLE" in
  namenode)
    # Format only on first start; the name directory lives on a Docker volume,
    # so later restarts keep all HDFS metadata.
    if [[ ! -d /hadoop/dfs/name/current ]]; then
      echo "Formatting new HDFS namespace"
      hdfs namenode -format -nonInteractive -clusterId "${CLUSTER_ID:-recsys}"
    fi
    exec hdfs namenode
    ;;
  datanode)
    exec hdfs datanode
    ;;
  *)
    # Anything else runs as a command, e.g. `docker compose run namenode hdfs dfs -ls /`.
    exec "$@"
    ;;
esac
