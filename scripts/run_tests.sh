#!/usr/bin/env bash
# Run the unit tests inside the spark-master container, which has the same
# Python, Java and PySpark versions as the cluster.
#
# Usage: ./scripts/run_tests.sh [pytest args]   e.g. ./scripts/run_tests.sh -k clean

set -euo pipefail

# The project is mounted read-only, so no bytecode or pytest cache is written.
docker exec \
  -e PYTHONPATH=/opt/project:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip \
  -e PYTHONDONTWRITEBYTECODE=1 \
  spark-master python3 -m pytest -p no:cacheprovider -q /opt/project/tests "$@"
