"""Phase 1 smoke test: prove Spark can read from and write to HDFS on all workers.

Run from the host:
    docker exec spark-master spark-submit /opt/project/scripts/smoke_test.py

Reads the KuaiRand-Pure standard logs from HDFS, counts rows per date, writes
the result as Parquet, reads it back and checks that nothing was lost. Exits
non-zero if any check fails.
"""

import sys

from pyspark.sql import SparkSession, functions as F

RAW = "hdfs:///raw/kuairand/pure/log_standard_*.csv"
OUT = "hdfs:///tmp/smoke_test/daily_counts"
EXPECTED_WORKERS = 3
EXPECTED_REPLICATION = 2


def check(ok: bool, message: str) -> bool:
    print(f"[{'PASS' if ok else 'FAIL'}] {message}")
    return ok


def main() -> int:
    spark = SparkSession.builder.appName("phase1-smoke-test").getOrCreate()
    sc = spark.sparkContext
    results = []

    # Executors register asynchronously; the first job may start before all
    # three have joined, so count them after a job has run.
    logs = spark.read.option("header", True).csv(RAW)
    total = logs.count()
    results.append(check(total > 0, f"read {total:,} rows from {RAW}"))

    executors = sc._jsc.sc().getExecutorMemoryStatus().size() - 1  # minus the driver
    results.append(check(executors == EXPECTED_WORKERS,
                         f"{executors} executors registered (expected {EXPECTED_WORKERS})"))

    daily = logs.groupBy("date").agg(F.count("*").alias("rows")).orderBy("date")
    daily.write.mode("overwrite").parquet(OUT)

    back = spark.read.parquet(OUT)
    back_total = back.agg(F.sum("rows")).first()[0]
    results.append(check(back_total == total,
                         f"Parquet round trip: {back_total:,} rows across {back.count()} dates"))

    # Check the replication factor of the files Spark just wrote.
    jvm = sc._jvm
    path = jvm.org.apache.hadoop.fs.Path(OUT)
    fs = path.getFileSystem(sc._jsc.hadoopConfiguration())
    parts = [s for s in fs.listStatus(path) if s.getPath().getName().endswith(".parquet")]
    replications = {s.getReplication() for s in parts}
    results.append(check(replications == {EXPECTED_REPLICATION},
                         f"written files have replication {sorted(replications)} "
                         f"(expected {EXPECTED_REPLICATION})"))

    back.show(40, truncate=False)
    spark.stop()

    passed = all(results)
    print("SMOKE TEST PASSED" if passed else "SMOKE TEST FAILED")
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
