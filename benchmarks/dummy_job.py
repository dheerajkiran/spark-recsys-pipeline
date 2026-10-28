"""Tiny cluster job for checking the benchmark harness: one shuffle, fixed output."""

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    spark = SparkSession.builder.appName("benchmark-dummy").getOrCreate()
    counts = (spark.range(2_000_000).groupBy((F.col("id") % 100).alias("key")).count()
              .orderBy("key").collect())
    assert len(counts) == 100 and all(row["count"] == 20_000 for row in counts)
    spark.stop()


if __name__ == "__main__":
    main()
