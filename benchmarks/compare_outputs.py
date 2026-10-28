"""Compare the Spark and pandas workload outputs row by row.

    spark-submit benchmarks/compare_outputs.py <spark output dir> <pandas output dir>

Both directories hold events and item_daily (Spark as directories, pandas as
single .parquet files). Rows are matched on their key; a row only one side has
counts as missing. Values must be equal, with floats within a relative 1e-9
(the engines add floats in different orders). Prints one line, COMPARE {json};
the caller decides what a difference means.
"""

import json
import sys

from pyspark.sql import SparkSession, functions as F

TABLES = {"events": ["user_id", "video_id", "time_ms"], "item_daily": ["video_id", "event_date"]}
TOLERANCE = 1e-9


def compare(spark_df, pandas_df, key) -> dict:
    values = [c for c in spark_df.columns if c not in key]
    if sorted(spark_df.columns) != sorted(pandas_df.columns):
        return {"columns_match": False}
    left = spark_df.select(*key, *[F.col(c).cast("double").alias(f"s_{c}") for c in values], F.lit(1).alias("_s"))
    right = pandas_df.select(*key, *[F.col(c).cast("double").alias(f"p_{c}") for c in values], F.lit(1).alias("_p"))
    joined = left.join(right, key, "full_outer")

    def differs(c):
        s, p = F.col(f"s_{c}"), F.col(f"p_{c}")
        both_null, one_null = s.isNull() & p.isNull(), s.isNull() | p.isNull()
        far = F.abs(s - p) > F.lit(TOLERANCE) * F.greatest(F.lit(1.0), F.abs(s))
        return F.when(both_null, False).when(one_null, True).otherwise(far)

    row = joined.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("_p").isNull(), 1).otherwise(0)).alias("only_spark"),
        F.sum(F.when(F.col("_s").isNull(), 1).otherwise(0)).alias("only_pandas"),
        *[F.sum(F.when(F.col("_s").isNotNull() & F.col("_p").isNotNull() & differs(c), 1).otherwise(0))
          .alias(c) for c in values]).first().asDict()
    counts = {c: row.pop(c) for c in values}
    return {"columns_match": True, **row, "mismatched_values": {c: n for c, n in counts.items() if n}}


def main() -> None:
    spark_dir, pandas_dir = sys.argv[1:3]
    spark = SparkSession.builder.appName("benchmark-compare").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    result = {name: compare(spark.read.parquet(f"{spark_dir}/{name}"),
                            spark.read.parquet(f"{pandas_dir}/{name}.parquet"), key)
              for name, key in TABLES.items()}
    result["identical"] = all(r["columns_match"] and not (r["only_spark"] or r["only_pandas"] or r["mismatched_values"])
                              for r in result.values())
    print("COMPARE " + json.dumps(result, sort_keys=True), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
