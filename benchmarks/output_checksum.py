"""Row count and order-independent value checksum of the pipeline's outputs.

    spark-submit benchmarks/output_checksum.py <pipeline config>

Prints one line, CHECKSUM {json}, with silver interactions and each gold
split. Two builds with the same values give the same checksums whatever their
row order or file layout, so benchmark variants can be compared for
correctness.
"""

import json
import sys

from pyspark.sql import SparkSession, functions as F

from pipeline.common import load_config

SPLITS = ["train", "val", "test", "test_random"]


def checksum(df) -> dict:
    # Sum of per-row 64-bit hashes, as an exact decimal so it cannot overflow.
    row = df.select(F.count("*").alias("rows"),
                    F.sum(F.xxhash64(*sorted(df.columns)).cast("decimal(38,0)")).alias("sum")).first()
    return {"rows": row["rows"], "checksum": str(row["sum"])}


def main() -> None:
    cfg = load_config(sys.argv[1])
    spark = SparkSession.builder.appName("benchmark-checksum").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    tables = {"silver/interactions": f"{cfg['paths']['silver']}/interactions",
              **{f"gold/{split}": f"{cfg['paths']['gold']}/{split}" for split in SPLITS}}
    result = {name: checksum(spark.read.parquet(path)) for name, path in tables.items()}
    print("CHECKSUM " + json.dumps(result, sort_keys=True), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
