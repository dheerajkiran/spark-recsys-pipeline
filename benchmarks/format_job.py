"""One file-format benchmark run: a query on the raw CSV logs or on bronze Parquet.

    spark-submit benchmarks/format_job.py <pipeline config> --format csv|parquet --query NAME

Both formats hold the same interactions: bronze is the raw logs plus columns
derived from time_ms (pipeline/ingest.py checks the row counts match). The CSV
side is read with the explicit raw schema (no inference) and gets event_date
from ingest.to_bronze_interactions, so every query sees the same columns and
rows in both formats. Queries:

- all_columns: sum of every raw column; nothing can be skipped.
- two_columns: per-user impressions and clicks; Parquet reads 2 of 22 columns.
- date_range:  the last 7 of 32 days; Parquet skips the other date directories.
- one_user:    one user's events; Parquet can skip row groups by their
               user_id statistics (files are sorted by user within a date).

Prints RESULT {json}: the query's own time (the action, not Spark start-up)
and the result's row count and checksum, which must match across formats.
"""

import argparse
import json
import time

from pyspark.sql import functions as F

from benchmarks.output_checksum import checksum
from pipeline import schemas as S
from pipeline.common import build_spark, load_config
from pipeline.ingest import read_csv, to_bronze_interactions

RAW_COLUMNS = [f.name for f in S.RAW_LOG.fields]
DATE_RANGE = (20220502, 20220508)
USER = 355   # the median user by event count (8,384 events)


def all_columns(df):
    return df.agg(F.count("*").alias("rows"), *[F.sum(c).alias(c) for c in RAW_COLUMNS])


def two_columns(df):
    return df.groupBy("user_id").agg(F.count("*").alias("impressions"), F.sum("is_click").alias("clicks"))


def date_range(df):
    return (df.filter(F.col("event_date").between(*DATE_RANGE))
            .agg(F.count("*").alias("rows"), F.sum("is_click").alias("clicks"),
                 F.sum("play_time_ms").alias("play_time_ms")))


def one_user(df):
    return (df.filter(F.col("user_id") == USER)
            .agg(F.count("*").alias("rows"), F.sum("is_click").alias("clicks"),
                 F.min("time_ms").alias("first_ms"), F.max("time_ms").alias("last_ms")))


QUERIES = {"all_columns": all_columns, "two_columns": two_columns, "date_range": date_range, "one_user": one_user}


def source(spark, cfg: dict, fmt: str):
    if fmt == "csv":
        raw = f"{cfg['dataset']['hdfs_raw']}/{cfg['dataset']['version']}"
        return to_bronze_interactions(read_csv(spark, f"{raw}/log_*.csv", S.RAW_LOG))
    return spark.read.parquet(f"{cfg['paths']['bronze']}/interactions")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config")
    parser.add_argument("--format", choices=["csv", "parquet"], required=True)
    parser.add_argument("--query", choices=QUERIES, required=True)
    args = parser.parse_args()
    cfg = load_config(args.config)
    spark = build_spark("benchmark-format", cfg)
    spark.sparkContext.setLogLevel("ERROR")
    result = QUERIES[args.query](source(spark, cfg, args.format))
    start = time.perf_counter()
    # The result is tiny, so checksumming it is the one action that runs the query.
    out = checksum(result)
    seconds = round(time.perf_counter() - start, 3)
    print("RESULT " + json.dumps({"query_seconds": seconds, **out}, sort_keys=True), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
