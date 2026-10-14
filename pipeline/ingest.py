"""Stage 1, ingest: raw KuaiRand CSVs on HDFS -> typed Parquet (bronze).

    docker exec spark-master spark-submit /opt/project/pipeline/ingest.py

No rows are added or removed here; only types and storage change:
- CSVs are read with explicit schemas in FAILFAST mode, so a malformed row
  stops the job instead of silently becoming nulls.
- event_ts, event_hour and event_date are derived from time_ms in the dataset's
  time zone. The raw date/hourmin columns are coarse (logged up to ~50 minutes
  away from time_ms), so time_ms is the single source of truth for time.
- Interactions (standard and random logs, told apart by is_rand) are written
  partitioned by event_date; dimension tables as single files.
"""

from pyspark.sql import DataFrame, SparkSession, functions as F

from pipeline import quality_checks as qc
from pipeline import schemas as S
from pipeline.common import RunStats, build_spark, load_config, parse_args


def read_csv(spark: SparkSession, path: str, schema) -> DataFrame:
    return (spark.read
            .option("header", True)
            .option("mode", "FAILFAST")
            .schema(schema)
            .csv(path))


def to_bronze_interactions(df: DataFrame) -> DataFrame:
    ts = F.expr("timestamp_millis(time_ms)")
    return (df
            .withColumn("event_ts", ts)
            .withColumn("event_hour", F.hour(ts))
            .withColumn("event_date", F.date_format(ts, "yyyyMMdd").cast("int")))


def to_bronze_users(df: DataFrame) -> DataFrame:
    return df.select(*[F.col(f.name).cast(f.dataType) for f in S.BRONZE_USERS.fields])


def to_bronze_videos(df: DataFrame) -> DataFrame:
    return df.select(*[F.col(f.name).cast(f.dataType) for f in S.BRONZE_VIDEOS.fields])


def main() -> None:
    args = parse_args(__doc__)
    cfg = load_config(args.config)
    spark = build_spark("ingest", cfg)
    stats = RunStats("ingest", cfg)

    raw = f"{cfg['dataset']['hdfs_raw']}/{cfg['dataset']['version']}"
    bronze = cfg["paths"]["bronze"]
    max_records = cfg["write"]["max_records_per_file"]

    tables = [
        # name, raw file pattern, raw schema, transform, bronze schema
        ("interactions", "log_*.csv", S.RAW_LOG, to_bronze_interactions, S.BRONZE_INTERACTIONS),
        ("users", "user_features_*.csv", S.RAW_USERS, to_bronze_users, S.BRONZE_USERS),
        ("videos", "video_features_basic_*.csv", S.RAW_VIDEOS, to_bronze_videos, S.BRONZE_VIDEOS),
        ("video_stats", "video_features_statistic_*.csv", S.RAW_VIDEO_STATS, lambda d: d,
         S.BRONZE_VIDEO_STATS),
    ]

    for name, pattern, raw_schema, transform, bronze_schema in tables:
        src = read_csv(spark, f"{raw}/{pattern}", raw_schema)
        raw_rows = src.count()
        stats.record(f"{name}_raw_rows", raw_rows)

        out = transform(src)
        dest = f"{bronze}/{name}"
        if name == "interactions":
            # One shuffle by date so each date directory gets few, large files,
            # sorted by user and time for compression and stage-3 reads. The sort
            # leads with event_date because the partitioned writer needs that
            # order and would otherwise re-sort, discarding ours.
            (out.repartition("event_date").sortWithinPartitions("event_date", "user_id", "time_ms")
             .write.mode("overwrite")
             .option("maxRecordsPerFile", max_records)
             .partitionBy("event_date").parquet(dest))
        else:
            out.coalesce(1).write.mode("overwrite").parquet(dest)

        written = spark.read.parquet(dest)
        written_rows = written.count()
        stats.record(f"{name}_bronze_rows", written_rows)
        qc.check_schema(written, bronze_schema, f"bronze.{name}")
        qc.check_not_empty(written_rows, f"bronze.{name}")
        qc.check_equal_counts(raw_rows, written_rows, f"bronze.{name} rows == raw rows")
        if name == "interactions":
            # Null IDs/timestamps are allowed in bronze; clean.py drops them.
            dates = written.agg(F.min("event_date"), F.max("event_date")).first()
            stats.record("interactions_date_range", f"{dates[0]}..{dates[1]}")

    stats.save(spark)
    spark.stop()


if __name__ == "__main__":
    main()
