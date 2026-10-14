"""Profile the raw KuaiRand CSVs to ground the Phase 2 cleaning rules in evidence.

Run from the host:
    docker exec spark-master spark-submit /opt/project/scripts/profile_raw.py [pure|1k|27k]

Prints a plain-text report; nothing is written to HDFS. Uses inferSchema because
this is a one-off read; the pipeline itself uses explicit schemas.
"""

import sys

from pyspark.sql import SparkSession, functions as F

VERSION = sys.argv[1] if len(sys.argv) > 1 else "pure"
RAW = f"hdfs:///raw/kuairand/{VERSION}"


def section(title: str) -> None:
    print(f"\n=== {title} " + "=" * max(0, 60 - len(title)))


def read(spark, pattern):
    return spark.read.option("header", True).option("inferSchema", True).csv(f"{RAW}/{pattern}")


def profile_log(df, name):
    section(f"{name}: shape and nulls")
    total = df.count()
    print(f"rows: {total:,}")
    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first()
    print("null counts:", {c: v for c, v in nulls.asDict().items() if v} or "none")
    print("types:", ", ".join(f"{f.name}:{f.dataType.simpleString()}" for f in df.schema.fields))

    section(f"{name}: duplicates")
    key = ["user_id", "video_id", "time_ms"]
    dup_key = total - df.dropDuplicates(key).count()
    dup_full = total - df.dropDuplicates().count()
    print(f"duplicate (user_id, video_id, time_ms): {dup_key:,} ({dup_key / total:.4%})")
    print(f"fully identical rows:                   {dup_full:,} ({dup_full / total:.4%})")

    section(f"{name}: impossible values")
    for label, cond in [
        ("play_time_ms < 0", F.col("play_time_ms") < 0),
        ("duration_ms <= 0", F.col("duration_ms") <= 0),
        ("duration_ms is null", F.col("duration_ms").isNull()),
        ("time_ms <= 0", F.col("time_ms") <= 0),
    ]:
        n = df.filter(cond).count()
        print(f"{label:<22} {n:,}")

    section(f"{name}: watch ratio (play_time_ms / duration_ms)")
    wr = df.filter(F.col("duration_ms") > 0).select(
        (F.col("play_time_ms") / F.col("duration_ms")).alias("wr"))
    qs = [0.5, 0.9, 0.99, 0.999, 0.9999]
    vals = wr.approxQuantile("wr", qs, 0.0001)
    print("quantiles:", ", ".join(f"p{q * 100:g}={v:.2f}" for q, v in zip(qs, vals)))
    print("max:", round(wr.agg(F.max("wr")).first()[0], 1))
    for cap in [2, 3, 5, 10]:
        n = wr.filter(F.col("wr") > cap).count()
        print(f"ratio > {cap:>2}: {n:,} ({n / total:.4%})")

    section(f"{name}: timestamps vs date column")
    # KuaiRand dates are Beijing time (UTC+8).
    derived = F.date_format(F.from_unixtime(F.col("time_ms") / 1000, "yyyy-MM-dd HH:mm:ss")
                            .cast("timestamp") + F.expr("INTERVAL 8 HOURS"), "yyyyMMdd").cast("int")
    mismatch = df.filter(derived != F.col("date")).count()
    print(f"rows where date(time_ms, UTC+8) != date: {mismatch:,}")
    print("date range:", df.agg(F.min("date"), F.max("date")).first())
    hm = df.agg(F.min("hourmin"), F.max("hourmin")).first()
    print("hourmin range:", hm, "| distinct:", df.select("hourmin").distinct().count())

    section(f"{name}: labels and categorical values")
    print(df.agg(*[F.round(F.avg(c), 4).alias(c) for c in
                   ["is_click", "long_view", "is_like", "is_follow", "is_comment",
                    "is_forward", "is_hate", "is_profile_enter"]]).first().asDict())
    print("is_rand values:", [r[0] for r in df.select("is_rand").distinct().collect()])
    print("tab counts:", sorted((r["tab"], r["count"]) for r in df.groupBy("tab").count().collect()))

    section(f"{name}: events per user per day (bot check)")
    daily = df.groupBy("user_id", "date").count()
    qs = [0.5, 0.9, 0.99, 0.999]
    vals = daily.approxQuantile("count", qs, 0.0001)
    print("quantiles:", ", ".join(f"p{q * 100:g}={v:.0f}" for q, v in zip(qs, vals)))
    print("max:", daily.agg(F.max("count")).first()[0])
    p999 = vals[-1]
    heavy = daily.filter(F.col("count") > p999)
    heavy_users = heavy.select("user_id").distinct()
    n_users = df.select("user_id").distinct().count()
    n_heavy = heavy_users.count()
    rows_lost = df.join(heavy_users, "user_id").count()
    print(f"users with any day above p99.9 ({p999:.0f}): {n_heavy:,} of {n_users:,}")
    print(f"rows removed if those users are dropped entirely: {rows_lost:,} ({rows_lost / total:.2%})")
    rows_days = heavy.agg(F.sum("count")).first()[0] or 0
    print(f"rows removed if only those user-days are dropped:  {rows_days:,} ({rows_days / total:.2%})")
    return df


def profile_dim(df, name, key):
    section(f"{name}")
    total = df.count()
    distinct = df.select(key).distinct().count()
    print(f"rows: {total:,} | distinct {key}: {distinct:,} | duplicate keys: {total - distinct:,}")
    nulls = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]).first()
    print("null counts:", {c: v for c, v in nulls.asDict().items() if v} or "none")
    print("types:", ", ".join(f"{f.name}:{f.dataType.simpleString()}" for f in df.schema.fields))
    for f in df.schema.fields:
        if f.dataType.simpleString() == "string" and f.name != key:
            vals = [r[0] for r in df.select(f.name).distinct().limit(12).collect()]
            print(f"  {f.name}: {df.select(f.name).distinct().count()} distinct, e.g. {vals}")
    # Float columns that might really be integers (e.g. IDs stored as 9.0)
    for f in df.schema.fields:
        if f.dataType.simpleString() == "double":
            frac = df.filter(F.col(f.name).isNotNull() & (F.col(f.name) != F.floor(f.name))).count()
            if frac == 0:
                print(f"  {f.name}: double but all values are whole numbers")


def main() -> int:
    spark = SparkSession.builder.appName(f"profile-raw-{VERSION}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    std = profile_log(read(spark, "log_standard_*.csv").cache(), "standard log")
    rnd = profile_log(read(spark, "log_random_*.csv").cache(), "random log")

    users = read(spark, "user_features_*.csv")
    videos = read(spark, "video_features_basic_*.csv")
    profile_dim(users, "user features", "user_id")
    profile_dim(videos, "video basic features", "video_id")

    section("referential integrity")
    for name, log in [("standard", std), ("random", rnd)]:
        miss_u = log.join(users, "user_id", "left_anti").count()
        miss_v = log.join(videos, "video_id", "left_anti").count()
        print(f"{name}: rows with user_id not in users: {miss_u:,} | video_id not in videos: {miss_v:,}")
    overlap = std.select("user_id", "video_id", "time_ms").intersect(
        rnd.select("user_id", "video_id", "time_ms")).count()
    print(f"events present in both standard and random logs: {overlap:,}")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
