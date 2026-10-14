"""Stage 2, clean: bronze -> silver.

    docker exec spark-master spark-submit /opt/project/pipeline/clean.py

Interaction rules, in order (each logs how many rows it removed or changed).
The thresholds come from config/pipeline.yaml; the evidence behind each rule
is in README section 8 and scripts/profile_raw.py.

1. Drop rows with a null user_id, video_id or time_ms.
2. Collapse duplicate events (same user, video, time_ms) into one row. Copies
   are the same event logged twice; a later copy sometimes has more engagement,
   so each outcome column takes its max across copies.
3. Drop physically impossible rows: negative play time or duration.
4. Treat duration_ms == 0 as unknown (null). These are real impressions of
   videos whose length was never recorded, so the rows are kept.
5. Cap play_time_ms at max_watch_ratio x duration_ms (looping videos).
6. Drop bot-like user-days: implausibly fast (many events with a tiny median
   gap) or implausibly long (more play time than a person can watch).

Dimension tables keep every row; missing categories become UNKNOWN and missing
integer codes become -1, so missingness stays visible to the model.
"""

from pyspark.sql import DataFrame, Window, functions as F

from pipeline import quality_checks as qc
from pipeline import schemas as S
from pipeline.common import RunStats, build_spark, load_config, parse_args

EVENT_KEY = ["user_id", "video_id", "time_ms"]


# --- Interaction rules ---------------------------------------------------------

def drop_missing_keys(df: DataFrame) -> DataFrame:
    return df.dropna(subset=EVENT_KEY)


def dedupe_events(df: DataFrame) -> DataFrame:
    others = [c for c in df.columns if c not in EVENT_KEY]
    return (df.groupBy(*EVENT_KEY)
            .agg(*[F.max(c).alias(c) for c in others])
            .select(*df.columns))


def drop_impossible(df: DataFrame) -> DataFrame:
    return df.filter(~(F.col("play_time_ms") < 0) & ~(F.col("duration_ms") < 0))


def null_unknown_duration(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "duration_ms",
        F.when(F.col("duration_ms") == 0, F.lit(None).cast("long")).otherwise(F.col("duration_ms")))


def cap_play_time(df: DataFrame, max_ratio: float) -> DataFrame:
    cap = (F.col("duration_ms") * max_ratio).cast("long")
    return df.withColumn(
        "play_time_ms",
        F.when(F.col("play_time_ms") > cap, cap).otherwise(F.col("play_time_ms")))


def flag_bot_user_days(df: DataFrame, min_events: int, max_median_gap_s: float,
                       max_play_hours: float) -> DataFrame:
    """Return (user_id, event_date) pairs whose activity is not humanly plausible."""
    w = Window.partitionBy("user_id", "event_date").orderBy("time_ms")
    gap_s = (F.col("time_ms") - F.lag("time_ms").over(w)) / 1000
    daily = (df.select("user_id", "event_date", "play_time_ms", gap_s.alias("gap_s"))
             .groupBy("user_id", "event_date")
             .agg(F.count("*").alias("events"),
                  F.percentile_approx("gap_s", 0.5).alias("median_gap_s"),
                  (F.sum("play_time_ms") / 3.6e6).alias("play_hours")))
    too_fast = (F.col("events") >= min_events) & (F.col("median_gap_s") < max_median_gap_s)
    too_long = F.col("play_hours") > max_play_hours
    return daily.filter(too_fast | too_long).select("user_id", "event_date")


def remove_bot_user_days(df: DataFrame, bots: DataFrame) -> DataFrame:
    return df.join(F.broadcast(bots), ["user_id", "event_date"], "left_anti").select(*df.columns)


# --- Dimension tables ---------------------------------------------------------

def fill_missing(df: DataFrame, key: str) -> DataFrame:
    """Strings -> UNKNOWN, integer codes -> -1. Key and wider numerics untouched."""
    fills = {}
    for f in df.schema.fields:
        if f.name == key:
            continue
        kind = f.dataType.simpleString()
        if kind == "string":
            fills[f.name] = S.UNKNOWN
        elif kind == "int":
            fills[f.name] = S.MISSING_INT
    return df.fillna(fills)


def clean_videos(df: DataFrame) -> DataFrame:
    # video_duration (long) stays null when unknown, matching interactions.
    out = fill_missing(df, "video_id")
    tags = F.when(F.col("tag").isNull() | (F.col("tag") == S.UNKNOWN), F.array().cast("array<int>")) \
            .otherwise(F.split("tag", ",").cast("array<int>"))
    return out.withColumn("tags", tags).drop("tag")


# --- Stage ---------------------------------------------------------------------

def clean_interactions(df: DataFrame, cfg: dict, stats: RunStats) -> DataFrame:
    c = cfg["clean"]
    n = df.count()
    stats.record("interactions_in", n)
    previous = None

    def step(name, out, changed=None):
        # Cache each step's output so the next rule and its count reuse it,
        # and release the step before it.
        nonlocal n, previous
        out = out.cache()
        m = out.count()
        stats.record(f"removed_{name}", n - m)
        if changed is not None:
            stats.record(f"changed_{name}", changed)
        if previous is not None:
            previous.unpersist()
        n, previous = m, out
        return out

    df = step("missing_keys", drop_missing_keys(df))
    df = step("duplicates", dedupe_events(df))
    df = step("impossible_values", drop_impossible(df))
    df = step("unknown_duration", null_unknown_duration(df),
              changed=df.filter(F.col("duration_ms") == 0).count())
    df = step("play_time_cap", cap_play_time(df, c["max_watch_ratio"]),
              changed=df.filter(F.col("play_time_ms") > F.col("duration_ms") * c["max_watch_ratio"]).count())
    bots = flag_bot_user_days(df, c["bot_min_events_per_day"], c["bot_max_median_gap_seconds"],
                              c["bot_max_play_hours_per_day"]).cache()
    stats.record("bot_user_days", bots.count())
    df = step("bot_user_days", remove_bot_user_days(df, bots))
    return df


def main() -> None:
    args = parse_args(__doc__)
    cfg = load_config(args.config)
    spark = build_spark("clean", cfg)
    stats = RunStats("clean", cfg)
    bronze, silver = cfg["paths"]["bronze"], cfg["paths"]["silver"]
    max_records = cfg["write"]["max_records_per_file"]

    # Interactions
    src = spark.read.parquet(f"{bronze}/interactions").cache()
    cleaned = clean_interactions(src, cfg, stats).select(*S.SILVER_INTERACTIONS.fieldNames())
    # Sorting within each date keeps a user's events together and in time
    # order: smaller files (better compression) and the order stage 3 reads in.
    (cleaned.repartition("event_date").sortWithinPartitions("event_date", "user_id", "time_ms")
     .write.mode("overwrite")
     .option("maxRecordsPerFile", max_records)
     .partitionBy("event_date").parquet(f"{silver}/interactions"))

    out = spark.read.parquet(f"{silver}/interactions")
    before, after = stats.stats["interactions_in"], out.count()
    stats.record("interactions_out", after)
    qc.check_schema(out, S.SILVER_INTERACTIONS, "silver.interactions")
    qc.check_not_empty(after, "silver.interactions")
    qc.check_removed_fraction(before, after, cfg["clean"]["max_removed_fraction"], "silver.interactions")
    qc.check_unique(out, EVENT_KEY, "silver.interactions")
    qc.check_no_nulls(out, EVENT_KEY + ["event_ts", "event_date", "play_time_ms"], "silver.interactions")
    ratio = out.withColumn("watch_ratio", F.col("play_time_ms") / F.col("duration_ms"))
    qc.check_range(ratio, "watch_ratio", 0.0, cfg["clean"]["max_watch_ratio"], "silver.interactions")
    for label in ["is_click", "long_view", "is_rand"]:
        qc.check_range(out, label, 0, 1, "silver.interactions")

    # Dimension tables: no rows dropped
    for name, transform, schema, key in [
        ("users", lambda d: fill_missing(d, "user_id"), S.SILVER_USERS, "user_id"),
        ("videos", clean_videos, S.SILVER_VIDEOS, "video_id"),
    ]:
        src_dim = spark.read.parquet(f"{bronze}/{name}")
        transform(src_dim).coalesce(1).write.mode("overwrite").parquet(f"{silver}/{name}")
        dim = spark.read.parquet(f"{silver}/{name}")
        rows = dim.count()
        stats.record(f"{name}_out", rows)
        qc.check_schema(dim, schema, f"silver.{name}")
        qc.check_equal_counts(src_dim.count(), rows, f"silver.{name} rows == bronze rows")
        qc.check_unique(dim, [key], f"silver.{name}")

    stats.save(spark)
    spark.stop()


if __name__ == "__main__":
    main()
