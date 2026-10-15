"""Stages 4-5, build dataset: feature tables -> gold train/val/test.

    docker exec spark-master spark-submit /opt/project/pipeline/build_dataset.py

Stage 4 joins every feature table onto features/events (one row per
interaction). The (video_id, event_date) join is the large, skewed one; how it
is executed is set by join.item_daily_strategy in config/pipeline.yaml:

- broadcast: copy item_daily to every executor; no shuffle. Best whenever the
  table fits in executor memory (3 MB in KuaiRand-Pure).
- salted:    split each key over salt_buckets partitions: events get a salt
  from a hash of the event, item_daily rows are replicated once per salt.
  For when both sides are too large to broadcast and hot keys stall a task.
- sortmerge: a plain shuffle join on the key (the baseline for benchmarks).

All three give identical rows (tests/test_build_dataset.py).
"""

from pyspark.sql import DataFrame, functions as F

from pipeline import quality_checks as qc
from pipeline.common import RunStats, build_spark, load_config, parse_args

EVENT_KEY = ["user_id", "video_id", "time_ms"]
ITEM_KEY = ["video_id", "event_date"]
STRATEGIES = ("broadcast", "salted", "sortmerge")


def join_item_daily(events: DataFrame, item_daily: DataFrame, strategy: str,
                    salt_buckets: int) -> DataFrame:
    """Left-join item_daily onto events on (video_id, event_date)."""
    if strategy == "broadcast":
        return events.join(F.broadcast(item_daily), ITEM_KEY, "left")
    if strategy == "sortmerge":
        return events.join(item_daily.hint("merge"), ITEM_KEY, "left")
    if strategy == "salted":
        # The salt comes from a hash of the event, not rand(), so every run
        # puts every row in the same bucket (deterministic output).
        salt = F.pmod(F.xxhash64(*EVENT_KEY), F.lit(salt_buckets)).cast("int")
        salted_events = events.withColumn("_salt", salt)
        salted_items = item_daily.withColumn("_salt", F.explode(F.sequence(F.lit(0), F.lit(salt_buckets - 1))))
        return (salted_events.join(salted_items.hint("merge"), ITEM_KEY + ["_salt"], "left")
                .drop("_salt"))
    raise ValueError(f"Unknown item_daily strategy {strategy!r}; use one of {STRATEGIES}")


def join_features(events: DataFrame, user_static: DataFrame, video_static: DataFrame,
                  item_daily: DataFrame, author_daily: DataFrame, strategy: str,
                  salt_buckets: int) -> DataFrame:
    """One row per event with every feature attached. Left joins, so a missing
    match shows up as nulls (caught by the match checks), never as lost rows."""
    joined = (events
              .join(F.broadcast(user_static), "user_id", "left")
              .join(F.broadcast(video_static), "video_id", "left"))
    joined = join_item_daily(joined, item_daily, strategy, salt_buckets)
    return joined.join(author_daily, ["author_id", "event_date"], "left")


def check_joined(joined: DataFrame, n_events: int, stats: RunStats) -> None:
    n = joined.count()
    stats.record("joined_rows", n)
    qc.check_equal_counts(n_events, n, "joined rows == event rows (no join fan-out)")
    qc.check_unique(joined, EVENT_KEY, "joined")
    # Every event must find its row in each table. Each column below is never
    # null in its own table, so a null here means the join found no match.
    qc.check_no_nulls(joined, ["user_idx", "video_idx", "video_impr_prev", "author_impr_prev"],
                      "joined match (user_static, video_static, item_daily, author_daily)")


def main() -> None:
    args = parse_args(__doc__)
    cfg = load_config(args.config)
    spark = build_spark("build_dataset", cfg)
    stats = RunStats("build_dataset", cfg)
    feat = cfg["paths"]["features"]
    strategy, salt_buckets = cfg["join"]["item_daily_strategy"], cfg["join"]["salt_buckets"]
    stats.record("item_daily_strategy", strategy)

    events = spark.read.parquet(f"{feat}/events")
    n_events = events.count()
    stats.record("events_in", n_events)
    joined = join_features(
        events,
        spark.read.parquet(f"{feat}/user_static"),
        spark.read.parquet(f"{feat}/video_static"),
        spark.read.parquet(f"{feat}/item_daily"),
        spark.read.parquet(f"{feat}/author_daily"),
        strategy, salt_buckets,
    ).cache()
    check_joined(joined, n_events, stats)
    stats.record("joined_columns", len(joined.columns))

    stats.save(spark)
    spark.stop()


if __name__ == "__main__":
    main()
