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

Numerics are then filled and scaled with statistics from training rows only
(standard log, training dates): heavy-tailed counts get log1p, every numeric
is standardized, and nulls ("no information yet") become the training mean,
i.e. exactly 0 after scaling, with a <col>_missing flag beside them.

Stage 5 splits by date (split.* in config) and writes gold/:
- train, val, test:  standard-log rows (the recommender's own exposures).
- test_random:       random-log rows on test dates, an unbiased test set.
Random-log rows on train/val dates are not used. Rows are written in a
deterministic pseudo-random order (a hash of the event), so training batches
are not time-ordered and every run writes the same files.
gold/_metadata/feature_spec.json tells the training code what each column is.
"""

import json

from pyspark.sql import DataFrame, functions as F

from pipeline import quality_checks as qc
from pipeline import schemas as S
from pipeline.common import RunStats, build_spark, load_config, parse_args, write_text

EVENT_KEY = ["user_id", "video_id", "time_ms"]
ITEM_KEY = ["video_id", "event_date"]
STRATEGIES = ("broadcast", "salted", "sortmerge")

# Numeric features, by source table. Everything else in gold is an index.
NUMERIC = [
    # features/events
    "user_hist_cnt", "user_hist_ctr", "user_hist_like_rate", "user_hist_avg_watch_ratio",
    "user_active_days", "hist_len", "secs_since_last_event", "session_position",
    "user_author_prev_impr", "user_author_prev_clicks", "video_age_days",
    # features/user_static, features/video_static
    "follow_user_num", "fans_user_num", "friend_user_num", "register_days", "video_duration_s",
    # features/item_daily, features/author_daily
    "video_impr_prev", "video_ctr_prev", "video_avg_watch_ratio_prev",
    "author_impr_prev", "author_ctr_prev", "author_avg_watch_ratio_prev",
]
# Heavy-tailed counts and durations: log1p before standardizing.
LOG1P = [
    "user_hist_cnt", "user_active_days", "secs_since_last_event", "session_position",
    "user_author_prev_impr", "user_author_prev_clicks", "follow_user_num", "fans_user_num",
    "friend_user_num", "register_days", "video_duration_s", "video_impr_prev", "author_impr_prev",
]
# Null means "no information yet" (first event, first day, unknown duration).
NULLABLE = [
    "user_hist_ctr", "user_hist_like_rate", "user_hist_avg_watch_ratio", "secs_since_last_event",
    "video_duration_s", "video_ctr_prev", "video_avg_watch_ratio_prev",
    "author_ctr_prev", "author_avg_watch_ratio_prev",
]
STAT_DECIMALS = 9

# Categorical fields (embedding lookups) and where their vocabulary sizes come from.
_USER_CAT = [c for c in S.USER_STATIC.fieldNames() if c.endswith("_idx")]
_VIDEO_CAT = [c for c in S.VIDEO_STATIC.fieldNames() if c.endswith("_idx")]
CATEGORICAL = _USER_CAT + _VIDEO_CAT + ["tab_idx", "hour_of_day", "day_of_week"]
FIXED_VOCAB = {"hour_of_day": 24, "day_of_week": 8}   # raw values 0..23 and 1..7
FLAGS = [f"{c}_missing" for c in NULLABLE]
SEQUENCE = "hist_video_idx"                            # shares the video_id vocabulary
ID_COLS = EVENT_KEY                                    # kept for traceability, not features
SPLITS = ("train", "val", "test", "test_random")


def vocab_name(col: str) -> str:
    """vocab_sizes.json key for a categorical column, e.g. user_idx -> user_id."""
    base = col[:-len("_idx")]
    return f"{base}_id" if base in ("user", "video", "author", "music") else base


def gold_columns() -> list:
    return ["label", *ID_COLS, *CATEGORICAL, *NUMERIC, *FLAGS, SEQUENCE, "event_date"]


def split_rows(df: DataFrame, split_cfg: dict) -> dict:
    """Split by event date. Standard-log rows -> train/val/test; random-log rows
    on test dates -> test_random."""
    def dates(name):
        lo, hi = split_cfg[name]
        return F.col("event_date").between(lo, hi)
    standard, rand = F.col("is_rand") == 0, F.col("is_rand") == 1
    return {
        "train": df.filter(standard & dates("train")),
        "val": df.filter(standard & dates("val")),
        "test": df.filter(standard & dates("test")),
        "test_random": df.filter(rand & dates("test")),
    }


def shuffled(df: DataFrame, n_files: int, seed: int) -> DataFrame:
    """Deterministic pseudo-random order: hash the event key with a seed, spread
    rows over n_files partitions by that hash, and sort each by it.

    Partition by the hash itself: reducing it to n_files values first (pmod)
    would be hashed again by repartition, and n values hashed into n buckets
    collide, leaving some files empty (it gave 4 train files instead of 8).
    """
    h = F.xxhash64(*EVENT_KEY, F.lit(seed))
    return (df.withColumn("_h", h)
            .repartition(n_files, "_h")
            .sortWithinPartitions("_h")
            .drop("_h"))


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


def _transformed(col: str, log_cols: list):
    x = F.col(col).cast("double")
    # Clip at 0 so a stray negative count can never become log1p(-1) = -inf.
    return F.log1p(F.greatest(x, F.lit(0.0))) if col in log_cols else x


def fit_scaler(train: DataFrame, numeric: list, log_cols: list) -> dict:
    """Mean and standard deviation of each (log1p-transformed) numeric over
    training rows, ignoring nulls. Rounded to STAT_DECIMALS so the float sums'
    run-to-run last-bit differences cannot reach the scaled output."""
    aggs = []
    for c in numeric:
        x = _transformed(c, log_cols)
        aggs += [F.avg(x).alias(f"{c}__mean"), F.stddev_samp(x).alias(f"{c}__std")]
    row = train.agg(*aggs).first().asDict()
    stats = {}
    for c in numeric:
        mean, std = row[f"{c}__mean"], row[f"{c}__std"]
        stats[c] = {
            "log1p": c in log_cols,
            "mean": round(mean if mean is not None else 0.0, STAT_DECIMALS),
            # A constant (or all-null) column has no spread: divide by 1, not 0.
            "std": round(std, STAT_DECIMALS) if std else 1.0,
        }
    return stats


def apply_scaler(df: DataFrame, stats: dict, nullable: list) -> DataFrame:
    """Add <col>_missing flags, then replace each numeric with its scaled float:
    (transform(x) - mean) / std, with nulls set to 0 (the training mean)."""
    log_cols = [c for c, s in stats.items() if s["log1p"]]
    for c in nullable:
        df = df.withColumn(f"{c}_missing", F.col(c).isNull().cast("int"))
    for c, s in stats.items():
        scaled = (_transformed(c, log_cols) - F.lit(s["mean"])) / F.lit(s["std"])
        df = df.withColumn(c, F.coalesce(scaled, F.lit(0.0)).cast("float"))
    return df


def check_joined(joined: DataFrame, n_events: int, stats: RunStats) -> None:
    n = joined.count()
    stats.record("joined_rows", n)
    qc.check_equal_counts(n_events, n, "joined rows == event rows (no join fan-out)")
    qc.check_unique(joined, EVENT_KEY, "joined")
    # Every event must find its row in each table. Each column below is never
    # null in its own table, so a null here means the join found no match.
    qc.check_no_nulls(joined, ["user_idx", "video_idx", "video_impr_prev", "author_impr_prev"],
                      "joined match (user_static, video_static, item_daily, author_daily)")


def check_scaled(scaled: DataFrame, train_lo: int, train_hi: int) -> None:
    qc.check_no_nulls(scaled, NUMERIC + [f"{c}_missing" for c in NULLABLE], "scaled")
    qc.check_finite(scaled, NUMERIC, "scaled")
    # On the rows the statistics came from, every scaled numeric has mean 0
    # (nulls were set to the mean) and standard deviation at most 1 (filled
    # nulls add no spread).
    train = scaled.filter((F.col("is_rand") == 0) & F.col("event_date").between(train_lo, train_hi))
    row = train.agg(*[F.avg(c).alias(f"m_{c}") for c in NUMERIC],
                    *[F.stddev_samp(c).alias(f"s_{c}") for c in NUMERIC]).first()
    off_mean = {c: round(row[f"m_{c}"], 6) for c in NUMERIC if abs(row[f"m_{c}"]) > 1e-3}
    off_std = {c: round(row[f"s_{c}"], 4) for c in NUMERIC if not 0 < row[f"s_{c}"] <= 1.0 + 1e-3}
    qc.check_equal_counts(0, len(off_mean), f"scaled train columns with mean != 0 {off_mean or ''}")
    qc.check_equal_counts(0, len(off_std), f"scaled train columns with std outside (0, 1] {off_std or ''}")


def check_gold(spark, cfg: dict, spec: dict, scaled: DataFrame, stats: RunStats) -> None:
    gold = cfg["paths"]["gold"]
    parts = {name: spark.read.parquet(f"{gold}/{name}") for name in SPLITS}

    # Row accounting: every standard-log row lands in exactly one of
    # train/val/test, and test_random holds every random-log row on test dates.
    n_standard = scaled.filter("is_rand = 0").count()
    lo, hi = cfg["split"]["test"]
    n_random_test = scaled.filter((F.col("is_rand") == 1) & F.col("event_date").between(lo, hi)).count()
    qc.check_equal_counts(n_standard, sum(parts[n].count() for n in ("train", "val", "test")),
                          "gold train + val + test == standard-log rows")
    qc.check_equal_counts(n_random_test, parts["test_random"].count(), "gold test_random == random-log test rows")

    for name, df in parts.items():
        n_files, want = len(df.inputFiles()), cfg["gold"]["files"][name]
        qc.check_true(n_files == want, f"gold.{name} file count", f"{n_files} files (configured {want})")

    # Split integrity: strictly increasing, non-overlapping date ranges.
    rng = {n: parts[n].agg(F.min("event_date"), F.max("event_date")).first() for n in ("train", "val", "test")}
    qc.check_true(rng["train"][1] < rng["val"][0] and rng["val"][1] < rng["test"][0],
                  "gold split dates ordered train < val < test",
                  f"train ends {rng['train'][1]}, val {rng['val'][0]}..{rng['val'][1]}, test starts {rng['test'][0]}")

    for name, df in parts.items():
        # Randomly shown videos are clicked far less, so test_random has its own band.
        band = cfg["quality"]["label_rate_band_random" if name == "test_random" else "label_rate_band"]
        qc.check_schema(df, parts["train"].schema, f"gold.{name}")
        qc.check_no_nulls(df, df.columns, f"gold.{name}")
        rate = df.agg(F.avg("label")).first()[0]
        stats.record(f"{name}_label_rate", round(rate, 4))
        if not band[0] <= rate <= band[1]:
            print(f"[WARN] gold.{name} label rate {rate:.3f} outside expected band {band}")

    # Every index must fit its embedding table, or training fails (or worse,
    # silently reads the wrong row on some backends).
    for name, df in parts.items():
        maxes = df.agg(*[F.max(c["name"]).alias(c["name"]) for c in spec["categorical"]],
                       F.max(F.array_max(SEQUENCE)).alias(SEQUENCE),
                       F.min(F.size(SEQUENCE)).alias("min_len"), F.max(F.size(SEQUENCE)).alias("max_len")).first()
        too_big = {c["name"]: maxes[c["name"]] for c in spec["categorical"] if maxes[c["name"]] >= c["vocab_size"]}
        if maxes[SEQUENCE] >= spec["sequence"]["vocab_size"]:
            too_big[SEQUENCE] = maxes[SEQUENCE]
        qc.check_equal_counts(0, len(too_big), f"gold.{name} indices outside their vocabulary {too_big or ''}")
        n = spec["sequence"]["length"]
        qc.check_true(maxes["min_len"] == maxes["max_len"] == n, f"gold.{name} {SEQUENCE} length",
                      f"every list has length {n}" if maxes["min_len"] == maxes["max_len"] == n
                      else f"lengths range {maxes['min_len']}..{maxes['max_len']}, expected {n}")


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

    # Scaling statistics: training rows only (standard log, training dates).
    lo, hi = cfg["split"]["train"]
    train = joined.filter((F.col("is_rand") == 0) & F.col("event_date").between(lo, hi))
    scaler = fit_scaler(train, NUMERIC, LOG1P)
    write_text(spark, f"{cfg['paths']['metadata']}/scaling_stats.json", json.dumps(scaler, indent=2))
    scaled = apply_scaler(joined, scaler, NULLABLE).cache()
    check_scaled(scaled, lo, hi)
    joined.unpersist()

    # Stage 5: split and write gold.
    gold, meta = cfg["paths"]["gold"], cfg["paths"]["metadata"]
    labelled = scaled.withColumn("label", F.col(cfg["label"]).cast("int")).select(*gold_columns())
    counts = {}
    for name, part in split_rows(labelled, cfg["split"]).items():
        shuffled(part, cfg["gold"]["files"][name], cfg["seed"]).write.mode("overwrite").parquet(f"{gold}/{name}")
        counts[name] = spark.read.parquet(f"{gold}/{name}").count()
        stats.record(f"{name}_rows", counts[name])
    unused = n_events - sum(counts.values())
    stats.record("random_rows_on_train_val_dates_unused", unused)

    sizes = json.loads(spark.read.text(f"{meta}/vocab_sizes.json", wholetext=True).first()[0])
    spec = {
        "label": cfg["label"],
        "id_columns": ID_COLS,
        "categorical": [{"name": c, "vocab_size": FIXED_VOCAB.get(c) or sizes[vocab_name(c)]}
                        for c in CATEGORICAL],
        "numeric": NUMERIC,
        "flags": FLAGS,
        "sequence": {"name": SEQUENCE, "vocab_size": sizes["video_id"],
                     "length": cfg["features"]["history_length"], "pad": 0},
        "splits": {name: {"dates": cfg["split"]["test" if name == "test_random" else name],
                          "rows": counts[name]} for name in SPLITS},
    }
    write_text(spark, f"{meta}/feature_spec.json", json.dumps(spec, indent=2))
    check_gold(spark, cfg, spec, scaled, stats)

    stats.save(spark)
    spark.stop()


if __name__ == "__main__":
    main()
