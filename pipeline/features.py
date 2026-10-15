"""Stage 3, features: silver -> feature tables + vocabularies.

    docker exec spark-master spark-submit /opt/project/pipeline/features.py

Outputs (all on HDFS, under paths.features unless noted):
- events/        one row per interaction: labels, user history, context and
                 cross features. Partitioned by event_date.
- item_daily/    per (video_id, event_date): video stats from previous days only.
- author_daily/  per (author_id, event_date): author stats from previous days only.
- user_static/   users with categorical columns encoded to indices.
- video_static/  videos with IDs and categorical columns encoded to indices.
- paths.metadata/vocab/<name>/ and vocab_sizes.json: the encodings.

Leakage rules (see README section 12):
- History windows end strictly before the event's timestamp (rangeBetween on
  time_ms), so events served in the same feed request never see each other.
- Item and author stats use previous days only; their click-rate prior is the
  global click rate of previous days.
- ID vocabularies are built from training-period events only.
Numeric scaling happens in stage 4, after the join, using training rows only.
"""

import json

from pyspark.sql import DataFrame, Window, functions as F

from pipeline import encoding as E
from pipeline import quality_checks as qc
from pipeline import schemas as S
from pipeline.common import RunStats, build_spark, load_config, parse_args, write_text

USER_CATEGORICAL = [
    "user_active_degree", "is_lowactive_period", "is_live_streamer", "is_video_author",
    "follow_user_num_range", "fans_user_num_range", "friend_user_num_range",
    "register_days_range", *S.ONEHOT_COLS,
]
USER_NUMERIC = ["follow_user_num", "fans_user_num", "friend_user_num", "register_days"]
VIDEO_CATEGORICAL = ["video_type", "upload_type", "music_type", "primary_tag"]
ID_COLS = ["user_id", "video_id", "author_id", "music_id"]

_PAST = (Window.unboundedPreceding, -1)


def _watch_ratio():
    """play_time / duration as an exact decimal (null if duration unknown).

    Sums of doubles depend on the order rows are added, and Spark does not fix
    the order of same-timestamp rows after a shuffle, so double sums differed
    in the last bits between rebuilds. Decimal addition is exact, so the result
    is the same in any order. Ratios are capped at 5, so 15 decimals fit.
    """
    return (F.col("play_time_ms") / F.col("duration_ms")).cast("decimal(20,15)")


# --- Event-level features ------------------------------------------------------

def add_user_history(df: DataFrame, history_length: int, tie_slack: int,
                     session_gap_s: float) -> DataFrame:
    """User history, session and user x author features, from strictly earlier events.

    Needs: user_id, video_id, video_idx, author_id, time_ms, event_date,
    is_click, is_like, play_time_ms, duration_ms.
    """
    by_user = Window.partitionBy("user_id")
    # Range frame on time_ms: everything strictly before this timestamp. A row
    # frame would let events from the same feed request see each other.
    past = by_user.orderBy("time_ms").rangeBetween(*_PAST)
    cnt = F.col("user_hist_cnt")

    def rate(col):
        return F.when(cnt > 0, F.col(col) / cnt)

    df = (df
          .withColumn("user_hist_cnt", F.count(F.lit(1)).over(past))
          .withColumn("_clicks", F.sum("is_click").over(past))
          .withColumn("_likes", F.sum("is_like").over(past))
          .withColumn("user_hist_ctr", rate("_clicks"))
          .withColumn("user_hist_like_rate", rate("_likes"))
          .withColumn("user_hist_avg_watch_ratio", F.avg(_watch_ratio()).over(past).cast("double"))
          # dense_rank counts distinct dates up to today; minus one = days before today.
          .withColumn("user_active_days", F.dense_rank().over(by_user.orderBy("event_date")) - 1)
          .withColumn("secs_since_last_event",
                      (F.col("time_ms") - F.max("time_ms").over(past)) / 1000))

    # Sessions: a new one starts after a gap longer than session_gap_s. The
    # running sum uses the default RANGE frame, so same-timestamp events share
    # a session; position counts strictly earlier events in the session.
    new_session = F.col("secs_since_last_event").isNull() | (F.col("secs_since_last_event") > session_gap_s)
    df = df.withColumn("_session", F.sum(new_session.cast("int")).over(by_user.orderBy("time_ms")))
    in_session = Window.partitionBy("user_id", "_session").orderBy("time_ms").rangeBetween(*_PAST)
    df = df.withColumn("session_position", (F.count(F.lit(1)).over(in_session) + 1).cast("int"))

    # Last N videos before this event. Look back N + slack rows, drop events at
    # the current timestamp, keep the last N, and left-pad with PAD to length N.
    n = history_length
    recent = by_user.orderBy("time_ms", "video_id").rowsBetween(-(n + tie_slack), -1)
    df = (df
          .withColumn("_recent", F.collect_list(F.struct("time_ms", "video_idx")).over(recent))
          .withColumn("_earlier", F.expr("transform(filter(_recent, x -> x.time_ms < time_ms), x -> x.video_idx)"))
          .withColumn("_last", F.expr(f"slice(_earlier, greatest(size(_earlier) - {n} + 1, 1), {n})"))
          .withColumn("hist_len", F.size("_last"))
          .withColumn("hist_video_idx", F.expr(f"concat(array_repeat({E.PAD}, {n} - size(_last)), _last)")))

    # User x author: how often this user saw / clicked this author before.
    ua = Window.partitionBy("user_id", "author_id").orderBy("time_ms").rangeBetween(*_PAST)
    df = (df
          .withColumn("user_author_prev_impr", F.count(F.lit(1)).over(ua))
          .withColumn("user_author_prev_clicks", F.coalesce(F.sum("is_click").over(ua), F.lit(0))))

    return df.drop("_clicks", "_likes", "_session", "_recent", "_earlier", "_last")


def add_context(df: DataFrame) -> DataFrame:
    """Needs: event_hour, event_date, upload_dt.

    Weekday and age come from event_date, the canonical date, so they can never
    disagree with it (event_ts would depend on the session time zone).
    """
    event_day = F.to_date(F.col("event_date").cast("string"), "yyyyMMdd")
    return (df
            .withColumn("hour_of_day", F.col("event_hour"))
            .withColumn("day_of_week", F.dayofweek(event_day))        # 1 = Sunday
            .withColumn("video_age_days", F.datediff(event_day, F.col("upload_dt"))))


# --- Item and author features ----------------------------------------------------

def daily_prev_stats(df: DataFrame, key: str, prefix: str, prior_strength: float) -> DataFrame:
    """Per (key, event_date): stats over all *previous* days, never the same day.

    The click rate is smoothed toward the global click rate of previous days:
    (clicks + a * prior) / (impressions + a). A key seen for the first time
    gets exactly the prior; on the first day of data there is no prior (null).
    """
    daily = (df.groupBy(key, "event_date")
             .agg(F.count(F.lit(1)).alias("impr"), F.sum("is_click").alias("clicks"),
                  F.sum(_watch_ratio()).alias("wr_sum"), F.count(_watch_ratio()).alias("wr_cnt")))
    prev = Window.partitionBy(key).orderBy("event_date").rowsBetween(*_PAST)

    def prev_sum(col):
        return F.coalesce(F.sum(col).over(prev), F.lit(0))

    stats = daily.select(key, "event_date", *[prev_sum(c).alias(f"{c}_prev")
                                               for c in ["impr", "clicks", "wr_sum", "wr_cnt"]])

    # A few dozen rows (one per date), so a single-partition window is fine.
    glob = df.groupBy("event_date").agg(F.count(F.lit(1)).alias("impr"), F.sum("is_click").alias("clicks"))
    g_prev = Window.orderBy("event_date").rowsBetween(*_PAST)
    prior = glob.select("event_date", (F.sum("clicks").over(g_prev) / F.sum("impr").over(g_prev)).alias("prior"))

    a = float(prior_strength)
    return (stats.join(F.broadcast(prior), "event_date")
            .select(key, "event_date",
                    F.col("impr_prev").alias(f"{prefix}_impr_prev"),
                    ((F.col("clicks_prev") + a * F.col("prior")) / (F.col("impr_prev") + a))
                    .alias(f"{prefix}_ctr_prev"),
                    F.when(F.col("wr_cnt_prev") > 0, F.col("wr_sum_prev") / F.col("wr_cnt_prev"))
                    .cast("double").alias(f"{prefix}_avg_watch_ratio_prev")))


# --- Static tables -------------------------------------------------------------

def encode_columns(df: DataFrame, cols: list, min_count: int, materialize) -> DataFrame:
    """Encode each column with a vocabulary built from df itself (dimension tables).

    All vocabularies are built from the unencoded table and materialized first.
    Building each one from the table as encoded so far would nest every
    earlier join into the next plan, doubling the plan size per column.
    """
    vocabs = {col: materialize(col, E.build_vocab(df, col, min_count)) for col in cols}
    for col in cols:
        df = E.apply_vocab(df, col, vocabs[col], f"{col}_idx")
    return df


# --- Stage -----------------------------------------------------------------------

def main() -> None:
    args = parse_args(__doc__)
    cfg = load_config(args.config)
    spark = build_spark("features", cfg)
    stats = RunStats("features", cfg)
    f = cfg["features"]
    silver, out, meta = cfg["paths"]["silver"], cfg["paths"]["features"], cfg["paths"]["metadata"]
    train_lo, train_hi = cfg["split"]["train"]
    min_count = f["min_count"]

    interactions = spark.read.parquet(f"{silver}/interactions")
    users = spark.read.parquet(f"{silver}/users")
    videos = (spark.read.parquet(f"{silver}/videos")
              .withColumn("primary_tag", F.col("tags")[0]))
    n_events = interactions.count()
    stats.record("events_in", n_events)

    events = interactions.join(videos.select("video_id", "author_id", "music_id", "upload_dt"), "video_id")
    train = events.filter(F.col("event_date").between(train_lo, train_hi))

    # Every vocabulary is saved, then read back, so later uses see exactly what
    # was saved and carry no lineage back to the data it was built from.
    vocabs = {}

    def materialize(name, vocab):
        vocab.coalesce(1).write.mode("overwrite").parquet(f"{meta}/vocab/{name}")
        vocabs[name] = spark.read.parquet(f"{meta}/vocab/{name}").cache()
        return vocabs[name]

    # ID and tab vocabularies: training-period events only.
    for col in ID_COLS + ["tab"]:
        materialize(col, E.build_vocab(train, col, min_count))

    # Event-level features: one shuffle by user, shared by every user window.
    ev = E.apply_vocab(events, "video_id", vocabs["video_id"], "video_idx")
    ev = E.apply_vocab(ev, "tab", vocabs["tab"], "tab_idx")
    ev = ev.repartition("user_id")
    ev = add_user_history(ev, f["history_length"], f["history_tie_slack"], f["session_gap_minutes"] * 60)
    ev = add_context(ev).select(*S.FEATURE_EVENTS.fieldNames())
    plan = ev._jdf.queryExecution().executedPlan().toString()
    stats.record("user_shuffles_in_event_plan", plan.count("hashpartitioning(user_id"))
    (ev.repartition("event_date").sortWithinPartitions("event_date", "user_id", "time_ms")
     .write.mode("overwrite").option("maxRecordsPerFile", cfg["write"]["max_records_per_file"])
     .partitionBy("event_date").parquet(f"{out}/events"))

    # Item and author stats
    a = f["ctr_prior_strength"]
    daily_prev_stats(events, "video_id", "video", a).write.mode("overwrite").parquet(f"{out}/item_daily")
    daily_prev_stats(events, "author_id", "author", a).write.mode("overwrite").parquet(f"{out}/author_daily")

    # Static tables
    u = E.apply_vocab(users, "user_id", vocabs["user_id"], "user_idx")
    u = encode_columns(u, USER_CATEGORICAL, min_count, materialize)
    u.select(*S.USER_STATIC.fieldNames()).coalesce(1).write.mode("overwrite").parquet(f"{out}/user_static")

    v = videos
    for col in ["video_id", "author_id", "music_id"]:
        v = E.apply_vocab(v, col, vocabs[col], f"{col.replace('_id', '')}_idx")
    v = encode_columns(v, VIDEO_CATEGORICAL, min_count, materialize)
    v = v.withColumn("video_duration_s", F.col("video_duration") / 1000.0)
    v.select(*S.VIDEO_STATIC.fieldNames()).coalesce(1).write.mode("overwrite").parquet(f"{out}/video_static")

    sizes = {name: E.vocab_size(vocab) for name, vocab in sorted(vocabs.items())}
    write_text(spark, f"{meta}/vocab_sizes.json", json.dumps(sizes, indent=2))
    stats.record("vocab_sizes", {k: sizes[k] for k in ID_COLS + ["tab"]})

    check_outputs(spark, cfg, n_events, users.count(), videos.count(), stats)
    stats.save(spark)
    spark.stop()


def check_outputs(spark, cfg, n_events: int, n_users: int, n_videos: int, stats: RunStats) -> None:
    out, n = cfg["paths"]["features"], cfg["features"]["history_length"]

    ev = spark.read.parquet(f"{out}/events")
    stats.record("events_out", ev.count())
    qc.check_schema(ev, S.FEATURE_EVENTS, "features.events")
    qc.check_equal_counts(n_events, stats.stats["events_out"], "features.events rows == silver rows (no join fan-out)")
    qc.check_unique(ev, ["user_id", "video_id", "time_ms"], "features.events")
    qc.check_no_nulls(ev, ["author_id", "user_hist_cnt", "user_active_days", "hist_video_idx", "hist_len",
                           "session_position", "tab_idx", "user_author_prev_impr", "user_author_prev_clicks",
                           "hour_of_day", "day_of_week", "video_age_days"], "features.events")
    for col in ["user_hist_ctr", "user_hist_like_rate"]:
        qc.check_range(ev, col, 0.0, 1.0, "features.events")
    qc.check_range(ev, "hist_len", 0, n, "features.events")
    qc.check_range(ev, "session_position", 1, 10**9, "features.events")
    qc.check_range(ev, "secs_since_last_event", 0.001, 10**9, "features.events")
    qc.check_range(ev, "video_age_days", 0, 10**5, "features.events")
    qc.check_equal_counts(0, ev.filter(F.size("hist_video_idx") != n).count(),
                          f"features.events hist_video_idx rows not of length {n}")
    # Leakage: events in the same feed request must see identical history, and
    # every user's first event must see none.
    qc.check_constant_within(ev, ["user_id", "time_ms"],
                             ["user_hist_cnt", "user_hist_ctr", "hist_video_idx", "session_position"],
                             "features.events same-timestamp events")
    qc.check_equal_counts(0, ev.groupBy("user_id").agg(F.min("user_hist_cnt").alias("m")).filter("m != 0").count(),
                          "features.events users whose first event has history")

    for name, key, prefix in [("item_daily", "video_id", "video"), ("author_daily", "author_id", "author")]:
        d = spark.read.parquet(f"{out}/{name}")
        stats.record(f"{name}_rows", d.count())
        qc.check_schema(d, getattr(S, f"FEATURE_{name.upper()}"), f"features.{name}")
        qc.check_unique(d, [key, "event_date"], f"features.{name}")
        qc.check_range(d, f"{prefix}_ctr_prev", 0.0, 1.0, f"features.{name}")
        qc.check_equal_counts(0, d.groupBy(key).agg(F.min(f"{prefix}_impr_prev").alias("m")).filter("m != 0").count(),
                              f"features.{name} keys whose first day has previous-day stats")

    for name, key, rows in [("user_static", "user_id", n_users), ("video_static", "video_id", n_videos)]:
        d = spark.read.parquet(f"{out}/{name}")
        qc.check_schema(d, getattr(S, name.upper()), f"features.{name}")
        qc.check_equal_counts(rows, d.count(), f"features.{name} rows == silver rows")
        qc.check_unique(d, [key], f"features.{name}")
        qc.check_no_nulls(d, [c for c in d.columns if c.endswith("_idx")], f"features.{name}")


if __name__ == "__main__":
    main()
