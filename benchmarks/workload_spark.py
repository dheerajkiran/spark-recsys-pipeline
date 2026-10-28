"""Spark version of the Spark-vs-pandas workload: ingest, clean, feature subset.

    spark-submit benchmarks/workload_spark.py <pipeline config> <log dir> <videos csv> <output dir>

The same steps as benchmarks/workload_pandas.py, using the pipeline's own
functions: ingest.read_csv and to_bronze_interactions, clean.clean_interactions
(all six rules, with its per-step counts), features.daily_prev_stats, and the
user history counts with the same window frames as features.add_user_history.
Writes events/ and item_daily/ as Parquet and prints RESULT {json} with row counts.
"""

import json
import sys

from pyspark.sql import Window, functions as F

from pipeline import schemas as S
from pipeline.clean import clean_interactions
from pipeline.common import RunStats, build_spark, load_config
from pipeline.features import _PAST, daily_prev_stats
from pipeline.ingest import read_csv, to_bronze_interactions, to_bronze_videos

EVENT_COLUMNS = ["user_id", "video_id", "time_ms", "event_date", "event_hour", "is_click",
                 "play_time_ms", "duration_ms", "user_hist_cnt", "user_hist_ctr",
                 "secs_since_last_event", "user_active_days"]


def add_user_history(df):
    """The count, click-rate, recency and active-day columns of features.add_user_history."""
    by_user = Window.partitionBy("user_id")
    past = by_user.orderBy("time_ms").rangeBetween(*_PAST)
    cnt = F.col("user_hist_cnt")
    return (df
            .withColumn("user_hist_cnt", F.count(F.lit(1)).over(past))
            .withColumn("_clicks", F.sum("is_click").over(past))
            .withColumn("user_hist_ctr", F.when(cnt > 0, F.col("_clicks") / cnt))
            .withColumn("user_active_days", F.dense_rank().over(by_user.orderBy("event_date")) - 1)
            .withColumn("secs_since_last_event", (F.col("time_ms") - F.max("time_ms").over(past)) / 1000)
            .drop("_clicks"))


def transform(cfg: dict, events, videos, stats: RunStats):
    """Raw log rows and bronze videos -> (event features, item_daily, silver)."""
    silver = clean_interactions(to_bronze_interactions(events), videos, cfg, stats)
    features = add_user_history(silver).select(*EVENT_COLUMNS)
    item_daily = daily_prev_stats(silver, "video_id", "video", cfg["features"]["ctr_prior_strength"])
    return features, item_daily, silver


def main() -> None:
    config, log_dir, videos_path, out_dir = sys.argv[1:5]
    cfg = load_config(config)
    spark = build_spark("benchmark-workload", cfg)
    stats = RunStats("workload", cfg)
    events = read_csv(spark, f"{log_dir}/log_*.csv", S.RAW_LOG)
    videos = to_bronze_videos(read_csv(spark, videos_path, S.RAW_VIDEOS))
    features, item_daily, silver = transform(cfg, events, videos, stats)
    features.write.mode("overwrite").parquet(f"{out_dir}/events")
    item_daily.write.mode("overwrite").parquet(f"{out_dir}/item_daily")
    result = {"interactions_in": stats.stats["interactions_in"], "bot_user_days": stats.stats["bot_user_days"],
              "silver_rows": silver.count(), "item_daily_rows": spark.read.parquet(f"{out_dir}/item_daily").count()}
    print("RESULT " + json.dumps(result, sort_keys=True), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
