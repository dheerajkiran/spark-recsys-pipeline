"""One skew-benchmark run: join item_daily onto events, print an output checksum.

    spark-submit benchmarks/skew_job.py <pipeline config> --strategy sortmerge|salted
        [--hot-fraction F --hot-key VIDEO_ID:EVENT_DATE] [--salt-buckets S]

Runs the pipeline's own join (build_dataset.join_item_daily) on the real
features/events and features/item_daily tables. Broadcast is never used, so
the join always shuffles both sides. AQE settings come from spark-submit
--conf, so the same script serves every variant.

With --hot-fraction, that share of events (picked by a hash of the event, so
the same rows every run) is moved to one (video_id, event_date) key: a
constructed viral video, for when the real keys are too balanced to show skew.

Prints one line, CHECKSUM {json}: row count and an order-independent value
checksum of the join output. The checksum is the job's only action, so the
timed run is the join itself.
"""

import argparse
import json

from pyspark.sql import SparkSession, functions as F

from benchmarks.output_checksum import checksum
from pipeline.build_dataset import EVENT_KEY, join_item_daily
from pipeline.common import load_config


def with_hot_key(events, fraction: float, video_id: int, event_date: int):
    """Move `fraction` of events to one key, chosen by a hash of the event."""
    # The flag is computed once: rehashing after video_id changes would pick
    # different rows for event_date.
    hot = F.pmod(F.xxhash64(*EVENT_KEY), F.lit(1_000_000)) < F.lit(round(fraction * 1_000_000))
    return (events
            .withColumn("_hot", hot)
            .withColumn("video_id", F.when(F.col("_hot"), F.lit(video_id)).otherwise(F.col("video_id")))
            .withColumn("event_date", F.when(F.col("_hot"), F.lit(event_date)).otherwise(F.col("event_date")))
            .drop("_hot"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config")
    parser.add_argument("--strategy", choices=["sortmerge", "salted"], required=True)
    parser.add_argument("--hot-fraction", type=float, default=0.0)
    parser.add_argument("--hot-key", help="VIDEO_ID:EVENT_DATE, required with --hot-fraction")
    parser.add_argument("--salt-buckets", type=int, default=8)
    args = parser.parse_args()
    if not 0 <= args.hot_fraction < 1 or (args.hot_fraction > 0) != bool(args.hot_key):
        parser.error("--hot-fraction must be in [0, 1) and needs --hot-key")

    cfg = load_config(args.config)
    spark = (SparkSession.builder.appName("benchmark-skew")
             .config("spark.sql.autoBroadcastJoinThreshold", "-1").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    feat = cfg["paths"]["features"]
    events = spark.read.parquet(f"{feat}/events")
    if args.hot_fraction:
        video_id, event_date = (int(part) for part in args.hot_key.split(":"))
        events = with_hot_key(events, args.hot_fraction, video_id, event_date)
    joined = join_item_daily(events, spark.read.parquet(f"{feat}/item_daily"), args.strategy, args.salt_buckets)
    print("CHECKSUM " + json.dumps(checksum(joined), sort_keys=True), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
