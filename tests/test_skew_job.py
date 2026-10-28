from pyspark.sql import functions as F

from benchmarks.output_checksum import checksum
from benchmarks.skew_job import with_hot_key
from pipeline.build_dataset import join_item_daily


def events(spark, n=2000):
    return spark.createDataFrame([(u % 50, u % 300, 1000 + u, 20220400 + u % 20, u % 2) for u in range(n)],
                                 "user_id int, video_id int, time_ms long, event_date int, is_click int")


def test_hot_key_moves_the_same_share_of_rows_on_both_columns(spark):
    df = events(spark)
    hot = with_hot_key(df, 0.25, 999, 20220506).cache()
    on_key = hot.filter((F.col("video_id") == 999) & (F.col("event_date") == 20220506)).count()
    # Every moved row gets both the hot video and the hot date, never one of them.
    assert hot.filter((F.col("video_id") == 999) != (F.col("event_date") == 20220506)).count() == 0
    assert 400 <= on_key <= 600 and hot.count() == 2000
    assert checksum(hot) == checksum(with_hot_key(df, 0.25, 999, 20220506))  # same rows every run
    assert with_hot_key(df, 0.0, 999, 20220506).filter("video_id = 999").count() == 0


def test_strategies_agree_on_a_hot_key(spark):
    hot = with_hot_key(events(spark), 0.25, 1, 20220401)
    items = spark.createDataFrame([(v, d, v * 10 + d % 7) for v in range(300) for d in range(20220400, 20220420)],
                                  "video_id int, event_date int, video_impr_prev long")
    sums = {strategy: checksum(join_item_daily(hot, items, strategy, 8)) for strategy in ("sortmerge", "salted")}
    assert sums["sortmerge"] == sums["salted"] and sums["salted"]["rows"] == 2000
