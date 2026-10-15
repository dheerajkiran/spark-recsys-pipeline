import pytest

from pipeline.build_dataset import join_features, join_item_daily


def tables(spark):
    # Video 1 on day 1 is a hot key: many events, as with a viral video.
    events = spark.createDataFrame(
        [(u, 1, 10, 100 + u, 1) for u in range(40)] + [(1, 2, 20, 500, 1), (2, 1, 10, 600, 2)],
        "user_id int, video_id int, author_id int, time_ms long, event_date int")
    users = spark.createDataFrame([(u, u + 2) for u in range(40)], "user_id int, user_idx int")
    videos = spark.createDataFrame([(1, 3), (2, 4)], "video_id int, video_idx int")
    items = spark.createDataFrame([(1, 1, 0), (1, 2, 40), (2, 1, 0)],
                                  "video_id int, event_date int, video_impr_prev long")
    authors = spark.createDataFrame([(10, 1, 0), (10, 2, 41), (20, 1, 0)],
                                    "author_id int, event_date int, author_impr_prev long")
    return events, users, videos, items, authors


def rows(df):
    return sorted(tuple(r[c] for c in sorted(df.columns)) for r in df.collect())


@pytest.mark.parametrize("strategy", ["salted", "sortmerge"])
def test_join_strategies_give_identical_rows(spark, strategy):
    events, _, _, items, _ = tables(spark)
    expected = rows(join_item_daily(events, items, "broadcast", 4))
    assert rows(join_item_daily(events, items, strategy, 4)) == expected
    assert len(expected) == events.count()  # salting must not multiply rows


def test_salted_join_handles_more_buckets_than_rows(spark):
    events, _, _, items, _ = tables(spark)
    assert rows(join_item_daily(events, items, "salted", 64)) == rows(join_item_daily(events, items, "broadcast", 4))


def test_join_features_attaches_every_table_without_fan_out(spark):
    out = join_features(*tables(spark), "salted", 4)
    assert out.count() == 42
    r = out.filter("user_id = 2 and event_date = 2").first()
    assert (r.user_idx, r.video_idx, r.video_impr_prev, r.author_impr_prev) == (4, 3, 40, 41)


def test_unknown_strategy_fails(spark):
    events, _, _, items, _ = tables(spark)
    with pytest.raises(ValueError):
        join_item_daily(events, items, "magic", 4)
