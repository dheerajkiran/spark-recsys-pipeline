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


from pipeline.build_dataset import apply_scaler, fit_scaler  # noqa: E402


def test_scaler_fits_on_train_only_and_standardizes(spark):
    train = spark.createDataFrame([(1.0,), (3.0,)], "x double")
    val = spark.createDataFrame([(1000.0,)], "x double")   # must not move the statistics
    stats = fit_scaler(train, ["x"], [])
    assert stats["x"] == {"log1p": False, "mean": 2.0, "std": pytest.approx(2 ** 0.5)}
    out = [r.x for r in apply_scaler(train.union(val), stats, []).collect()]
    assert out[:2] == pytest.approx([-(2 ** -0.5), 2 ** -0.5], rel=1e-6)
    assert out[2] == pytest.approx((1000 - 2) / 2 ** 0.5, rel=1e-6)


def test_log1p_then_standardize(spark):
    import math
    df = spark.createDataFrame([(0.0,), (math.e - 1,)], "n double")
    stats = fit_scaler(df, ["n"], ["n"])
    assert stats["n"]["log1p"] and stats["n"]["mean"] == pytest.approx(0.5)


def test_nulls_become_zero_with_a_missing_flag(spark):
    df = spark.createDataFrame([(1.0,), (3.0,), (None,)], "r double")
    stats = fit_scaler(df, ["r"], [])
    assert stats["r"]["mean"] == 2.0                        # nulls ignored when fitting
    out = apply_scaler(df, stats, ["r"]).orderBy("r_missing", "r").collect()
    assert [(r.r_missing, round(r.r, 4)) for r in out] == [(0, -0.7071), (0, 0.7071), (1, 0.0)]


def test_constant_column_and_negative_counts_stay_finite(spark):
    df = spark.createDataFrame([(5, -1), (5, 3)], "c int, n int")
    stats = fit_scaler(df, ["c", "n"], ["n"])
    assert stats["c"]["std"] == 1.0                         # no division by zero
    vals = [v for r in apply_scaler(df, stats, []).collect() for v in (r.c, r.n)]
    assert all(v == v and abs(v) != float("inf") for v in vals)  # no NaN, no inf


from pipeline.build_dataset import shuffled, split_rows  # noqa: E402

SPLIT_CFG = {"train": [1, 10], "val": [11, 12], "test": [13, 15]}


def test_split_by_date_and_log(spark):
    rows = [(d, r) for d in range(1, 16) for r in (0, 1)]
    df = spark.createDataFrame(rows, "event_date int, is_rand int")
    parts = {k: sorted((r.event_date, r.is_rand) for r in v.collect()) for k, v in split_rows(df, SPLIT_CFG).items()}
    assert parts["train"] == [(d, 0) for d in range(1, 11)]
    assert parts["val"] == [(11, 0), (12, 0)]
    assert parts["test"] == [(13, 0), (14, 0), (15, 0)]
    assert parts["test_random"] == [(13, 1), (14, 1), (15, 1)]   # random rows on train/val dates unused


def test_shuffle_keeps_rows_breaks_time_order_and_is_deterministic(spark):
    df = spark.createDataFrame([(u, 1, t) for u in range(5) for t in range(40)],
                               "user_id int, video_id int, time_ms long")
    a = shuffled(df, 4, seed=42)
    b = shuffled(df.orderBy("time_ms", ascending=False), 4, seed=42)   # different input order
    rows = lambda d: [tuple(r) for r in d.collect()]                   # noqa: E731
    assert sorted(rows(a)) == sorted(rows(df))                         # same rows, none lost
    assert rows(a) == rows(b)                                          # same output order
    assert rows(a) != sorted(rows(a), key=lambda r: r[2])              # not time-ordered
    assert rows(shuffled(df, 4, seed=7)) != rows(a)                    # the seed matters
    sizes = a.rdd.glom().map(len).collect()
    assert len(sizes) == 4 and min(sizes) > 0                          # every file gets rows
