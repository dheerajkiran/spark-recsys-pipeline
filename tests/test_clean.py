from datetime import datetime

from pyspark.sql import functions as F

from pipeline import schemas as S
from pipeline.clean import (
    clean_videos, dedupe_events, drop_impossible, drop_missing_keys, fill_missing,
    cap_play_time, flag_bot_user_days, null_unknown_duration, remove_bot_user_days,
)

def events(spark, rows):
    return spark.createDataFrame(
        rows, "user_id int, video_id int, time_ms long, event_date int, is_click int, "
              "long_view int, play_time_ms long, duration_ms long, hourmin int")


def row(user=1, video=10, t=1000, date=20220410, click=0, lv=0, play=1000, dur=10000, hm=800):
    return (user, video, t, date, click, lv, play, dur, hm)


def test_drop_missing_keys(spark):
    df = events(spark, [row(), row(user=None), row(video=None), row(t=None)])
    assert drop_missing_keys(df).count() == 1


def test_dedupe_keeps_one_row_with_most_engagement(spark):
    # Same event logged twice: the later copy has the click and more play time,
    # and a different coarse hour.
    df = events(spark, [row(click=0, play=1047, hm=800), row(click=1, play=2852, hm=900)])
    out = dedupe_events(df).collect()
    assert len(out) == 1
    assert out[0].is_click == 1 and out[0].play_time_ms == 2852
    assert dedupe_events(df).columns == df.columns


def test_dedupe_keeps_distinct_events(spark):
    # Same user and time but different videos is a feed page, not a duplicate.
    df = events(spark, [row(video=10), row(video=11), row(t=2000)])
    assert dedupe_events(df).count() == 3


def test_drop_impossible(spark):
    df = events(spark, [row(), row(play=-1), row(dur=-5), row(dur=0)])
    assert drop_impossible(df).count() == 2  # zero duration is handled separately


def test_zero_duration_becomes_null_and_row_is_kept(spark):
    df = events(spark, [row(dur=0), row(dur=5000)])
    out = null_unknown_duration(df).orderBy(F.col("duration_ms").asc_nulls_first()).collect()
    assert len(out) == 2
    assert out[0].duration_ms is None and out[1].duration_ms == 5000


def test_cap_play_time(spark):
    df = events(spark, [row(play=60000, dur=10000), row(play=30000, dur=10000), row(play=9, dur=None)])
    out = sorted(r.play_time_ms for r in cap_play_time(df, 5.0).collect())
    assert out == [9, 30000, 50000]  # capped to 5x; unknown duration untouched


def _day(user, n, gap_ms, play_ms, date=20220410):
    return [row(user=user, video=i, t=i * gap_ms, date=date, play=play_ms) for i in range(n)]


def test_bot_rule_flags_fast_clicker_not_heavy_scroller(spark):
    hour_ms = 3_600_000
    rows = (_day(1, 30, gap_ms=500, play_ms=100)                 # 30 events, 0.5 s apart: bot
            + _day(2, 200, gap_ms=15000, play_ms=100)            # 200 events, 15 s apart: human
            + _day(3, 10, gap_ms=500, play_ms=100)               # fast but too few events
            + _day(4, 3, gap_ms=60000, play_ms=6 * hour_ms))     # 18 hours of play: bot
    bots = flag_bot_user_days(events(spark, rows), 20, 2.0, 16)
    assert sorted(r.user_id for r in bots.collect()) == [1, 4]


def test_remove_bot_user_days_only_removes_that_day(spark):
    df = events(spark, [row(user=1, date=20220410), row(user=1, date=20220411, t=2),
                        row(user=2, date=20220410, t=3)])
    bots = spark.createDataFrame([(1, 20220410)], "user_id int, event_date int")
    out = remove_bot_user_days(df, bots)
    assert sorted((r.user_id, r.event_date) for r in out.collect()) == [(1, 20220411), (2, 20220410)]
    assert out.columns == df.columns


def test_fill_missing_users(spark):
    df = spark.createDataFrame([(1, None, None, 5)], "user_id int, degree string, onehot int, n int")
    r = fill_missing(df, "user_id").first()
    assert (r.degree, r.onehot, r.n) == (S.UNKNOWN, S.MISSING_INT, 5)


def test_clean_videos_parses_tags_and_fills(spark):
    def video(video_id, tag):
        # music_type and video_duration are missing
        return (video_id, 1, "NORMAL", datetime(2022, 4, 10).date(), "LongImport", 0, None,
                720, 1280, 9, None, tag)

    df = spark.createDataFrame([video(0, "12,60"), video(1, "14"), video(2, None)], S.BRONZE_VIDEOS)
    out = clean_videos(df)
    assert out.schema.fieldNames() == S.SILVER_VIDEOS.fieldNames()
    rows = out.orderBy("video_id").collect()
    assert [r.tags for r in rows] == [[12, 60], [14], []]
    assert rows[0].music_type == S.MISSING_INT
    assert rows[0].video_duration is None  # unknown length stays null
