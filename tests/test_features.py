"""Leakage tests for stage 3: no feature may see the event itself, its
same-timestamp siblings, or anything later."""

from datetime import date

import pytest

from pipeline.features import add_context, add_user_history, daily_prev_stats

MIN = 60_000
SCHEMA = ("user_id int, video_id int, video_idx int, author_id int, time_ms long, event_date int, "
          "is_click int, is_like int, play_time_ms long, duration_ms long")


def ev(user=1, video=10, t=0, date=20220410, click=0, like=0, play=1000, dur=10000, author=100):
    return (user, video, video + 1000, author, t, date, click, like, play, dur)


def history(spark, rows, n=20, slack=10, gap_s=1800):
    df = add_user_history(spark.createDataFrame(rows, SCHEMA), n, slack, gap_s)
    return {(r.user_id, r.video_id, r.time_ms): r for r in df.collect()}


def test_first_event_has_no_history(spark):
    r = history(spark, [ev(t=0, click=1)])[(1, 10, 0)]
    assert r.user_hist_cnt == 0 and r.user_hist_ctr is None
    assert r.secs_since_last_event is None and r.session_position == 1
    assert r.hist_len == 0 and r.hist_video_idx == [0] * 20


def test_same_timestamp_events_never_see_each_other(spark):
    # v11 and v12 are served in the same feed request (same time_ms); v11 is clicked.
    out = history(spark, [ev(video=10, t=0, click=0), ev(video=11, t=MIN, click=1),
                          ev(video=12, t=MIN, click=0), ev(video=13, t=2 * MIN)])
    a, b = out[(1, 11, MIN)], out[(1, 12, MIN)]
    for r in (a, b):
        assert r.user_hist_cnt == 1 and r.user_hist_ctr == 0.0   # only v10 is in the past
        assert r.hist_video_idx[-1] == 1010 and r.hist_len == 1
        assert r.secs_since_last_event == 60.0                   # gap to v10, not to the sibling
    later = out[(1, 13, 2 * MIN)]
    assert later.user_hist_cnt == 3 and later.user_hist_ctr == pytest.approx(1 / 3)
    assert later.hist_video_idx[-3:] == [1010, 1011, 1012]


def test_future_events_never_leak(spark):
    out = history(spark, [ev(video=10, t=0), ev(video=11, t=MIN, click=1, like=1)])
    first = out[(1, 10, 0)]
    assert first.user_hist_cnt == 0 and 1011 not in first.hist_video_idx


def test_history_keeps_last_n_most_recent_last(spark):
    rows = [ev(video=v, t=v * MIN) for v in range(10, 16)]
    r = history(spark, rows, n=3)[(1, 15, 15 * MIN)]
    assert r.hist_video_idx == [1012, 1013, 1014] and r.hist_len == 3


def test_history_is_left_padded(spark):
    r = history(spark, [ev(video=10, t=0), ev(video=11, t=MIN)], n=4)[(1, 11, MIN)]
    assert r.hist_video_idx == [0, 0, 0, 1010]


def test_sessions_split_after_gap(spark):
    out = history(spark, [ev(video=10, t=0), ev(video=11, t=10 * MIN), ev(video=12, t=50 * MIN)])
    assert [out[(1, v, t)].session_position for v, t in [(10, 0), (11, 10 * MIN), (12, 50 * MIN)]] == [1, 2, 1]


def test_active_days_counts_previous_dates_only(spark):
    out = history(spark, [ev(video=10, t=0, date=20220410), ev(video=11, t=MIN, date=20220410),
                          ev(video=12, t=10**9, date=20220412)])
    assert [out[k].user_active_days for k in [(1, 10, 0), (1, 11, MIN), (1, 12, 10**9)]] == [0, 0, 1]


def test_user_author_counts_only_earlier_events_with_that_author(spark):
    out = history(spark, [ev(video=10, t=0, author=7, click=1), ev(video=11, t=MIN, author=8, click=1),
                          ev(video=12, t=2 * MIN, author=7), ev(video=13, t=2 * MIN, author=7, click=1)])
    for v in (12, 13):
        r = out[(1, v, 2 * MIN)]
        assert (r.user_author_prev_impr, r.user_author_prev_clicks) == (1, 1)


def test_users_are_independent(spark):
    out = history(spark, [ev(user=1, video=10, t=0, click=1), ev(user=2, video=11, t=MIN)])
    assert out[(2, 11, MIN)].user_hist_cnt == 0


def daily(spark, rows, a=10.0):
    df = daily_prev_stats(spark.createDataFrame(rows, SCHEMA), "video_id", "video", a)
    return {(r.video_id, r.event_date): r for r in df.collect()}


def test_item_stats_use_previous_days_only(spark):
    rows = [ev(video=10, date=1, click=1), ev(video=10, date=1, t=1),   # day 1: 2 impr, 1 click
            ev(video=10, date=2, t=2, click=1)]                          # day 2: a same-day click
    out = daily(spark, rows)
    assert out[(10, 1)].video_impr_prev == 0 and out[(10, 1)].video_ctr_prev is None  # no prior yet
    d2 = out[(10, 2)]
    assert d2.video_impr_prev == 2                     # the day-2 click is not counted
    prior = 1 / 2                                      # global click rate of day 1
    assert d2.video_ctr_prev == pytest.approx((1 + 10 * prior) / (2 + 10))


def test_new_item_gets_the_prior(spark):
    rows = [ev(video=10, date=1, click=1), ev(video=10, date=1, t=1), ev(video=20, date=2, t=2)]
    r = daily(spark, rows)[(20, 2)]
    assert r.video_impr_prev == 0 and r.video_ctr_prev == pytest.approx(0.5)


def test_context(spark):
    df = spark.createDataFrame([(23, 20220410, date(2022, 4, 1))],
                               "event_hour int, event_date int, upload_dt date")
    r = add_context(df).first()
    assert (r.hour_of_day, r.day_of_week, r.video_age_days) == (23, 1, 9)   # 2022-04-10 is a Sunday
