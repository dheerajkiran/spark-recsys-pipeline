"""The pandas baseline must compute exactly what the Spark workload computes.

Both read the same CSV files, so the test covers parsing as well as every
clean rule and feature, on data built to hit their edge cases.
"""

import csv
import random

import numpy as np
import pandas as pd
import pytest
from pyspark.sql import functions as F

from benchmarks import workload_pandas as P
from benchmarks import workload_spark as W
from pipeline import schemas as S
from pipeline.common import RunStats
from pipeline.ingest import read_csv, to_bronze_videos

LOG_FIELDS = [f.name for f in S.RAW_LOG.fields]
VIDEO_FIELDS = [f.name for f in S.RAW_VIDEOS.fields]
# 23:58 Beijing time on 2022-04-10, so events cross midnight (event_date is local).
T0 = 1_649_606_280_000
HOUR = 3_600_000


def event(user, video, time_ms, click=0, like=0, play=1000, duration=10_000):
    row = dict.fromkeys(LOG_FIELDS, 0)
    row.update(user_id=user, video_id=video, date=20220410, hourmin=2300, time_ms=time_ms,
               is_click=click, is_like=like, play_time_ms=play, duration_ms=duration)
    return row


def raw_events():
    rng = random.Random(7)
    rows = []
    for user in range(1, 5):
        t = T0 + user * 1000
        for _ in range(40):
            t += rng.choice([500, 60_000, 3 * HOUR])
            for video in rng.sample(range(1, 15), rng.randint(1, 3)):   # one feed request
                rows.append(event(user, video, t, click=rng.randint(0, 1),
                                  play=rng.randint(0, 60_000), duration=rng.choice([0, 20_000, 45_000])))
    rows += [dict(rows[3], is_like=1), dict(rows[10])]                    # duplicates
    rows.append(event(1, 3, T0 + 5, play=-1))                             # impossible
    rows.append(event(2, 99, T0 + 7))                                     # undated video
    rows.append(event(3, 4, T0 + 9, play=500_000, duration=20_000))       # over the cap
    rows.append(dict(event(4, 2, T0 + 11), user_id=""))                   # missing key
    # Fast scroller: 21 requests, gaps 10 x 1 s and 10 x 3 s. Spark's median is
    # the lower middle value (1 s, a bot); averaging the middle two would give 2 s.
    t = T0 + 2 * HOUR
    for gap in [0] + [1000] * 10 + [3000] * 10:
        t += gap
        rows += [event(7, 1, t), event(7, 2, t)]
    rows += [event(9, 5, T0 + 3 * HOUR, play=9 * HOUR, duration=9 * HOUR),  # 18 h of play in one day
             event(9, 6, T0 + 4 * HOUR, play=9 * HOUR, duration=9 * HOUR)]
    return rows


@pytest.fixture
def files(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    rows = raw_events()
    for name, part in (("log_a.csv", rows[::2]), ("log_b.csv", rows[1::2])):
        with (logs / name).open("w", newline="") as handle:
            writer = csv.DictWriter(handle, LOG_FIELDS)
            writer.writeheader()
            writer.writerows(part)
    videos = tmp_path / "videos.csv"
    with videos.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, VIDEO_FIELDS)
        writer.writeheader()
        writer.writerows({"video_id": v, "upload_dt": "" if v == 99 else "2022-04-01"} for v in range(1, 100))
    return logs, videos


def test_pandas_and_spark_workloads_agree(spark, cfg, files):
    logs, videos = files
    cfg = {**cfg, "features": {"ctr_prior_strength": 20}}
    # The cluster's default filesystem is HDFS, so local files need file://.
    stats = RunStats("test", cfg)
    s_features, s_items, _ = W.transform(cfg, read_csv(spark, f"file://{logs}/log_*.csv", S.RAW_LOG),
                                         to_bronze_videos(read_csv(spark, f"file://{videos}", S.RAW_VIDEOS)),
                                         stats)
    p_features, p_items, counts = P.transform(
        cfg, P.read_logs(logs.glob("log_*.csv")),
        pd.read_csv(videos, engine="pyarrow", usecols=["video_id", "upload_dt"]))

    assert counts["bot_user_days"] == stats.stats["bot_user_days"] >= 2
    for spark_df, pandas_df, key in ((s_features, p_features, P.EVENT_KEY),
                                     (s_items, p_items, ["video_id", "event_date"])):
        expected = spark_df.toPandas().sort_values(key, ignore_index=True)
        actual = pandas_df.sort_values(key, ignore_index=True)
        assert list(actual.columns) == list(expected.columns) and len(actual) == len(expected) > 0
        for col in expected.columns:
            want, got = expected[col].astype("float64").to_numpy(), actual[col].astype("float64").to_numpy()
            assert np.array_equal(np.isnan(want), np.isnan(got)), col
            assert np.allclose(want[~np.isnan(want)], got[~np.isnan(got)], rtol=1e-12, atol=0), col
    # User 7 (fast, by the lower median) and user 9 (18 h of play) were removed as bots.
    assert not {7, 9} & set(p_features["user_id"]) and {1, 2, 3, 4} <= set(p_features["user_id"])
    assert p_features["event_date"].nunique() >= 2   # crossed midnight in Beijing time


def test_lower_median_matches_spark_percentile_approx(spark):
    rng = random.Random(3)
    groups = [[rng.randint(0, 50) for _ in range(rng.randint(1, 12))] for _ in range(40)]
    df = spark.createDataFrame([(g, v) for g, values in enumerate(groups) for v in values], "g int, v int")
    spark_medians = {r["g"]: r["m"] for r in df.groupBy("g").agg(F.percentile_approx("v", 0.5).alias("m")).collect()}
    assert spark_medians == {g: P.lower_median(pd.Series(values)) for g, values in enumerate(groups)}
