from datetime import datetime, timezone

from pipeline import schemas as S
from pipeline.ingest import to_bronze_interactions, to_bronze_users, to_bronze_videos


def _ms(year, month, day, hour, minute, utc_offset_hours=8):
    """Epoch ms for a Beijing (UTC+8) wall-clock time."""
    utc = datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp()
    return int((utc - utc_offset_hours * 3600) * 1000)


def _log_row(time_ms, date=20220101, hourmin=0):
    return (1, 10, date, hourmin, time_ms, 0, 0, 0, 0, 0, 0, 0, 1000, 10000, 0, 0, 0, 0, 1)


def test_event_date_and_hour_use_beijing_time(spark):
    # 23:40 in Beijing is 15:40 UTC: the date must not roll over, and the
    # coarse raw date/hourmin (next day, 00) must not be used.
    late = _ms(2022, 4, 29, 23, 40)
    df = spark.createDataFrame([_log_row(late, date=20220430, hourmin=0)], S.RAW_LOG)
    row = to_bronze_interactions(df).first()
    assert row.event_date == 20220429
    assert row.event_hour == 23
    assert row.date == 20220430  # raw column kept unchanged


def test_bronze_interactions_schema(spark):
    df = spark.createDataFrame([_log_row(_ms(2022, 4, 10, 8, 0))], S.RAW_LOG)
    out = to_bronze_interactions(df)
    assert [f.name for f in out.schema.fields] == S.BRONZE_INTERACTIONS.fieldNames()


def test_users_onehot_floats_become_ints(spark):
    row = (7, "full_active", 0, 1, 1, 514, "500+", 150, "[100,1k)", 34, "[30,60)", 799, "730+",
           *[1.0] * 4, None, *[2.0] * 13)
    out = to_bronze_users(spark.createDataFrame([row], S.RAW_USERS))
    assert out.schema == S.BRONZE_USERS
    r = out.first()
    assert r.onehot_feat0 == 1 and r.onehot_feat5 == 2
    assert r.onehot_feat4 is None  # nulls are filled in silver, not bronze


def test_videos_float_ids_become_ints(spark):
    row = (0, 7349781, "NORMAL", datetime(2022, 4, 10).date(), "LongImport", 0.0, 87433.0,
           720.0, 1280.0, 9155697141, 9.0, "12,60")
    out = to_bronze_videos(spark.createDataFrame([row], S.RAW_VIDEOS))
    assert out.schema == S.BRONZE_VIDEOS
    r = out.first()
    assert (r.video_duration, r.server_width, r.music_type) == (87433, 720, 9)
