"""Explicit Spark schemas: the source of truth for every table's columns and types.

RAW_* schemas describe the KuaiRand CSVs exactly as published (confirmed by
scripts/profile_raw.py). Some whole-number columns are written as floats
("9.0"), so they are read as double here and cast to integers in bronze.

BRONZE_* and SILVER_* schemas are what each stage must produce; the quality
checks compare stage output against them.
"""

from pyspark.sql.types import (
    ArrayType, DateType, DoubleType, IntegerType, LongType, StringType,
    StructField, StructType, TimestampType,
)


def _schema(*fields) -> StructType:
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


# --- Raw CSVs --------------------------------------------------------------

_LOG_FIELDS = [
    ("user_id", IntegerType()),
    ("video_id", IntegerType()),
    ("date", IntegerType()),          # YYYYMMDD, coarse; see event_date
    ("hourmin", IntegerType()),       # HH00, coarse; see event_hour
    ("time_ms", LongType()),          # event time, epoch milliseconds
    ("is_click", IntegerType()),
    ("is_like", IntegerType()),
    ("is_follow", IntegerType()),
    ("is_comment", IntegerType()),
    ("is_forward", IntegerType()),
    ("is_hate", IntegerType()),
    ("long_view", IntegerType()),
    ("play_time_ms", LongType()),
    ("duration_ms", LongType()),
    ("profile_stay_time", LongType()),
    ("comment_stay_time", LongType()),
    ("is_profile_enter", IntegerType()),
    ("is_rand", IntegerType()),
    ("tab", IntegerType()),
]
RAW_LOG = _schema(*_LOG_FIELDS)

ONEHOT_COLS = [f"onehot_feat{i}" for i in range(18)]

RAW_USERS = _schema(
    ("user_id", IntegerType()),
    ("user_active_degree", StringType()),
    ("is_lowactive_period", IntegerType()),
    ("is_live_streamer", IntegerType()),
    ("is_video_author", IntegerType()),
    ("follow_user_num", IntegerType()),
    ("follow_user_num_range", StringType()),
    ("fans_user_num", IntegerType()),
    ("fans_user_num_range", StringType()),
    ("friend_user_num", IntegerType()),
    ("friend_user_num_range", StringType()),
    ("register_days", IntegerType()),
    ("register_days_range", StringType()),
    *[(c, DoubleType()) for c in ONEHOT_COLS],   # some written as "1.0"
)

RAW_VIDEOS = _schema(
    ("video_id", IntegerType()),
    ("author_id", IntegerType()),
    ("video_type", StringType()),
    ("upload_dt", DateType()),
    ("upload_type", StringType()),
    ("visible_status", DoubleType()),
    ("video_duration", DoubleType()),
    ("server_width", DoubleType()),
    ("server_height", DoubleType()),
    ("music_id", LongType()),
    ("music_type", DoubleType()),
    ("tag", StringType()),            # comma-separated tag ids, e.g. "12,60"
)

# Monthly averages per video. Stored in bronze for completeness but never used
# as features: the averages include the val/test period (leakage).
VIDEO_STAT_COLS = [
    "show_cnt", "show_user_num", "play_cnt", "play_user_num", "play_duration",
    "complete_play_cnt", "complete_play_user_num", "valid_play_cnt",
    "valid_play_user_num", "long_time_play_cnt", "long_time_play_user_num",
    "short_time_play_cnt", "short_time_play_user_num", "play_progress",
    "comment_stay_duration", "like_cnt", "like_user_num", "click_like_cnt",
    "double_click_cnt", "cancel_like_cnt", "cancel_like_user_num", "comment_cnt",
    "comment_user_num", "direct_comment_cnt", "reply_comment_cnt",
    "delete_comment_cnt", "delete_comment_user_num", "comment_like_cnt",
    "comment_like_user_num", "follow_cnt", "follow_user_num", "cancel_follow_cnt",
    "cancel_follow_user_num", "share_cnt", "share_user_num", "download_cnt",
    "download_user_num", "report_cnt", "report_user_num", "reduce_similar_cnt",
    "reduce_similar_user_num", "collect_cnt", "collect_user_num",
    "cancel_collect_cnt", "cancel_collect_user_num", "direct_comment_user_num",
    "reply_comment_user_num", "share_all_cnt", "share_all_user_num",
    "outsite_share_all_cnt",
]
RAW_VIDEO_STATS = _schema(
    ("video_id", IntegerType()),
    ("counts", IntegerType()),
    *[(c, DoubleType()) for c in VIDEO_STAT_COLS],
)

# --- Bronze: typed Parquet ---------------------------------------------------

BRONZE_INTERACTIONS = _schema(
    *_LOG_FIELDS,
    ("event_ts", TimestampType()),
    ("event_hour", IntegerType()),
    ("event_date", IntegerType()),    # partition column, from time_ms
)

BRONZE_USERS = _schema(
    *[(f.name, f.dataType) for f in RAW_USERS.fields if f.name not in ONEHOT_COLS],
    *[(c, IntegerType()) for c in ONEHOT_COLS],
)

BRONZE_VIDEOS = _schema(
    ("video_id", IntegerType()),
    ("author_id", IntegerType()),
    ("video_type", StringType()),
    ("upload_dt", DateType()),
    ("upload_type", StringType()),
    ("visible_status", IntegerType()),
    ("video_duration", LongType()),
    ("server_width", IntegerType()),
    ("server_height", IntegerType()),
    ("music_id", LongType()),
    ("music_type", IntegerType()),
    ("tag", StringType()),
)

BRONZE_VIDEO_STATS = RAW_VIDEO_STATS

# --- Silver: cleaned -----------------------------------------------------------

# Same columns as bronze; cleaning changes rows and values, not the schema.
# (watch_ratio is derived in stage 3, not stored: ~8 bytes/row of incompressible doubles.)
SILVER_INTERACTIONS = BRONZE_INTERACTIONS

SILVER_USERS = BRONZE_USERS

SILVER_VIDEOS = _schema(
    *[(f.name, f.dataType) for f in BRONZE_VIDEOS.fields if f.name != "tag"],
    ("tags", ArrayType(IntegerType())),
)

UNKNOWN = "UNKNOWN"
MISSING_INT = -1
