"""pandas version of the Spark-vs-pandas workload: ingest, clean, feature subset.

    python3 benchmarks/workload_pandas.py <pipeline config> <log dir> <videos csv> <output dir>

Single process, everything in memory. It reproduces what benchmarks/workload_spark.py
does with the pipeline's own functions, with Spark's semantics where they differ
from pandas defaults (noted inline):

1. Ingest: the raw log CSVs; event_date and event_hour in the dataset's time zone.
2. Clean: the six interaction rules of pipeline/clean.py, in order.
3. Features: user history counts (as in features.add_user_history) and
   item_daily (as in features.daily_prev_stats).

Writes events.parquet and item_daily.parquet to the output directory and prints
one line, RESULT {json}, with row counts and the process's peak memory (RSS).
"""

import glob
import json
import resource
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, "/opt/project")
from pipeline.config import load_config  # noqa: E402

EVENT_KEY = ["user_id", "video_id", "time_ms"]
EVENT_COLUMNS = ["user_id", "video_id", "time_ms", "event_date", "event_hour", "is_click",
                 "play_time_ms", "duration_ms", "user_hist_cnt", "user_hist_ctr",
                 "secs_since_last_event", "user_active_days"]


# --- Ingest --------------------------------------------------------------------

def read_logs(paths) -> pd.DataFrame:
    return pd.concat([pd.read_csv(path, engine="pyarrow") for path in sorted(paths)], ignore_index=True)


def add_event_time(df: pd.DataFrame, timezone: str) -> pd.DataFrame:
    ts = pd.to_datetime(df["time_ms"], unit="ms", utc=True).dt.tz_convert(timezone)
    df["event_hour"] = ts.dt.hour.astype("int32")
    df["event_date"] = (ts.dt.year * 10000 + ts.dt.month * 100 + ts.dt.day).astype("int32")
    return df


# --- Clean (pipeline/clean.py) -------------------------------------------------

def dedupe_events(df: pd.DataFrame) -> pd.DataFrame:
    # Each outcome column takes its max across copies of an event.
    return df.groupby(EVENT_KEY, as_index=False, sort=False).max()


def drop_impossible(df: pd.DataFrame) -> pd.DataFrame:
    # Spark's filter drops rows where the condition is null, so a missing play
    # time or duration is dropped too; >= 0 is False for NaN, which matches.
    return df[(df["play_time_ms"] >= 0) & (df["duration_ms"] >= 0)]


def drop_unknown_upload_date(df: pd.DataFrame, videos: pd.DataFrame) -> pd.DataFrame:
    undated = videos.loc[videos["upload_dt"].isna(), "video_id"]
    return df[~df["video_id"].isin(undated)]


def null_unknown_duration(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["duration_ms"] = df["duration_ms"].astype("Int64").mask(df["duration_ms"] == 0)
    return df


def cap_play_time(df: pd.DataFrame, max_ratio: float) -> pd.DataFrame:
    # Spark casts the double cap to long, truncating toward zero.
    cap = np.trunc(df["duration_ms"].astype("Float64") * max_ratio).astype("Int64")
    over = (df["play_time_ms"] > cap).fillna(False)
    df = df.copy()
    df["play_time_ms"] = df["play_time_ms"].mask(over, cap).astype("int64")
    return df


def lower_median(values: pd.Series) -> float:
    """Spark's percentile_approx(x, 0.5) on small groups: the ceil(n/2)-th smallest value."""
    ordered = np.sort(values.to_numpy())
    return ordered[(len(ordered) + 1) // 2 - 1] if len(ordered) else np.nan


def flag_bot_user_days(df: pd.DataFrame, min_events: int, max_median_gap_s: float,
                       max_play_hours: float) -> pd.DataFrame:
    keys = ["user_id", "event_date"]
    requests = df[keys + ["time_ms"]].drop_duplicates().sort_values(keys + ["time_ms"])
    requests["gap_s"] = requests.groupby(keys)["time_ms"].diff() / 1000
    speed = requests.groupby(keys).agg(requests=("time_ms", "size"),
                                       median_gap_s=("gap_s", lambda s: lower_median(s.dropna())))
    play = df.groupby(keys)["play_time_ms"].sum() / 3.6e6
    days = speed.join(play.rename("play_hours"))
    too_fast = (days["requests"] >= min_events) & (days["median_gap_s"] < max_median_gap_s)
    too_long = days["play_hours"] > max_play_hours
    return days[too_fast | too_long].reset_index()[keys]


def remove_bot_user_days(df: pd.DataFrame, bots: pd.DataFrame) -> pd.DataFrame:
    if bots.empty:
        return df
    flagged = df.merge(bots.assign(_bot=True), on=["user_id", "event_date"], how="left")["_bot"]
    return df[flagged.isna().to_numpy()]


def clean_interactions(df: pd.DataFrame, videos: pd.DataFrame, c: dict) -> tuple[pd.DataFrame, dict]:
    counts = {"interactions_in": len(df)}
    df = df.dropna(subset=EVENT_KEY).astype({"user_id": "int64", "video_id": "int64", "time_ms": "int64"})
    df = dedupe_events(df)
    df = drop_impossible(df)
    df = drop_unknown_upload_date(df, videos)
    df = null_unknown_duration(df)
    df = cap_play_time(df, c["max_watch_ratio"])
    bots = flag_bot_user_days(df, c["bot_min_events_per_day"], c["bot_max_median_gap_seconds"],
                              c["bot_max_play_hours_per_day"])
    counts["bot_user_days"] = len(bots)
    df = remove_bot_user_days(df, bots)
    counts["silver_rows"] = len(df)
    return df, counts


# --- Features (pipeline/features.py) ---------------------------------------------

def add_user_history(df: pd.DataFrame) -> pd.DataFrame:
    """Counts over the user's strictly earlier timestamps (Spark's range frame),
    so events of one feed request never see each other."""
    per_ts = (df.groupby(["user_id", "time_ms"], sort=True)
              .agg(n=("is_click", "size"), clicks=("is_click", "sum"))
              .reset_index())
    by_user = per_ts.groupby("user_id", sort=False)
    per_ts["user_hist_cnt"] = by_user["n"].cumsum() - per_ts["n"]
    clicks_before = by_user["clicks"].cumsum() - per_ts["clicks"]
    per_ts["user_hist_ctr"] = (clicks_before / per_ts["user_hist_cnt"]).where(per_ts["user_hist_cnt"] > 0)
    per_ts["secs_since_last_event"] = (per_ts["time_ms"] - by_user["time_ms"].shift()) / 1000
    df = df.merge(per_ts[["user_id", "time_ms", "user_hist_cnt", "user_hist_ctr", "secs_since_last_event"]],
                  on=["user_id", "time_ms"], how="left")
    # dense_rank of event_date within the user, minus one: days active before today.
    df["user_active_days"] = (df.groupby("user_id")["event_date"].rank(method="dense") - 1).astype("int32")
    return df


def daily_prev_stats(df: pd.DataFrame, key: str, prefix: str, prior_strength: float) -> pd.DataFrame:
    """Per (key, event_date): stats over the key's previous days, click rate
    smoothed toward the global click rate of previous days."""
    ratio = df["play_time_ms"] / df["duration_ms"].astype("Float64")
    daily = (df.assign(_wr=ratio.astype("float64"))
             .groupby([key, "event_date"], sort=True)
             .agg(impr=("is_click", "size"), clicks=("is_click", "sum"),
                  wr_sum=("_wr", "sum"), wr_cnt=("_wr", "count"))
             .reset_index())
    by_key = daily.groupby(key, sort=False)
    prev = {col: by_key[col].cumsum() - daily[col] for col in ("impr", "clicks", "wr_sum", "wr_cnt")}
    glob = df.groupby("event_date").agg(impr=("is_click", "size"), clicks=("is_click", "sum")).sort_index()
    prior = (glob["clicks"].cumsum() - glob["clicks"]) / (glob["impr"].cumsum() - glob["impr"])
    prior = prior.where(glob["impr"].cumsum() - glob["impr"] > 0)
    a = float(prior_strength)
    out = daily[[key, "event_date"]].copy()
    out[f"{prefix}_impr_prev"] = prev["impr"]
    out[f"{prefix}_ctr_prev"] = (prev["clicks"] + a * daily["event_date"].map(prior)) / (prev["impr"] + a)
    out[f"{prefix}_avg_watch_ratio_prev"] = (prev["wr_sum"] / prev["wr_cnt"]).where(prev["wr_cnt"] > 0)
    return out


# --- Job -----------------------------------------------------------------------

def transform(cfg: dict, events: pd.DataFrame, videos: pd.DataFrame):
    """Raw log rows and videos -> (event features, item_daily, counts)."""
    events = add_event_time(events, cfg["dataset"]["timezone"])
    silver, counts = clean_interactions(events, videos, cfg["clean"])
    del events
    item_daily = daily_prev_stats(silver, "video_id", "video", cfg["features"]["ctr_prior_strength"])
    features = add_user_history(silver)[EVENT_COLUMNS]
    return features, item_daily, {**counts, "item_daily_rows": len(item_daily)}


def run(cfg: dict, log_paths, videos_path: str, out_dir: str) -> dict:
    videos = pd.read_csv(videos_path, engine="pyarrow", usecols=["video_id", "upload_dt"])
    features, item_daily, counts = transform(cfg, read_logs(log_paths), videos)
    features.to_parquet(f"{out_dir}/events.parquet", index=False)
    item_daily.to_parquet(f"{out_dir}/item_daily.parquet", index=False)
    return counts


def main() -> None:
    config, log_dir, videos_path, out_dir = sys.argv[1:5]
    start = time.perf_counter()
    result = run(load_config(config), glob.glob(f"{log_dir}/log_*.csv"), videos_path, out_dir)
    result["seconds"] = round(time.perf_counter() - start, 3)
    # ru_maxrss is in KiB on Linux.
    result["peak_rss_mb"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    print("RESULT " + json.dumps(result, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
