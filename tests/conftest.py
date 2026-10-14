import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Local in-process Spark: tests use tiny hand-built DataFrames, not the cluster.
    session = (SparkSession.builder
               .master("local[2]")
               .appName("tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.session.timeZone", "Asia/Shanghai")
               .config("spark.eventLog.enabled", "false")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def cfg():
    return {
        "dataset": {"version": "test", "timezone": "Asia/Shanghai"},
        "clean": {
            "max_watch_ratio": 5.0,
            "bot_min_events_per_day": 20,
            "bot_max_median_gap_seconds": 2.0,
            "bot_max_play_hours_per_day": 16,
            "max_removed_fraction": 0.10,
        },
    }
