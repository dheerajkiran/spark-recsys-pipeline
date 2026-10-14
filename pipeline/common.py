"""Shared helpers for pipeline stages: config, Spark session, run stats."""

import argparse
import json
import time
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "config" / "pipeline.yaml"


def parse_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    return parser.parse_args()


def load_config(path: str = str(DEFAULT_CONFIG)) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def build_spark(app_name: str, cfg: dict) -> SparkSession:
    # Dates and hours are derived in the dataset's local time zone, so every
    # stage must use the same session time zone.
    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.sql.session.timeZone", cfg["dataset"]["timezone"])
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_text(spark: SparkSession, path: str, text: str) -> None:
    """Write a small text file to HDFS (or any Hadoop filesystem), overwriting it."""
    jvm = spark.sparkContext._jvm
    hpath = jvm.org.apache.hadoop.fs.Path(path)
    fs = hpath.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    out = fs.create(hpath, True)
    try:
        out.write(bytearray(text.encode("utf-8")))
    finally:
        out.close()


class RunStats:
    """Collects row counts and timings for one stage and saves them as JSON."""

    def __init__(self, stage: str, cfg: dict):
        self.stage = stage
        self.cfg = cfg
        self.started = time.time()
        self.stats: dict = {"stage": stage, "dataset": cfg["dataset"]["version"]}

    def record(self, key: str, value) -> None:
        self.stats[key] = value
        print(f"[{self.stage}] {key}: {value:,}" if isinstance(value, int)
              else f"[{self.stage}] {key}: {value}")

    def save(self, spark: SparkSession) -> None:
        self.stats["seconds"] = round(time.time() - self.started, 1)
        path = f"{self.cfg['paths']['metadata']}/run_stats/{self.stage}.json"
        write_text(spark, path, json.dumps(self.stats, indent=2))
        print(f"[{self.stage}] run stats written to {path}")
