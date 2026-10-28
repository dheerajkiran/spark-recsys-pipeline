"""Skew benchmark: variant settings, join-stage selection, checksum agreement."""

import csv
import json

import pytest

from benchmarks import skew
from benchmarks.tests.test_common import FakeCluster


def test_variants_change_only_skew_handling():
    naive, naive_args = skew.variant_settings("hot-200", "naive")
    aqe, aqe_args = skew.variant_settings("hot-200", "aqe")
    salted, salted_args = skew.variant_settings("hot-200", "salted")
    assert naive_args == aqe_args and naive_args[2] == "sortmerge" and salted_args[2] == "salted"
    assert salted == naive and naive["spark.sql.adaptive.enabled"] == "false"
    assert {key for key in aqe if aqe[key] != naive.get(key)} == {
        "spark.sql.adaptive.enabled", "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled"}
    assert all(conf["spark.sql.autoBroadcastJoinThreshold"] == "-1" for conf in (naive, aqe, salted))
    assert ["--hot-fraction", "0.2", "--hot-key", skew.HOT_KEY] == naive_args[-4:]
    real, real_args = skew.variant_settings("real-200", "naive")
    assert real == naive and "--hot-fraction" not in real_args
    assert skew.variant_settings("hot-24", "naive")[0]["spark.sql.shuffle.partitions"] == "24"


class FakeSubmit:
    def __init__(self, cluster, outputs):
        self.cluster, self.outputs, self.calls = cluster, iter(outputs), []

    def __call__(self, script, conf=None, args=(), capture=False):
        self.calls.append((script, conf, list(args)))
        self.cluster.job()
        return "log\nCHECKSUM " + json.dumps(next(self.outputs)) + "\n"


def test_runs_every_variant_and_reports_the_join_stage(tmp_path):
    cluster = FakeCluster()
    same = {"rows": 10, "checksum": "7"}
    submit = FakeSubmit(cluster, [same] * 6)
    checksums = skew.run_skew(["hot-24"], ["naive", "aqe", "salted"], runs=1, warmup=1, results_dir=tmp_path,
                              metrics=cluster.metrics(), submit=submit, clock=iter(range(100)).__next__)
    assert checksums == {"hot-24": (10, "7")}
    assert [conf["spark.sql.adaptive.enabled"] for _, conf, _ in submit.calls] == \
        ["false", "false", "true", "true", "false", "false"]
    with (tmp_path / "skew_join_stages.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    # One timed run per variant; the join stage is the one with the most shuffle reads.
    assert [(row["variant"], row["run"], row["tasks"], row["max_task_shuffle_read_records"]) for row in rows] == \
        [(f"hot-24_{name}", "2", "24", "900.0") for name in ("naive", "aqe", "salted")]
    with (tmp_path / "skew_checksums.csv").open() as handle:
        assert len(list(csv.DictReader(handle))) == 6
    assert (tmp_path / "skew_summary.csv").exists()


def test_differing_outputs_within_a_scenario_fail(tmp_path):
    cluster = FakeCluster()
    outputs = [{"rows": 10, "checksum": "7"}, {"rows": 10, "checksum": "8"}]
    with pytest.raises(RuntimeError, match="differ within hot-24"):
        skew.run_skew(["hot-24"], ["naive"], runs=1, warmup=1, results_dir=tmp_path,
                      metrics=cluster.metrics(), submit=FakeSubmit(cluster, outputs),
                      clock=iter(range(100)).__next__)


def test_job_must_print_one_checksum():
    with pytest.raises(RuntimeError, match="one CHECKSUM line"):
        skew.parse_checksum("no checksum here\n")
