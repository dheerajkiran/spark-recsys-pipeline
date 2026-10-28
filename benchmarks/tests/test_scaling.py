"""Scaling benchmark: worker control, executor verification, checksums, cleanup."""

import csv
import json

import pytest

from benchmarks import scaling
from benchmarks.tests.test_common import FakeCluster


def master(states):
    return lambda url: {"workers": [{"state": state} for state in states]}


def test_set_workers_starts_and_stops_containers_then_waits_for_spark():
    commands, polls = [], iter([["ALIVE"] * 3, ["ALIVE"] * 3, ["ALIVE", "DEAD", "DEAD"]])
    scaling.set_workers(1, run=lambda command, **_: commands.append(command),
                        fetch=lambda url: master(next(polls))(url), sleep=lambda _: None)
    assert commands == [["docker", "start", "spark-worker-1"],
                        ["docker", "stop", "spark-worker-2", "spark-worker-3"]]
    commands.clear()
    scaling.set_workers(3, run=lambda command, **_: commands.append(command),
                        fetch=master(["ALIVE"] * 3), sleep=lambda _: None)
    assert commands == [["docker", "start", "spark-worker-1", "spark-worker-2", "spark-worker-3"]]
    with pytest.raises(TimeoutError, match="2 alive workers, expected 1"):
        scaling.set_workers(1, run=lambda *a, **k: None, fetch=master(["ALIVE", "ALIVE"]),
                            sleep=lambda _: None, timeout=4)
    with pytest.raises(ValueError):
        scaling.set_workers(0, run=lambda *a, **k: None)


def test_variants_differ_only_in_worker_count():
    one, three = (scaling.variant_config("config/pipeline-1k.yaml", scaling.STAGES, n) for n in (1, 3))
    assert {key for key in one if one[key] != three[key]} == {"workers"}
    assert set(one["files"]) == {"config/pipeline-1k.yaml", *scaling.CONFIG_FILES}


class FakeSubmit:
    """Stands in for spark_submit: pipeline stages complete apps, checksums print."""

    def __init__(self, cluster, checksums):
        self.cluster, self.checksums, self.calls = cluster, list(checksums), []

    def __call__(self, script, conf=None, args=(), capture=False):
        self.calls.append((script, list(args)))
        if script == "benchmarks/output_checksum.py":
            return "log line\nCHECKSUM " + json.dumps(self.checksums.pop(0)) + "\n"
        self.cluster.job()
        return None


class ExecutorCluster(FakeCluster):
    def __init__(self):
        super().__init__()
        self.workers = 3

    def fetch(self, url):
        if url.endswith("/allexecutors"):
            return [{"id": "driver"}] + [{"id": str(i)} for i in range(self.workers)]
        return super().fetch(url)


def run(tmp_path, cluster, submit, counts, set_workers=None):
    def set_count(count):
        cluster.workers = count
        calls.append(count)
    calls = []
    scaling.run_scaling("config/pipeline-1k.yaml", counts, ["clean", "features"], runs=2, warmup=1,
                        results_dir=tmp_path, metrics=cluster.metrics(), submit=submit,
                        set_workers=set_workers or set_count, clock=iter(range(1000)).__next__)
    return calls


def test_runs_each_worker_count_and_checks_outputs_match(tmp_path):
    cluster = ExecutorCluster()
    same = {"gold/train": {"rows": 5, "checksum": "42"}}
    submit = FakeSubmit(cluster, [same, same])
    calls = run(tmp_path, cluster, submit, [1, 3])
    assert calls == [1, 3, 3]  # the last call restores all workers
    stages = [script for script, _ in submit.calls if script.startswith("pipeline/")]
    assert stages == ["pipeline/clean.py", "pipeline/features.py"] * 6  # 2 counts x (1 warm-up + 2 runs)
    assert submit.calls[0][1] == ["--config", "/opt/project/config/pipeline-1k.yaml"]
    with (tmp_path / "scaling_runs.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert [(row["variant"], row["executors"], row["apps"]) for row in rows] == \
        [("1_workers", "1", "2")] * 3 + [("3_workers", "3", "2")] * 3
    with (tmp_path / "scaling_summary.csv").open() as handle:
        assert [row["runs"] for row in csv.DictReader(handle)] == ["2", "2"]
    with (tmp_path / "scaling_checksums.csv").open() as handle:
        assert [(row["variant"], row["checksum"]) for row in csv.DictReader(handle)] == \
            [("1_workers", "42"), ("3_workers", "42")]


def test_differing_outputs_fail_after_results_are_written(tmp_path):
    cluster = ExecutorCluster()
    submit = FakeSubmit(cluster, [{"gold/train": {"rows": 5, "checksum": "1"}},
                                  {"gold/train": {"rows": 5, "checksum": "2"}}])
    with pytest.raises(RuntimeError, match="differ between worker counts"):
        run(tmp_path, cluster, submit, [1, 2])
    assert (tmp_path / "scaling_checksums.csv").read_text().count("scaling,") == 2


def test_missing_executor_fails_and_still_restores_all_workers(tmp_path):
    cluster = ExecutorCluster()
    calls = []

    def set_count(count):
        calls.append(count)
        cluster.workers = 3 if count == 3 else count - 1  # one worker never joins

    with pytest.raises(RuntimeError, match="did not have 2 executors"):
        run(tmp_path, cluster, FakeSubmit(cluster, []), [2], set_workers=set_count)
    assert calls == [2, 3]
