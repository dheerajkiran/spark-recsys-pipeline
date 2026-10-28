"""Benchmark harness: warm-up handling, medians, metric parsing and CSV output.

The unit tests use a fake cluster. The last test runs a real dummy job and is
skipped unless the Docker cluster is up.
"""

import csv
import json
from urllib.error import URLError

import pytest

from benchmarks import common


class FakeCluster:
    """Master + History Server REST responses; each job() completes new apps."""

    def __init__(self, apps_per_job=1, history_lag=0):
        self.completed, self.history, self.lag = [], [], history_lag
        self.apps_per_job, self.urls, self.next_id = apps_per_job, [], 0

    def job(self):
        for _ in range(self.apps_per_job):
            self.next_id += 1
            app_id = f"app-20261027100000-{self.next_id:04d}"
            self.completed.append({"id": app_id, "name": f"stage-{self.next_id}"})
            self.history.append([self.lag, {"id": app_id, "name": f"stage-{self.next_id}",
                                            "attempts": [{"completed": True, "duration": 4500}]}])

    def fetch(self, url):
        self.urls.append(url)
        if url.endswith("/json/"):
            return {"activeapps": [], "completedapps": list(self.completed)}
        if url.endswith("/applications?status=completed"):
            visible = [app for lag, app in self.history if lag <= 0]
            for entry in self.history:
                entry[0] -= 1
            return visible
        if url.endswith("/stages?status=complete"):
            return [{"stageId": 1, "attemptId": 0, "name": "collect", "numTasks": 24,
                     "submissionTime": "2026-10-27T10:00:01.000GMT", "completionTime": "2026-10-27T10:00:01.250GMT",
                     "inputBytes": 0, "shuffleReadBytes": 2400, "shuffleWriteBytes": 0},
                    {"stageId": 0, "attemptId": 0, "name": "range", "numTasks": 4,
                     "submissionTime": "2026-10-27T10:00:00.000GMT", "completionTime": "2026-10-27T10:00:00.900GMT",
                     "inputBytes": 100, "shuffleReadBytes": 0, "shuffleWriteBytes": 2400}]
        if url.endswith("/allexecutors"):
            return [{"id": "driver"}, {"id": "0"}, {"id": "1"}, {"id": "2"}]
        if url.endswith("/0/taskSummary?quantiles=0.5,1.0"):
            stage = url.split("/stages/")[1].split("/")[0]
            return {"quantiles": [0.5, 1.0], "duration": [200.0, 800.0] if stage == "0" else [10.0, 30.0]}
        raise AssertionError(f"unexpected URL {url}")

    def metrics(self, **kwargs):
        return common.SparkMetrics("http://master", "http://history/api/v1", fetch=self.fetch,
                                   sleep=lambda _: None, **kwargs)


def clock(times):
    values = iter(times)
    return lambda: next(values)


def test_runs_discard_warmup_and_summarize_median_and_spread(tmp_path):
    cluster = FakeCluster()
    # Warm-up takes 9 s, timed runs 3, 1 and 2 s.
    ticks = clock([0, 9, 10, 13, 20, 21, 30, 32])
    runs, stages = common.run_benchmark("dummy", "base", cluster.job, config={"workers": 3},
                                        runs=3, warmup=1, metrics=cluster.metrics(), clock=ticks)
    assert [(row["run"], row["warmup"], row["seconds"]) for row in runs] == [
        (1, True, 9), (2, False, 3), (3, False, 1), (4, False, 2)]
    [summary] = common.summarize(runs)
    assert summary == {"benchmark": "dummy", "variant": "base", "runs": 3, "median_seconds": 2,
                       "min_seconds": 1, "max_seconds": 3, "config_hash": common.config_hash({"workers": 3})}
    assert len(stages) == 8 and all(row["app_seconds"] == 4.5 for row in stages)

    paths = common.write_results("dummy", runs, stages, tmp_path)
    with paths["runs"].open() as handle:
        written = list(csv.DictReader(handle))
    assert [row["warmup"] for row in written] == ["True", "False", "False", "False"]
    assert json.loads(written[0]["config"]) == {"workers": 3}
    with paths["summary"].open() as handle:
        assert list(csv.DictReader(handle))[0]["median_seconds"] == "2"
    assert not list(tmp_path.glob("*.tmp"))


def test_metrics_cover_every_new_app_and_its_slowest_stage():
    cluster = FakeCluster(apps_per_job=2)
    cluster.job()  # an earlier app must not be attributed to the benchmark
    runs, stages = common.run_benchmark("pipeline", "base", cluster.job, config={}, runs=1, warmup=0,
                                        metrics=cluster.metrics(), clock=clock([0, 5]))
    [run] = runs
    assert run["app_ids"] == "app-20261027100000-0003;app-20261027100000-0004"
    assert run["apps"] == 2 and run["executors"] == 3 and run["stages"] == 4 and run["tasks"] == 56
    assert run["input_bytes"] == 200 and run["shuffle_read_bytes"] == run["shuffle_write_bytes"] == 4800
    assert (run["slowest_stage_seconds"], run["slowest_stage_max_task_ms"],
            run["slowest_stage_median_task_ms"]) == (0.9, 800.0, 200.0)
    assert [(row["app_name"], row["stage_id"], row["seconds"]) for row in stages] == [
        ("stage-3", 0, 0.9), ("stage-3", 1, 0.25), ("stage-4", 0, 0.9), ("stage-4", 1, 0.25)]


def test_waits_for_history_server_lag_and_times_out():
    cluster = FakeCluster(history_lag=3)
    runs, _ = common.run_benchmark("dummy", "base", cluster.job, config={}, runs=1, warmup=0,
                                   metrics=cluster.metrics(), clock=clock([0, 1]))
    assert runs[0]["stages"] == 2
    slow = FakeCluster(history_lag=100)
    with pytest.raises(TimeoutError, match="0001"):
        common.run_benchmark("dummy", "base", slow.job, config={}, runs=1, warmup=0,
                             metrics=slow.metrics(timeout=10, poll=2), clock=clock([0, 1]))


def test_refuses_busy_cluster_jobs_without_apps_and_bad_counts():
    cluster = FakeCluster()
    busy = cluster.fetch
    cluster.fetch = lambda url: ({"activeapps": [{"name": "other"}], "completedapps": []}
                                 if url.endswith("/json/") else busy(url))
    with pytest.raises(RuntimeError, match="other"):
        common.run_benchmark("dummy", "base", cluster.job, config={}, metrics=cluster.metrics())
    idle = FakeCluster()
    with pytest.raises(RuntimeError, match="no Spark application"):
        common.run_benchmark("dummy", "base", lambda: None, config={}, runs=1, warmup=0,
                             metrics=idle.metrics(), clock=clock([0, 1]))
    for runs, warmup in [(0, 1), (3, -1), (2.0, 1)]:
        with pytest.raises(ValueError):
            common.run_benchmark("dummy", "base", idle.job, config={}, runs=runs, warmup=warmup,
                                 metrics=idle.metrics())


def test_config_hash_is_order_independent_and_changes_with_any_setting():
    assert common.config_hash({"a": 1, "b": 2}) == common.config_hash({"b": 2, "a": 1})
    assert common.config_hash({"a": 1, "b": 2}) != common.config_hash({"a": 1, "b": 3})


def test_spark_submit_builds_a_cluster_command(monkeypatch):
    calls = []
    class Done:
        stdout = "CHECKSUM {}\n"

    monkeypatch.setattr(common.subprocess, "run", lambda command, **kwargs: calls.append((command, kwargs)) or Done)
    assert common.spark_submit("benchmarks/dummy_job.py", {"spark.sql.shuffle.partitions": 8,
                                                           "spark.sql.adaptive.enabled": "false"}, ["--x"]) is None
    assert calls == [(["docker", "exec", "spark-master", "spark-submit",
                       "--conf", "spark.sql.adaptive.enabled=false",
                       "--conf", "spark.sql.shuffle.partitions=8",
                       "/opt/project/benchmarks/dummy_job.py", "--x"],
                      {"check": True, "stdout": None, "text": None})]
    assert common.spark_submit("benchmarks/dummy_job.py", capture=True) == "CHECKSUM {}\n"
    assert calls[1][1] == {"check": True, "stdout": common.subprocess.PIPE, "text": True}


def _cluster_is_up():
    try:
        common._fetch_json(f"{common.MASTER_URL}/json/")
        common._fetch_json(f"{common.HISTORY_URL}/applications?limit=1")
    except (URLError, OSError):
        return False
    return True


@pytest.mark.skipif(not _cluster_is_up(), reason="Spark cluster is not running")
def test_dummy_job_on_the_real_cluster(tmp_path):
    runs, stages = common.run_benchmark(
        "dummy", "base", lambda: common.spark_submit("benchmarks/dummy_job.py"),
        config={"job": "dummy"}, runs=2, warmup=1)
    assert [row["warmup"] for row in runs] == [True, False, False]
    assert all(row["apps"] == 1 and row["stages"] >= 2 and row["shuffle_write_bytes"] > 0 for row in runs)
    assert all(row["executors"] == 3 for row in runs)
    assert all(row["app_name"] == "benchmark-dummy" and row["max_task_ms"] >= row["median_task_ms"] >= 0
               for row in stages)
    paths = common.write_results("dummy", runs, stages, tmp_path)
    assert all(path.stat().st_size > 0 for path in paths.values())
