"""Benchmark harness: repeated timed runs, Spark task metrics, CSV results.

Runs on the host with the standard library only. A job is any callable that
submits work to the cluster (usually `spark_submit`). Each run is timed end to
end; the Spark applications it started are found as the ones the master lists
as newly completed, and their stage/task metrics are read from the History
Server REST API. The cluster must be otherwise idle while a benchmark runs.
"""

import csv
from datetime import datetime
import hashlib
import json
import math
from pathlib import Path
import statistics
import subprocess
import time
from urllib.request import urlopen

RESULTS_DIR = Path(__file__).resolve().parent / "results"
MASTER_URL = "http://127.0.0.1:8080"
HISTORY_URL = "http://127.0.0.1:18080/api/v1"

RUN_FIELDS = ["benchmark", "variant", "run", "warmup", "seconds", "apps", "app_ids", "stages",
              "tasks", "input_bytes", "shuffle_read_bytes", "shuffle_write_bytes",
              "slowest_stage_seconds", "slowest_stage_max_task_ms",
              "slowest_stage_median_task_ms", "config_hash", "config"]
STAGE_FIELDS = ["benchmark", "variant", "run", "warmup", "app_id", "app_name", "app_seconds",
                "stage_id", "attempt_id", "name", "seconds", "tasks", "max_task_ms",
                "median_task_ms", "input_bytes", "shuffle_read_bytes", "shuffle_write_bytes"]
SUMMARY_FIELDS = ["benchmark", "variant", "runs", "median_seconds", "min_seconds",
                  "max_seconds", "config_hash"]


def config_hash(config: dict) -> str:
    """Short fingerprint of a variant's settings, as in pipeline.common."""
    return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:12]


def spark_submit(script: str, conf: dict | None = None, args=()) -> None:
    """Run a project script on the cluster (client mode, driver on spark-master).

    `script` is relative to the repository root, which the master mounts at
    /opt/project.
    """
    command = ["docker", "exec", "spark-master", "spark-submit"]
    for key, value in sorted((conf or {}).items()):
        command += ["--conf", f"{key}={value}"]
    subprocess.run([*command, f"/opt/project/{script}", *args], check=True)


def _fetch_json(url: str):
    with urlopen(url, timeout=30) as response:
        return json.load(response)


def _timestamp(value: str) -> float:
    # The REST API reports times like 2026-10-27T09:58:01.123GMT.
    return datetime.strptime(value.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()


class SparkMetrics:
    """Reads applications from the master and their metrics from the History Server."""

    def __init__(self, master_url=MASTER_URL, history_url=HISTORY_URL, *, fetch=_fetch_json,
                 sleep=time.sleep, timeout=120.0, poll=2.0):
        self.master_url, self.history_url = master_url, history_url
        self.fetch, self.sleep, self.timeout, self.poll = fetch, sleep, timeout, poll

    def completed_app_ids(self) -> set:
        state = self.fetch(f"{self.master_url}/json/")
        if state.get("activeapps"):
            names = ", ".join(app.get("name", "?") for app in state["activeapps"])
            raise RuntimeError(f"Other Spark applications are running ({names}); benchmarks need an idle cluster")
        return {app["id"] for app in state.get("completedapps", [])}

    def wait_for_history(self, app_ids) -> dict:
        """Wait until the History Server has every completed app; returns id -> app."""
        waited = 0.0
        while True:
            apps = {app["id"]: app for app in self.fetch(f"{self.history_url}/applications?status=completed")}
            if set(app_ids) <= apps.keys():
                return {app_id: apps[app_id] for app_id in app_ids}
            if waited >= self.timeout:
                missing = ", ".join(sorted(set(app_ids) - apps.keys()))
                raise TimeoutError(f"History Server did not list {missing} within {self.timeout:g}s")
            self.sleep(self.poll)
            waited += self.poll

    def stages(self, app_id: str) -> list:
        """Completed stages of one app with their task time distribution.

        Spark's quantiles are nearest-rank, so the median is always an observed
        task time (of two tasks, the slower one).
        """
        rows = []
        for stage in self.fetch(f"{self.history_url}/applications/{app_id}/stages?status=complete"):
            stage_id, attempt = stage["stageId"], stage["attemptId"]
            summary = self.fetch(f"{self.history_url}/applications/{app_id}/stages/{stage_id}/{attempt}"
                                 "/taskSummary?quantiles=0.5,1.0")
            median_ms, max_ms = summary["duration"]
            rows.append({"stage_id": stage_id, "attempt_id": attempt, "name": stage.get("name", ""),
                         "seconds": round(_timestamp(stage["completionTime"]) - _timestamp(stage["submissionTime"]), 3),
                         "tasks": stage["numTasks"], "max_task_ms": max_ms, "median_task_ms": median_ms,
                         "input_bytes": stage.get("inputBytes", 0),
                         "shuffle_read_bytes": stage.get("shuffleReadBytes", 0),
                         "shuffle_write_bytes": stage.get("shuffleWriteBytes", 0)})
        return sorted(rows, key=lambda row: (row["stage_id"], row["attempt_id"]))


def run_benchmark(benchmark: str, variant: str, job, *, config: dict, runs=3, warmup=1,
                  metrics=None, clock=time.perf_counter):
    """Run `job()` warmup + runs times; return (run rows, stage rows).

    `config` holds every setting of this variant; its hash lets results show
    that two variants differ only in the variable under test.
    """
    if type(runs) is not int or runs < 1 or type(warmup) is not int or warmup < 0:
        raise ValueError("runs must be >= 1 and warmup >= 0")
    metrics = SparkMetrics() if metrics is None else metrics
    digest, config_json = config_hash(config), json.dumps(config, sort_keys=True)
    run_rows, stage_rows = [], []
    for run in range(1, warmup + runs + 1):
        is_warmup = run <= warmup
        before = metrics.completed_app_ids()
        start = clock()
        job()
        seconds = round(clock() - start, 3)
        new_ids = sorted(metrics.completed_app_ids() - before)
        if not new_ids:
            raise RuntimeError(f"{benchmark}/{variant} run {run} started no Spark application")
        apps = metrics.wait_for_history(new_ids)
        stages = []
        for app_id in new_ids:
            attempt = apps[app_id]["attempts"][-1]
            for stage in metrics.stages(app_id):
                stages.append({"benchmark": benchmark, "variant": variant, "run": run,
                               "warmup": is_warmup, "app_id": app_id, "app_name": apps[app_id]["name"],
                               "app_seconds": round(attempt["duration"] / 1000, 3), **stage})
        slowest = max(stages, key=lambda row: row["seconds"], default=None)
        run_rows.append({
            "benchmark": benchmark, "variant": variant, "run": run, "warmup": is_warmup,
            "seconds": seconds, "apps": len(new_ids), "app_ids": ";".join(new_ids),
            "stages": len(stages), "tasks": sum(row["tasks"] for row in stages),
            **{key: sum(row[key] for row in stages)
               for key in ("input_bytes", "shuffle_read_bytes", "shuffle_write_bytes")},
            "slowest_stage_seconds": slowest["seconds"] if slowest else None,
            "slowest_stage_max_task_ms": slowest["max_task_ms"] if slowest else None,
            "slowest_stage_median_task_ms": slowest["median_task_ms"] if slowest else None,
            "config_hash": digest, "config": config_json})
        stage_rows.extend(stages)
        print(f"benchmark {benchmark}/{variant} run {run}{' (warm-up)' if is_warmup else ''}: {seconds:.1f}s",
              flush=True)
    return run_rows, stage_rows


def summarize(run_rows) -> list:
    """Median and min-max spread of timed (non-warm-up) runs per variant."""
    groups = {}
    for row in run_rows:
        if not row["warmup"]:
            groups.setdefault((row["benchmark"], row["variant"], row["config_hash"]), []).append(row["seconds"])
    return [{"benchmark": benchmark, "variant": variant, "runs": len(times),
             "median_seconds": round(statistics.median(times), 3),
             "min_seconds": min(times), "max_seconds": max(times), "config_hash": digest}
            for (benchmark, variant, digest), times in groups.items()]


def _write_csv(path: Path, fields, rows) -> None:
    for row in rows:
        for value in row.values():
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError(f"Non-finite value in {path.name}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".csv.tmp")
    with temporary.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="raise", lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def write_results(benchmark: str, run_rows, stage_rows, results_dir=RESULTS_DIR) -> dict:
    """Write <benchmark>_runs/_stages/_summary.csv, replacing earlier results."""
    results_dir = Path(results_dir)
    paths = {name: results_dir / f"{benchmark}_{name}.csv" for name in ("runs", "stages", "summary")}
    _write_csv(paths["runs"], RUN_FIELDS, run_rows)
    _write_csv(paths["stages"], STAGE_FIELDS, stage_rows)
    _write_csv(paths["summary"], SUMMARY_FIELDS, summarize(run_rows))
    return paths
