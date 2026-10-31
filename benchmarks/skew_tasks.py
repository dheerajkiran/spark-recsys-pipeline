"""Save the skew benchmark's join-stage tasks, for the task-timeline plot.

    .venv/bin/python -m benchmarks.skew_tasks [--run 3]

The Spark UI's event timeline only renders after a click, so it cannot be
captured as a plain screenshot. This reads the same data (each task's executor,
start, duration and shuffle records) from the History Server for one timed run
of the hot-key scenario's naive and AQE variants, and writes
benchmarks/results/skew_tasks.csv, so benchmarks/plots.py can draw it from
saved data. Needs the cluster and the History Server's event logs of that run.
"""

import argparse
import csv
from pathlib import Path

from benchmarks import common

VARIANTS = ["hot-200_naive", "hot-200_aqe"]
FIELDS = ["variant", "run", "app_id", "stage_id", "task_id", "executor_id", "start_ms", "duration_ms",
          "shuffle_read_records"]


def join_stage(stage_rows, variant: str, run: int) -> dict:
    """The run's join stage: the one that read the most shuffle bytes (as skew.join_stages)."""
    rows = [r for r in stage_rows if r["variant"] == variant and int(r["run"]) == run]
    if not rows:
        raise RuntimeError(f"no stages for {variant} run {run} in skew_stages.csv")
    return max(rows, key=lambda r: int(r["shuffle_read_bytes"]))


def task_rows(stage: dict, run: int, fetch=common._fetch_json) -> list:
    url = (f"{common.HISTORY_URL}/applications/{stage['app_id']}/stages/{stage['stage_id']}/"
           f"{stage['attempt_id']}/taskList?length=10000")
    tasks = [t for t in fetch(url) if t["status"] == "SUCCESS"]
    if len(tasks) != int(stage["tasks"]):
        raise RuntimeError(f"{stage['app_id']} stage {stage['stage_id']}: {len(tasks)} tasks, expected {stage['tasks']}")
    first = min(common._timestamp(t["launchTime"]) for t in tasks)
    return sorted(({"variant": stage["variant"], "run": run, "app_id": stage["app_id"],
                    "stage_id": stage["stage_id"], "task_id": t["taskId"], "executor_id": t["executorId"],
                    "start_ms": round((common._timestamp(t["launchTime"]) - first) * 1000),
                    "duration_ms": t["duration"],
                    "shuffle_read_records": t["taskMetrics"]["shuffleReadMetrics"]["recordsRead"]}
                   for t in tasks), key=lambda r: (r["start_ms"], r["task_id"]))


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", type=int, default=3, help="timed run number in skew_stages.csv")
    parser.add_argument("--results-dir", type=Path, default=common.RESULTS_DIR)
    args = parser.parse_args(argv)
    with (args.results_dir / "skew_stages.csv").open() as handle:
        stages = list(csv.DictReader(handle))
    rows = [row for variant in VARIANTS for row in task_rows(join_stage(stages, variant, args.run), args.run)]
    common._write_csv(args.results_dir / "skew_tasks.csv", FIELDS, rows)
    print(f"wrote {len(rows)} tasks to {args.results_dir / 'skew_tasks.csv'}")


if __name__ == "__main__":
    main()
