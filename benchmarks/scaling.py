"""Scaling benchmark: the same pipeline on 1, 2 and 3 Spark workers.

    .venv/bin/python -m benchmarks.scaling [--workers 1 2 3] [--runs 3] [--warmup 1]

Only the number of running worker containers changes; data, config and cores
per executor stay fixed. Workers are stopped and started with Docker, and each
run must show that many executors in Spark (from the History Server), so a
worker that failed to join cannot pass as a smaller cluster. After each worker
count, the pipeline's outputs are checksummed; all counts must match. All
three workers are running again when the benchmark ends, even after a failure.
"""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time

from benchmarks import common

ROOT = Path(__file__).resolve().parents[1]
WORKERS = ["spark-worker-1", "spark-worker-2", "spark-worker-3"]
STAGES = ["ingest", "clean", "features", "build_dataset"]
CHECKSUM_FIELDS = ["benchmark", "variant", "table", "rows", "checksum"]
# Files whose contents define a variant besides the worker count.
CONFIG_FILES = ["config/pipeline.yaml", "docker/spark/spark-defaults.conf", "docker-compose.yml"]


def alive_workers(fetch=common._fetch_json) -> int:
    state = fetch(f"{common.MASTER_URL}/json/")
    return sum(1 for worker in state["workers"] if worker["state"] == "ALIVE")


def set_workers(count: int, *, run=subprocess.run, fetch=common._fetch_json, sleep=time.sleep,
                timeout=180.0, poll=2.0) -> None:
    """Run exactly the first `count` worker containers; wait until Spark agrees."""
    if not 1 <= count <= len(WORKERS):
        raise ValueError(f"workers must be 1..{len(WORKERS)}")
    run(["docker", "start", *WORKERS[:count]], check=True, stdout=subprocess.DEVNULL)
    if count < len(WORKERS):
        run(["docker", "stop", *WORKERS[count:]], check=True, stdout=subprocess.DEVNULL)
    waited = 0.0
    while (alive := alive_workers(fetch)) != count:
        if waited >= timeout:
            raise TimeoutError(f"Spark master shows {alive} alive workers, expected {count}")
        sleep(poll)
        waited += poll


def variant_config(config: str, stages, workers: int) -> dict:
    files = [config, *CONFIG_FILES]
    return {"benchmark": "scaling", "pipeline_config": config, "stages": list(stages),
            "workers": workers, "executor_cores": 2,
            "files": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()[:12] for name in files}}


def pipeline_job(config: str, stages, submit=common.spark_submit):
    def job():
        for stage in stages:
            submit(f"pipeline/{stage}.py", args=["--config", f"/opt/project/{config}"])
    return job


def output_checksums(config: str, submit=common.spark_submit) -> dict:
    out = submit("benchmarks/output_checksum.py", args=[f"/opt/project/{config}"], capture=True)
    lines = [line for line in out.splitlines() if line.startswith("CHECKSUM ")]
    if len(lines) != 1:
        raise RuntimeError("output_checksum.py did not print one CHECKSUM line")
    return json.loads(lines[0].removeprefix("CHECKSUM "))


def run_scaling(config: str, worker_counts, stages=STAGES, *, runs=3, warmup=1,
                results_dir=common.RESULTS_DIR, metrics=None, set_workers=set_workers,
                submit=common.spark_submit, clock=time.perf_counter) -> dict:
    """Benchmark each worker count; write CSVs after every count, return checksums."""
    run_rows, stage_rows, checksum_rows, checksums = [], [], [], {}
    try:
        for count in worker_counts:
            set_workers(count)
            variant = f"{count}_workers"
            runs_, stages_ = common.run_benchmark(
                "scaling", variant, pipeline_job(config, stages, submit),
                config=variant_config(config, stages, count), runs=runs, warmup=warmup,
                metrics=metrics, clock=clock)
            wrong = [row["run"] for row in runs_ if row["executors"] != count]
            if wrong:
                raise RuntimeError(f"{variant}: runs {wrong} did not have {count} executors in every app")
            run_rows += runs_
            stage_rows += stages_
            checksums[count] = output_checksums(config, submit)
            checksum_rows += [{"benchmark": "scaling", "variant": variant, "table": table, **value}
                              for table, value in sorted(checksums[count].items())]
            common.write_results("scaling", run_rows, stage_rows, results_dir)
            common._write_csv(Path(results_dir) / "scaling_checksums.csv", CHECKSUM_FIELDS, checksum_rows)
    finally:
        set_workers(len(WORKERS))
    if len({json.dumps(value, sort_keys=True) for value in checksums.values()}) > 1:
        raise RuntimeError("Pipeline outputs differ between worker counts; see scaling_checksums.csv")
    return checksums


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default="config/pipeline-1k.yaml", help="Pipeline config, repo-relative")
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 3])
    parser.add_argument("--stages", nargs="+", default=STAGES, choices=STAGES)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--results-dir", type=Path, default=common.RESULTS_DIR)
    args = parser.parse_args(argv)
    run_scaling(args.config, args.workers, args.stages, runs=args.runs, warmup=args.warmup,
                results_dir=args.results_dir)
    print((args.results_dir / "scaling_summary.csv").read_text(), end="")


if __name__ == "__main__":
    main()
