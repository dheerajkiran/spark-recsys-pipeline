"""Spark vs pandas on the same workload, at 10%, 25%, 50% and 100% of KuaiRand-1K.

    .venv/bin/python -m benchmarks.pandas_baseline [--sizes 10 25 50 100] [--runs 3] [--warmup 1]

The workload (ingest, clean, a feature subset) is benchmarks/workload_spark.py
on the cluster and benchmarks/workload_pandas.py in one container from the same
image, with 6 CPUs (the cluster's executor cores) and a hard memory limit
without swap. Two limits are tried: 2.5 GB, one Spark worker's limit, and
7.5 GB, all three workers' limits together. A run is recorded as "oom" only
when Docker reports the container OOM-killed (State.OOMKilled); larger sizes
are then skipped for that limit. Any other failure, including a SIGKILL that
Docker did not attribute to memory, stops the benchmark.

Sizes are samples of whole users (seeded shuffle, each sample contains the
smaller ones), so every user's history stays complete. After each size, the
Spark and pandas outputs are compared row by row; they must be identical.
"""

import argparse
import hashlib
import json
from pathlib import Path
import random
import statistics
import subprocess
import time
import uuid

from benchmarks import common

ROOT = Path(__file__).resolve().parents[1]
CONFIG = "config/pipeline-1k.yaml"
IMAGE = "recsys/spark:3.5.9"
RAW = ROOT / "data/raw/KuaiRand-1K/data"
VIDEOS = "video_features_basic_1k.csv"
# The namenode mounts only data/raw (at /data/raw), so files it uploads live there.
SAMPLES = ROOT / "data/raw/KuaiRand-1K-samples"
PANDAS_OUT = ROOT / "data/raw/bench-pandas-out"
HDFS_RAW = "hdfs:///raw/kuairand/1k"
HDFS_BENCH = "/bench/pandas"
SIZES = [10, 25, 50, 100]
MEMORY = {"2.5g": "2560m", "7.5g": "7680m"}
CPUS = 6
SEED = 42
FILES = [CONFIG, "config/pipeline.yaml", "docker/spark/Dockerfile", "benchmarks/workload_pandas.py",
         "benchmarks/workload_spark.py", "pipeline/clean.py", "pipeline/features.py", "pipeline/ingest.py"]
PANDAS_FIELDS = ["benchmark", "variant", "size_pct", "memory", "run", "warmup", "status", "seconds",
                 "process_seconds", "peak_rss_mb", "interactions_in", "silver_rows", "item_daily_rows",
                 "config_hash", "config"]
SUMMARY_FIELDS = ["engine", "size_pct", "memory", "status", "runs", "median_seconds", "min_seconds",
                  "max_seconds", "median_peak_rss_mb", "interactions_in", "silver_rows"]
COMPARE_FIELDS = ["size_pct", "table", "rows", "only_spark", "only_pandas", "mismatched_values", "identical"]


# --- Samples -------------------------------------------------------------------

def choose_users(counts: dict, sizes, seed=SEED) -> dict:
    """Per size, a set of users holding at least that share of events; nested."""
    users = sorted(counts)
    random.Random(seed).shuffle(users)
    total, chosen = sum(counts.values()), {}
    for size in sorted(sizes):
        if size == 100:
            continue
        taken, events = set(), 0
        for user in users:
            if events >= total * size / 100:
                break
            taken.add(user)
            events += counts[user]
        chosen[size] = taken
    return chosen


def make_samples(raw=RAW, out=SAMPLES, sizes=SIZES, seed=SEED) -> dict:
    """Write each sample's log files (users chosen from all logs); returns the manifest."""
    logs = sorted(raw.glob("log_*.csv"))
    sources = {path.name: path.stat().st_size for path in logs}
    manifest_path = out / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest["sources"] == sources and manifest["seed"] == seed and \
                {int(s) for s in manifest["samples"]} >= {s for s in sizes if s < 100}:
            return manifest
    counts = {}
    for path in logs:
        with path.open() as handle:
            next(handle)
            for line in handle:
                user = line.split(",", 1)[0]
                counts[user] = counts.get(user, 0) + 1
    chosen = choose_users(counts, sizes, seed)
    samples = {size: {"users": len(users), "events": sum(counts[u] for u in users)} for size, users in chosen.items()}
    for path in logs:
        handles = {}
        for size in chosen:
            (out / f"p{size}").mkdir(parents=True, exist_ok=True)
            handles[size] = (out / f"p{size}" / path.name).open("w")
        try:
            with path.open() as handle:
                header = next(handle)
                for target in handles.values():
                    target.write(header)
                for line in handle:
                    user = line.split(",", 1)[0]
                    for size, users in chosen.items():
                        if user in users:
                            handles[size].write(line)
        finally:
            for target in handles.values():
                target.close()
    manifest = {"seed": seed, "sources": sources, "total_events": sum(counts.values()),
                "samples": {str(size): value for size, value in samples.items()}}
    manifest_path.write_text(json.dumps(manifest, indent=2))
    return manifest


def hdfs(*args, run=subprocess.run) -> None:
    run(["docker", "exec", "namenode", "hdfs", "dfs", *args], check=True,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def put_dir(local: Path, target: str, run=subprocess.run) -> None:
    """Replace an HDFS directory with a local directory under data/raw."""
    parent = target.rsplit("/", 1)[0]
    hdfs("-rm", "-r", "-f", target, run=run)
    hdfs("-mkdir", "-p", parent, run=run)
    hdfs("-put", f"/data/raw/{local.relative_to(ROOT / 'data/raw')}", target, run=run)


def spark_paths(size: int) -> tuple[str, str, str]:
    logs = HDFS_RAW if size == 100 else f"hdfs://{HDFS_BENCH}/samples/p{size}"
    return logs, f"{HDFS_RAW}/{VIDEOS}", f"hdfs://{HDFS_BENCH}/spark/p{size}"


def local_logs(size: int) -> Path:
    return RAW if size == 100 else SAMPLES / f"p{size}"


# --- Runs ----------------------------------------------------------------------

def variant_config(engine: str, size: int, memory: str | None = None) -> dict:
    config = {"benchmark": "pandas_spark", "engine": engine, "size_pct": size, "seed": SEED,
              "files": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()[:12] for name in FILES}}
    if engine == "pandas":
        config.update(memory=memory, cpus=CPUS, image=IMAGE)
    return config


def parse_line(prefix: str, out: str) -> dict:
    lines = [line for line in out.splitlines() if line.startswith(prefix + " ")]
    if len(lines) != 1:
        raise RuntimeError(f"expected one {prefix} line in the job output")
    return json.loads(lines[0].removeprefix(prefix + " "))


def pandas_command(size: int, memory: str, out: Path, name: str) -> list:
    # No --rm: the stopped container is kept until inspected, so its
    # OOMKilled flag can be read; run_pandas removes it afterwards.
    limit = MEMORY[memory]
    return ["docker", "run", f"--name={name}", f"--memory={limit}", f"--memory-swap={limit}", f"--cpus={CPUS}",
            "--network=none", "-e", "PYTHONPATH=/opt/project",
            "-v", f"{ROOT}:/opt/project:ro", "-v", f"{local_logs(size)}:/data/logs:ro",
            "-v", f"{RAW / VIDEOS}:/data/videos.csv:ro", "-v", f"{out}:/out", IMAGE,
            "python3", "/opt/project/benchmarks/workload_pandas.py", f"/opt/project/{CONFIG}",
            "/data/logs", "/data/videos.csv", "/out"]


def inspect_container(name: str, run=subprocess.run) -> dict:
    """Docker's State of a stopped container (ExitCode, OOMKilled, ...)."""
    result = run(["docker", "inspect", "--format", "{{json .State}}", name], capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"docker inspect {name} failed: {result.stderr.strip()}")
    state = json.loads(result.stdout)
    if not isinstance(state.get("OOMKilled"), bool):
        raise RuntimeError(f"docker inspect {name} did not report OOMKilled")
    return state


def remove_container(name: str, run=subprocess.run) -> None:
    run(["docker", "rm", "-f", name], capture_output=True, text=True)


def run_pandas(size: int, memory: str, *, runs: int, warmup: int, run=subprocess.run,
               clock=time.perf_counter) -> list:
    """Timed pandas runs; stops at the first run Docker reports OOM-killed."""
    config = variant_config("pandas", size, memory)
    out = PANDAS_OUT / f"p{size}-{memory}"
    out.mkdir(parents=True, exist_ok=True)
    rows = []
    for number in range(1, warmup + runs + 1):
        name = f"pandas-bench-{uuid.uuid4().hex}"
        try:
            start = clock()
            result = run(pandas_command(size, memory, out, name), capture_output=True, text=True)
            seconds = round(clock() - start, 3)   # inspection and cleanup are not timed
            state = inspect_container(name, run=run)
            row = {"benchmark": "pandas_spark", "variant": f"pandas-{memory}_p{size}", "size_pct": size,
                   "memory": memory, "run": number, "warmup": number <= warmup, "seconds": seconds,
                   "config_hash": common.config_hash(config), "config": json.dumps(config, sort_keys=True)}
            if state["OOMKilled"]:
                rows.append({**row, "status": "oom"})
                print(f"benchmark pandas-{memory}_p{size} run {number}: out of memory after {seconds:.1f}s",
                      flush=True)
                break
            if result.returncode == 137:
                raise RuntimeError(f"pandas run was killed (exit 137) but Docker did not report an OOM kill:\n"
                                   f"{result.stderr[-2000:]}")
            if result.returncode != 0:
                raise RuntimeError(f"pandas run failed ({result.returncode}):\n{result.stderr[-2000:]}")
            parsed = parse_line("RESULT", result.stdout)
        finally:
            remove_container(name, run=run)
        rows.append({**row, "status": "ok", "process_seconds": parsed["seconds"],
                     "peak_rss_mb": parsed["peak_rss_mb"], "interactions_in": parsed["interactions_in"],
                     "silver_rows": parsed["silver_rows"], "item_daily_rows": parsed["item_daily_rows"]})
        print(f"benchmark pandas-{memory}_p{size} run {number}{' (warm-up)' if number <= warmup else ''}: "
              f"{seconds:.1f}s, peak {parsed['peak_rss_mb']:.0f} MB", flush=True)
    return rows


def summarize(spark_rows, spark_results, pandas_rows) -> list:
    rows = []
    for size in sorted({row["size_pct"] for row in spark_results}):
        times = [row["seconds"] for row in spark_rows if row["variant"] == f"spark_p{size}" and not row["warmup"]]
        result = next(r for r in spark_results if r["size_pct"] == size)
        rows.append({"engine": "spark", "size_pct": size, "memory": "", "status": "ok", "runs": len(times),
                     "median_seconds": round(statistics.median(times), 3), "min_seconds": min(times),
                     "max_seconds": max(times), "median_peak_rss_mb": "",
                     "interactions_in": result["interactions_in"], "silver_rows": result["silver_rows"]})
    for (size, memory) in sorted({(row["size_pct"], row["memory"]) for row in pandas_rows}):
        group = [row for row in pandas_rows if (row["size_pct"], row["memory"]) == (size, memory)]
        timed = [row for row in group if not row["warmup"] and row["status"] == "ok"]
        status = "oom" if any(row["status"] == "oom" for row in group) else "ok"
        base = {"engine": "pandas", "size_pct": size, "memory": memory, "status": status, "runs": len(timed)}
        if timed:
            base.update(median_seconds=round(statistics.median(r["seconds"] for r in timed), 3),
                        min_seconds=min(r["seconds"] for r in timed), max_seconds=max(r["seconds"] for r in timed),
                        median_peak_rss_mb=statistics.median(r["peak_rss_mb"] for r in timed),
                        interactions_in=timed[0]["interactions_in"], silver_rows=timed[0]["silver_rows"])
        rows.append({field: base.get(field, "") for field in SUMMARY_FIELDS})
    return rows


def run_all(sizes, memories, *, runs=3, warmup=1, results_dir=common.RESULTS_DIR, metrics=None,
            submit=common.spark_submit, run=subprocess.run, clock=time.perf_counter, prepare=True) -> list:
    results_dir = Path(results_dir)
    if prepare:
        manifest = make_samples(sizes=sizes)
        print(f"samples: {json.dumps(manifest['samples'])}", flush=True)
        for size in sizes:
            if size < 100:
                put_dir(SAMPLES / f"p{size}", f"{HDFS_BENCH}/samples/p{size}", run=run)
    spark_rows, stage_rows, spark_results, pandas_rows, compare_rows = [], [], [], [], []
    failed_memory = set()
    for size in sorted(sizes):
        logs, videos, out = spark_paths(size)
        outputs = []

        def job():
            text = submit("benchmarks/workload_spark.py", args=[f"/opt/project/{CONFIG}", logs, videos, out],
                          capture=True)
            outputs.append(parse_line("RESULT", text))

        runs_, stages_ = common.run_benchmark("pandas_spark", f"spark_p{size}", job,
                                              config=variant_config("spark", size), runs=runs, warmup=warmup,
                                              metrics=metrics, clock=clock)
        if len({json.dumps(o, sort_keys=True) for o in outputs}) != 1:
            raise RuntimeError(f"Spark runs at {size}% gave different row counts: {outputs}")
        spark_rows += runs_
        stage_rows += stages_
        spark_results.append({"size_pct": size, **outputs[0]})

        compared_from = None
        for memory in memories:
            if memory in failed_memory:
                continue
            rows = run_pandas(size, memory, runs=runs, warmup=warmup, run=run, clock=clock)
            pandas_rows += rows
            if rows[-1]["status"] == "oom":
                failed_memory.add(memory)
            elif compared_from is None:
                compared_from = memory
        if compared_from is not None:
            put_dir(PANDAS_OUT / f"p{size}-{compared_from}", f"{HDFS_BENCH}/pandas/p{size}", run=run)
            result = parse_line("COMPARE", submit("benchmarks/compare_outputs.py",
                                                  args=[out, f"hdfs://{HDFS_BENCH}/pandas/p{size}"], capture=True))
            for table in ("events", "item_daily"):
                r = result[table]
                compare_rows.append({"size_pct": size, "table": table, "rows": r.get("rows", ""),
                                     "only_spark": r.get("only_spark", ""), "only_pandas": r.get("only_pandas", ""),
                                     "mismatched_values": json.dumps(r.get("mismatched_values", {"columns": "differ"})),
                                     "identical": result["identical"]})
            print(f"compare {size}%: identical={result['identical']}", flush=True)

        common.write_results("pandas_spark", spark_rows, stage_rows, results_dir)
        common._write_csv(results_dir / "pandas_runs.csv", PANDAS_FIELDS,
                          [{field: row.get(field, "") for field in PANDAS_FIELDS} for row in pandas_rows])
        common._write_csv(results_dir / "pandas_compare.csv", COMPARE_FIELDS, compare_rows)
        common._write_csv(results_dir / "pandas_summary.csv", SUMMARY_FIELDS,
                          summarize(spark_rows, spark_results, pandas_rows))
        if compare_rows and not all(row["identical"] for row in compare_rows):
            raise RuntimeError(f"Spark and pandas outputs differ at {size}%; see pandas_compare.csv")
    return summarize(spark_rows, spark_results, pandas_rows)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sizes", type=int, nargs="+", default=SIZES, choices=SIZES)
    parser.add_argument("--memory", nargs="+", default=list(MEMORY), choices=MEMORY)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--results-dir", type=Path, default=common.RESULTS_DIR)
    args = parser.parse_args(argv)
    run_all(args.sizes, args.memory, runs=args.runs, warmup=args.warmup, results_dir=args.results_dir)
    print((args.results_dir / "pandas_summary.csv").read_text(), end="")


if __name__ == "__main__":
    main()
