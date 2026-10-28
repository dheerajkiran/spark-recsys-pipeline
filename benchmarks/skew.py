"""Skew benchmark: naive sort-merge vs AQE skew join vs salting.

    .venv/bin/python -m benchmarks.skew [--scenarios real-200 hot-200 hot-24] [--runs 3] [--warmup 1]

Each run is benchmarks/skew_job.py: the pipeline's item_daily join on
KuaiRand-1K, broadcast disabled, output checksummed. Within a scenario the
variants differ only in how the join handles skew:

- naive:  sort-merge join, AQE off.
- aqe:    sort-merge join, AQE on with skew-join splitting only (partition
          coalescing off, so it is the one change from naive).
- salted: the pipeline's salted join (join.salt_buckets), AQE off.

Scenarios fix the data and shuffle partitions:

- real-200: the real keys, which are balanced (busiest key 382 events).
- hot-200:  HOT_FRACTION of events moved to one key, a constructed viral video.
- hot-24:   the same hot key with the pipeline's 24 shuffle partitions.

AQE's skew thresholds are lowered to fit this data size (the defaults target
partitions of hundreds of MB). Every run's checksum must match the others in
its scenario.
"""

import argparse
import hashlib
import json
from pathlib import Path
import time

from benchmarks import common

ROOT = Path(__file__).resolve().parents[1]
CONFIG = "config/pipeline-1k.yaml"
# The busiest real (video_id, event_date) key in 1K, so item_daily has its row.
HOT_KEY = "160718:20220506"
HOT_FRACTION = 0.2
SALT_BUCKETS = 8
SCENARIOS = {"real-200": (0.0, 200), "hot-200": (HOT_FRACTION, 200), "hot-24": (HOT_FRACTION, 24)}
BASE_CONF = {
    "spark.sql.autoBroadcastJoinThreshold": "-1",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "32m",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "16m",
}
VARIANTS = {
    "naive": ("sortmerge", {"spark.sql.adaptive.enabled": "false"}),
    "aqe": ("sortmerge", {"spark.sql.adaptive.enabled": "true",
                          "spark.sql.adaptive.skewJoin.enabled": "true",
                          "spark.sql.adaptive.coalescePartitions.enabled": "false"}),
    "salted": ("salted", {"spark.sql.adaptive.enabled": "false"}),
}
CONFIG_FILES = [CONFIG, "config/pipeline.yaml", "docker/spark/spark-defaults.conf", "docker-compose.yml",
                "benchmarks/skew_job.py", "pipeline/build_dataset.py"]
JOIN_FIELDS = ["benchmark", "variant", "run", "seconds", "tasks", "max_task_ms", "median_task_ms",
               "max_task_shuffle_read_records", "median_task_shuffle_read_records", "disk_spilled_bytes"]
CHECKSUM_FIELDS = ["benchmark", "variant", "run", "rows", "checksum"]


def variant_settings(scenario: str, variant: str) -> tuple[dict, list]:
    """spark-submit conf and skew_job.py arguments for one variant."""
    fraction, partitions = SCENARIOS[scenario]
    strategy, aqe = VARIANTS[variant]
    conf = {**BASE_CONF, "spark.sql.shuffle.partitions": str(partitions), **aqe}
    args = [f"/opt/project/{CONFIG}", "--strategy", strategy, "--salt-buckets", str(SALT_BUCKETS)]
    if fraction:
        args += ["--hot-fraction", str(fraction), "--hot-key", HOT_KEY]
    return conf, args


def variant_config(scenario: str, variant: str) -> dict:
    conf, args = variant_settings(scenario, variant)
    return {"benchmark": "skew", "scenario": scenario, "variant": variant, "conf": conf, "args": args,
            "files": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()[:12] for name in CONFIG_FILES}}


def parse_checksum(out: str) -> dict:
    lines = [line for line in out.splitlines() if line.startswith("CHECKSUM ")]
    if len(lines) != 1:
        raise RuntimeError("skew_job.py did not print one CHECKSUM line")
    return json.loads(lines[0].removeprefix("CHECKSUM "))


def join_stages(stage_rows) -> list:
    """The join stage of each run: the stage that read the most shuffle bytes."""
    runs = {}
    for row in stage_rows:
        key = (row["variant"], row["run"])
        if key not in runs or row["shuffle_read_bytes"] > runs[key]["shuffle_read_bytes"]:
            runs[key] = row
    return [{field: row[field] for field in JOIN_FIELDS} for row in runs.values() if not row["warmup"]]


def run_skew(scenarios, variants=tuple(VARIANTS), *, runs=3, warmup=1, results_dir=common.RESULTS_DIR,
             metrics=None, submit=common.spark_submit, clock=time.perf_counter) -> dict:
    """Benchmark every scenario x variant; write CSVs after each; return checksums."""
    run_rows, stage_rows, checksum_rows = [], [], []
    for scenario in scenarios:
        for variant in variants:
            name = f"{scenario}_{variant}"
            conf, args = variant_settings(scenario, variant)
            outputs = []

            def job():
                outputs.append(parse_checksum(submit("benchmarks/skew_job.py", conf=conf, args=args, capture=True)))

            runs_, stages_ = common.run_benchmark("skew", name, job, config=variant_config(scenario, variant),
                                                  runs=runs, warmup=warmup, metrics=metrics, clock=clock)
            run_rows += runs_
            stage_rows += stages_
            checksum_rows += [{"benchmark": "skew", "variant": name, "run": number, **output}
                              for number, output in enumerate(outputs, 1)]
            common.write_results("skew", run_rows, stage_rows, results_dir)
            common._write_csv(Path(results_dir) / "skew_join_stages.csv", JOIN_FIELDS, join_stages(stage_rows))
            common._write_csv(Path(results_dir) / "skew_checksums.csv", CHECKSUM_FIELDS, checksum_rows)
    checksums = {}
    for row in checksum_rows:
        checksums.setdefault(row["variant"].rsplit("_", 1)[0], set()).add((row["rows"], row["checksum"]))
    differing = sorted(scenario for scenario, values in checksums.items() if len(values) > 1)
    if differing:
        raise RuntimeError(f"Join outputs differ within {', '.join(differing)}; see skew_checksums.csv")
    return {scenario: values.pop() for scenario, values in checksums.items()}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenarios", nargs="+", default=list(SCENARIOS), choices=SCENARIOS)
    parser.add_argument("--variants", nargs="+", default=list(VARIANTS), choices=VARIANTS)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--results-dir", type=Path, default=common.RESULTS_DIR)
    args = parser.parse_args(argv)
    run_skew(args.scenarios, args.variants, runs=args.runs, warmup=args.warmup, results_dir=args.results_dir)
    print((args.results_dir / "skew_summary.csv").read_text(), end="")
    print((args.results_dir / "skew_join_stages.csv").read_text(), end="")


if __name__ == "__main__":
    main()
