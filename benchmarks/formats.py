"""File-format benchmark: the same queries on raw CSV and on bronze Parquet.

    .venv/bin/python -m benchmarks.formats [--queries ...] [--runs 3] [--warmup 1]

Each run is benchmarks/format_job.py on KuaiRand-1K: one query, one format,
one Spark application. Two variants per query differ only in the format read.
Recorded per run: end-to-end time, the query's own time (from the job, without
Spark start-up), and bytes and records read (History Server). Every run's
result checksum must match the other format's for the same query.
"""

import argparse
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import time

from benchmarks import common

ROOT = Path(__file__).resolve().parents[1]
CONFIG = "config/pipeline-1k.yaml"
FORMATS = ["csv", "parquet"]
# The queries of format_job.py (which needs PySpark, so it is not imported here;
# tests/test_format_job.py checks the two lists agree).
QUERIES = ["all_columns", "two_columns", "date_range", "one_user"]
STORED = {"csv": "/raw/kuairand/1k/log_*.csv", "parquet": "/1k/bronze/interactions"}
FILES = [CONFIG, "config/pipeline.yaml", "docker/spark/spark-defaults.conf", "docker-compose.yml",
         "benchmarks/format_job.py", "pipeline/ingest.py", "pipeline/schemas.py"]
QUERY_FIELDS = ["benchmark", "variant", "query", "format", "run", "warmup", "seconds", "query_seconds",
                "input_bytes", "input_records", "rows", "checksum"]
SUMMARY_FIELDS = ["query", "format", "runs", "median_query_seconds", "min_query_seconds", "max_query_seconds",
                  "median_seconds", "input_bytes", "input_records", "stored_bytes", "rows", "checksum"]


def stored_bytes(run=subprocess.run) -> dict:
    """Bytes each format occupies on HDFS (one replica)."""
    sizes = {}
    for fmt, path in STORED.items():
        out = run(["docker", "exec", "namenode", "bash", "-c", f"hdfs dfs -du -s '{path}'"],
                  check=True, capture_output=True, text=True).stdout
        sizes[fmt] = sum(int(line.split()[0]) for line in out.splitlines() if line.strip())
    return sizes


def variant_config(query: str, fmt: str) -> dict:
    return {"benchmark": "formats", "query": query, "format": fmt,
            "files": {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()[:12] for name in FILES}}


def parse_result(out: str) -> dict:
    lines = [line for line in out.splitlines() if line.startswith("RESULT ")]
    if len(lines) != 1:
        raise RuntimeError("format_job.py did not print one RESULT line")
    return json.loads(lines[0].removeprefix("RESULT "))


def summarize(query_rows, sizes) -> list:
    out = []
    for query in dict.fromkeys(row["query"] for row in query_rows):
        for fmt in FORMATS:
            timed = [r for r in query_rows if (r["query"], r["format"]) == (query, fmt) and not r["warmup"]]
            if not timed:
                continue
            q = [r["query_seconds"] for r in timed]
            out.append({"query": query, "format": fmt, "runs": len(timed),
                        "median_query_seconds": round(statistics.median(q), 3),
                        "min_query_seconds": min(q), "max_query_seconds": max(q),
                        "median_seconds": round(statistics.median(r["seconds"] for r in timed), 3),
                        "input_bytes": statistics.median(r["input_bytes"] for r in timed),
                        "input_records": statistics.median(r["input_records"] for r in timed),
                        "stored_bytes": sizes.get(fmt, ""), "rows": timed[0]["rows"], "checksum": timed[0]["checksum"]})
    return out


def run_formats(queries, *, runs=3, warmup=1, results_dir=common.RESULTS_DIR, metrics=None,
                submit=common.spark_submit, run=subprocess.run, clock=time.perf_counter) -> list:
    results_dir = Path(results_dir)
    sizes = stored_bytes(run)
    run_rows, stage_rows, query_rows = [], [], []
    for query in queries:
        for fmt in FORMATS:
            outputs = []

            def job():
                outputs.append(parse_result(submit("benchmarks/format_job.py", capture=True,
                                                   args=[f"/opt/project/{CONFIG}", "--format", fmt, "--query", query])))

            runs_, stages_ = common.run_benchmark("formats", f"{query}_{fmt}", job,
                                                  config=variant_config(query, fmt), runs=runs, warmup=warmup,
                                                  metrics=metrics, clock=clock)
            run_rows += runs_
            stage_rows += stages_
            query_rows += [{"benchmark": "formats", "variant": row["variant"], "query": query, "format": fmt,
                            "run": row["run"], "warmup": row["warmup"], "seconds": row["seconds"],
                            "input_bytes": row["input_bytes"], "input_records": row["input_records"], **output}
                           for row, output in zip(runs_, outputs)]
            common.write_results("formats", run_rows, stage_rows, results_dir)
            common._write_csv(results_dir / "formats_queries.csv", QUERY_FIELDS, query_rows)
            common._write_csv(results_dir / "formats_query_summary.csv", SUMMARY_FIELDS,
                              summarize(query_rows, sizes))
        results = {(r["rows"], r["checksum"]) for r in query_rows if r["query"] == query}
        if len(results) != 1:
            raise RuntimeError(f"{query}: CSV and Parquet results differ; see formats_queries.csv")
    return summarize(query_rows, sizes)


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--queries", nargs="+", default=list(QUERIES), choices=QUERIES)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--results-dir", type=Path, default=common.RESULTS_DIR)
    args = parser.parse_args(argv)
    run_formats(args.queries, runs=args.runs, warmup=args.warmup, results_dir=args.results_dir)
    print((args.results_dir / "formats_query_summary.csv").read_text(), end="")


if __name__ == "__main__":
    main()
