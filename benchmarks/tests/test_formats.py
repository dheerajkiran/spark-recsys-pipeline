"""File-format benchmark: stored sizes, per-run query results, result agreement."""

import csv
import json
import subprocess

import pytest

from benchmarks import formats
from benchmarks.tests.test_common import FakeCluster


def fake_du(command, **kwargs):
    # `hdfs dfs -du -s` prints size, size with replicas, path; the CSV glob gives one line per file.
    out = "10 20 /raw/a.csv\n5 10 /raw/b.csv\n" if "log_*" in command[-1] else "4 8 /1k/bronze/interactions\n"
    return subprocess.CompletedProcess(command, 0, stdout=out, stderr="")


class FakeSubmit:
    def __init__(self, cluster, checksums):
        self.cluster, self.checksums, self.calls = cluster, iter(checksums), []

    def __call__(self, script, conf=None, args=(), capture=False):
        self.calls.append(list(args))
        self.cluster.job()
        result = {"query_seconds": 0.5, "rows": 1, "checksum": next(self.checksums)}
        return "log\nRESULT " + json.dumps(result) + "\n"


def run(tmp_path, checksums):
    cluster = FakeCluster()
    submit = FakeSubmit(cluster, checksums)
    summary = formats.run_formats(["one_user"], runs=2, warmup=1, results_dir=tmp_path, metrics=cluster.metrics(),
                                  submit=submit, run=fake_du, clock=iter(range(100)).__next__)
    return summary, submit


def test_each_format_runs_the_same_query_and_results_are_summarized(tmp_path):
    summary, submit = run(tmp_path, ["9"] * 6)
    assert [call[1:] for call in submit.calls] == \
        [["--format", "csv", "--query", "one_user"]] * 3 + [["--format", "parquet", "--query", "one_user"]] * 3
    assert [(row["format"], row["runs"], row["stored_bytes"], row["median_query_seconds"]) for row in summary] == \
        [("csv", 2, 15, 0.5), ("parquet", 2, 4, 0.5)]
    with (tmp_path / "formats_queries.csv").open() as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 6 and {row["checksum"] for row in rows} == {"9"}
    assert rows[0]["input_records"] == "7"   # from the History Server stages


def test_differing_results_between_formats_fail(tmp_path):
    with pytest.raises(RuntimeError, match="one_user: CSV and Parquet results differ"):
        run(tmp_path, ["9"] * 3 + ["8"] * 3)
