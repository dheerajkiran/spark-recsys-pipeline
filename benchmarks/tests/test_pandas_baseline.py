"""pandas baseline driver: user samples, memory-limit failures, summaries."""

import json
import subprocess

import pytest

from benchmarks import pandas_baseline as B


def test_samples_are_nested_whole_users_with_enough_events():
    counts = {str(user): user % 7 + 1 for user in range(200)}
    chosen = B.choose_users(counts, [10, 25, 50, 100])
    assert set(chosen) == {10, 25, 50}
    assert chosen[10] <= chosen[25] <= chosen[50]
    total = sum(counts.values())
    for size, users in chosen.items():
        events = sum(counts[u] for u in users)
        # At least the share, and short of it without the last user added.
        assert events >= total * size / 100 > events - max(counts.values())
    assert B.choose_users(counts, [25]) == {25: chosen[25]}   # same seed, same users


def test_make_samples_writes_each_log_filtered_and_reuses_the_manifest(tmp_path):
    raw, out = tmp_path / "raw", tmp_path / "samples"
    raw.mkdir()
    for name, users in (("log_a.csv", range(10)), ("log_b.csv", range(5, 20))):
        (raw / name).write_text("user_id,video_id\n" + "".join(f"{u},{u * 3}\n" for u in users))
    manifest = B.make_samples(raw, out, sizes=[25, 100])
    picked = {line.split(",")[0] for name in ("log_a.csv", "log_b.csv")
              for line in (out / "p25" / name).read_text().splitlines()[1:]}
    assert manifest["total_events"] == 25 and manifest["samples"]["25"]["events"] >= 25 / 4
    assert (out / "p25" / "log_a.csv").read_text().startswith("user_id,video_id\n")
    assert len(picked) == manifest["samples"]["25"]["users"]
    (out / "p25" / "log_a.csv").unlink()
    assert B.make_samples(raw, out, sizes=[25]) == manifest   # cached: same sources and seed


def fake_docker(outcomes):
    outcomes = iter(outcomes)

    def run(command, **kwargs):
        code, result = next(outcomes)
        stdout = "RESULT " + json.dumps(result) + "\n" if result else ""
        return subprocess.CompletedProcess(command, code, stdout=stdout, stderr="killed")
    return run


RESULT = {"seconds": 1.5, "peak_rss_mb": 900.0, "interactions_in": 10, "silver_rows": 9, "item_daily_rows": 4}


def test_pandas_runs_stop_at_the_memory_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(B, "PANDAS_OUT", tmp_path)
    rows = B.run_pandas(25, "2.5g", runs=3, warmup=1, run=fake_docker([(0, RESULT), (137, None)]),
                        clock=iter(range(100)).__next__)
    assert [(row["run"], row["warmup"], row["status"]) for row in rows] == [(1, True, "ok"), (2, False, "oom")]
    with pytest.raises(RuntimeError, match="pandas run failed"):
        B.run_pandas(25, "2.5g", runs=1, warmup=0, run=fake_docker([(1, None)]), clock=iter(range(9)).__next__)


def test_pandas_container_is_limited_and_read_only():
    command = B.pandas_command(50, "7.5g", B.PANDAS_OUT / "p50-7.5g")
    assert "--memory=7680m" in command and "--memory-swap=7680m" in command and f"--cpus={B.CPUS}" in command
    assert f"{B.SAMPLES / 'p50'}:/data/logs:ro" in command and f"{B.ROOT}:/opt/project:ro" in command
    assert f"{B.RAW}:/data/logs:ro" in B.pandas_command(100, "7.5g", B.PANDAS_OUT)


def test_summary_reports_out_of_memory_without_times():
    spark_rows = [{"variant": "spark_p25", "warmup": w, "seconds": s} for w, s in ((True, 9), (False, 5), (False, 7))]
    pandas_rows = [{"size_pct": 25, "memory": "2.5g", "warmup": True, "status": "oom", "seconds": 3}]
    summary = B.summarize(spark_rows, [{"size_pct": 25, **RESULT}], pandas_rows)
    assert summary[0]["median_seconds"] == 6 and summary[0]["silver_rows"] == 9
    assert summary[1] == {**dict.fromkeys(B.SUMMARY_FIELDS, ""), "engine": "pandas", "size_pct": 25,
                          "memory": "2.5g", "status": "oom", "runs": 0}
