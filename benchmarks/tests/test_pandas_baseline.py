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


class FakeDocker:
    """docker run / inspect / rm: each run is (exit code, RESULT dict or None, OOMKilled)."""

    def __init__(self, outcomes, inspect_fails=False):
        self.outcomes, self.inspect_fails = iter(outcomes), inspect_fails
        self.started, self.removed, self.state = [], [], {}

    def __call__(self, command, **kwargs):
        verb = command[1]
        if verb == "run":
            name = next(arg for arg in command if arg.startswith("--name=")).removeprefix("--name=")
            code, result, oom = next(self.outcomes)
            self.started.append(name)
            self.state[name] = {"ExitCode": code, "OOMKilled": oom}
            stdout = "RESULT " + json.dumps(result) + "\n" if result else ""
            return subprocess.CompletedProcess(command, code, stdout=stdout, stderr="stderr text")
        if verb == "inspect":
            if self.inspect_fails:
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="No such object")
            return subprocess.CompletedProcess(command, 0, stdout=json.dumps(self.state[command[-1]]), stderr="")
        if verb == "rm":
            self.removed.append(command[-1])
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        raise AssertionError(command)


RESULT = {"seconds": 1.5, "peak_rss_mb": 900.0, "interactions_in": 10, "silver_rows": 9, "item_daily_rows": 4}


def pandas_runs(docker, tmp_path, monkeypatch, runs=3, warmup=1):
    monkeypatch.setattr(B, "PANDAS_OUT", tmp_path)
    return B.run_pandas(25, "2.5g", runs=runs, warmup=warmup, run=docker, clock=iter(range(100)).__next__)


def test_successful_runs_are_parsed_and_every_container_removed(tmp_path, monkeypatch):
    docker = FakeDocker([(0, RESULT, False)] * 4)
    rows = pandas_runs(docker, tmp_path, monkeypatch)
    assert [(row["run"], row["status"], row["peak_rss_mb"]) for row in rows] == [(n, "ok", 900.0) for n in range(1, 5)]
    assert len(set(docker.started)) == 4 and docker.removed == docker.started


def test_only_a_docker_confirmed_oom_kill_is_recorded_as_oom(tmp_path, monkeypatch):
    docker = FakeDocker([(0, RESULT, False), (137, None, True)])
    rows = pandas_runs(docker, tmp_path, monkeypatch)
    assert [(row["run"], row["warmup"], row["status"]) for row in rows] == [(1, True, "ok"), (2, False, "oom")]
    assert rows[1]["seconds"] == 1   # timed before inspection: clock ticks 2 -> 3
    assert docker.removed == docker.started


def test_sigkill_without_oom_fails_instead_of_counting_as_oom(tmp_path, monkeypatch):
    docker = FakeDocker([(137, None, False)])
    with pytest.raises(RuntimeError, match="did not report an OOM kill"):
        pandas_runs(docker, tmp_path, monkeypatch)
    assert docker.removed == docker.started != []


@pytest.mark.parametrize("docker, message", [
    (FakeDocker([(1, None, False)]), "pandas run failed"),
    (FakeDocker([(0, None, False)]), "one RESULT line"),                  # exit 0 but no output
    (FakeDocker([(0, RESULT, False)], inspect_fails=True), "docker inspect"),
])
def test_failures_are_reported_and_the_container_still_removed(tmp_path, monkeypatch, docker, message):
    with pytest.raises(RuntimeError, match=message):
        pandas_runs(docker, tmp_path, monkeypatch)
    assert docker.removed == docker.started != []


def test_pandas_container_is_named_limited_and_read_only():
    command = B.pandas_command(50, "7.5g", B.PANDAS_OUT / "p50-7.5g", "pandas-bench-abc")
    assert "--rm" not in command and "--name=pandas-bench-abc" in command
    assert "--memory=7680m" in command and "--memory-swap=7680m" in command and f"--cpus={B.CPUS}" in command
    assert f"{B.SAMPLES / 'p50'}:/data/logs:ro" in command and f"{B.ROOT}:/opt/project:ro" in command
    assert f"{B.RAW}:/data/logs:ro" in B.pandas_command(100, "7.5g", B.PANDAS_OUT, "x")


def test_summary_reports_out_of_memory_without_times():
    spark_rows = [{"variant": "spark_p25", "warmup": w, "seconds": s} for w, s in ((True, 9), (False, 5), (False, 7))]
    pandas_rows = [{"size_pct": 25, "memory": "2.5g", "warmup": True, "status": "oom", "seconds": 3}]
    summary = B.summarize(spark_rows, [{"size_pct": 25, **RESULT}], pandas_rows)
    assert summary[0]["median_seconds"] == 6 and summary[0]["silver_rows"] == 9
    assert summary[1] == {**dict.fromkeys(B.SUMMARY_FIELDS, ""), "engine": "pandas", "size_pct": 25,
                          "memory": "2.5g", "status": "oom", "runs": 0}
