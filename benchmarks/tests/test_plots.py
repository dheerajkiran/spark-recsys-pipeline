"""Plots regenerate identically from the saved CSVs; README numbers match them."""

import hashlib

import pytest

from benchmarks import check_readme, common, plots


def digests(directory):
    return {path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(directory.glob("*.png"))}


def test_plots_regenerate_byte_for_byte(tmp_path):
    first, second = tmp_path / "a", tmp_path / "b"
    for out in (first, second):
        out.mkdir()
        plots.main(["--out", str(out)])
    assert set(digests(first)) == {"scaling.png", "skew_join.png", "skew_timeline.png", "pandas_spark.png",
                                   "formats.png"}
    assert digests(first) == digests(second)
    # The committed plots are the ones the current CSVs produce.
    assert digests(common.RESULTS_DIR / "plots") == digests(first)


def test_readme_tables_match_the_results():
    readme = (check_readme.ROOT / "README.md").read_text()
    assert check_readme.differences(readme, common.RESULTS_DIR) == dict.fromkeys(check_readme.CHECKS, [])


@pytest.mark.parametrize("old, new, benchmark", [
    ("| 2 | 4 | 467 s", "| 2 | 4 | 470 s", "scaling"),                   # one number
    ("| 4.1 s (4.0–4.3) | 206 |", "| 4.1 s (4.0–4.3) | 999 |", "skew"),   # a task count
    ("| naive | 15.6 s (14.9–17.1) |", "| naive | 14.4 s (13.5–14.4) |", "skew"),   # values swapped between rows
    ("| 25% |", "| 50% |", "pandas"),                                    # a row label
    ("| one_user |", "| two_columns |", "formats"),
])
def test_any_changed_table_cell_is_reported(old, new, benchmark):
    readme = (check_readme.ROOT / "README.md").read_text()
    assert readme.count(old) == 1
    found = check_readme.differences(readme.replace(old, new), common.RESULTS_DIR)
    assert found[benchmark] and not any(v for k, v in found.items() if k != benchmark)


def test_a_dropped_row_is_reported():
    readme = (check_readme.ROOT / "README.md").read_text()
    row = next(line for line in readme.splitlines() if line.startswith("| date_range |"))
    assert "rows, expected" in " ".join(check_readme.differences(readme.replace(row + "\n", ""),
                                                                   common.RESULTS_DIR)["formats"])


def test_half_up_rounding_matches_the_readme():
    assert check_readme.fmt(0.105, 2) == "0.11" and check_readme.fmt("1.615", 2) == "1.62"
    assert check_readme.fmt(646.086, 0) == "646"
