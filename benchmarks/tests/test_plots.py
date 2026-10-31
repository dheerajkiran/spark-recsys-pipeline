"""Plots regenerate identically from the saved CSVs; README numbers match them."""

import hashlib

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


def test_readme_numbers_match_the_results():
    readme = (check_readme.ROOT / "README.md").read_text()
    assert check_readme.missing(readme, common.RESULTS_DIR) == dict.fromkeys(check_readme.CHECKS, [])
    # A stale number is caught: change one scaling median in the README.
    stale = readme.replace("| 2 | 4 | 467 s", "| 2 | 4 | 470 s")
    assert check_readme.missing(stale, common.RESULTS_DIR)["scaling"]


def test_half_up_rounding_matches_the_readme():
    assert check_readme.fmt(0.105, 2) == "0.11" and check_readme.fmt("1.615", 2) == "1.62"
    assert check_readme.fmt(646.086, 0) == "646"
