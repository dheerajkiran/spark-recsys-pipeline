"""Check that README section 18's benchmark numbers match benchmarks/results/.

    .venv/bin/python -m benchmarks.check_readme

Recomputes each table cell (medians, min–max ranges, speedups, bytes) from
the CSVs, formatted as the README writes it, and requires it to appear in that
benchmark's subsection. Exits non-zero and lists every missing cell, so a
rerun benchmark whose README was not updated fails here.
"""

import csv
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
import statistics
import sys

from benchmarks import common

ROOT = Path(__file__).resolve().parents[1]
SECTIONS = {"scaling": "### Scaling (KuaiRand-1K)", "skew": "### Skew (KuaiRand-1K, item_daily join)",
            "pandas": "### Spark vs. pandas", "formats": "### CSV vs. Parquet"}


def read(results: Path, name: str) -> list:
    with (results / name).open() as handle:
        return list(csv.DictReader(handle))


def section(readme: str, header: str) -> str:
    start = readme.index(header)
    end = readme.find("\n### ", start + len(header))
    return readme[start:end if end != -1 else None].replace("**", "")


def fmt(value, digits: int) -> str:
    """Round half up from the value's decimal form, as the README does (0.105 -> 0.11)."""
    return str(Decimal(str(value)).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP))


def span(values, digits) -> str:
    return f"{fmt(statistics.median(values), digits)} s ({fmt(min(values), digits)}–{fmt(max(values), digits)})"


def scaling_cells(results: Path) -> list:
    summary = {int(r["variant"].split("_")[0]): r for r in read(results, "scaling_summary.csv")}
    base = float(summary[1]["median_seconds"])
    cells = [f"| {w} | {2 * w} | {fmt(r['median_seconds'], 0)} s ({fmt(r['min_seconds'], 0)}–"
             f"{fmt(r['max_seconds'], 0)}) | {fmt(base / float(r['median_seconds']), 2)}× |"
             for w, r in sorted(summary.items())]
    stages = [r for r in read(results, "scaling_stages.csv") if r["warmup"] == "False"]
    for name in dict.fromkeys(r["app_name"] for r in stages):
        med = [statistics.median({r["run"]: float(r["app_seconds"]) for r in stages
                                  if r["variant"] == f"{w}_workers" and r["app_name"] == name}.values())
               for w in (1, 2, 3)]
        cells.append(f"| {name} | {fmt(med[0], 0)} s | {fmt(med[1], 0)} s | {fmt(med[2], 0)} s | "
                     f"{fmt(med[0] / med[2], 2)}× |")
    return cells


def skew_cells(results: Path) -> list:
    runs = [r for r in read(results, "skew_runs.csv") if r["warmup"] == "False"]
    joins = read(results, "skew_join_stages.csv")
    cells = []
    for variant in dict.fromkeys(r["variant"] for r in runs):
        cells.append(span([float(r["seconds"]) for r in runs if r["variant"] == variant], 1))
        rows = [r for r in joins if r["variant"] == variant]
        cells.append(span([float(r["seconds"]) for r in rows], 1))
        cells.append(f"{fmt(statistics.median(float(r['max_task_ms']) for r in rows) / 1000, 2)} s / "
                     f"{fmt(statistics.median(float(r['median_task_ms']) for r in rows) / 1000, 2)} s")
    return cells


def pandas_cells(results: Path) -> list:
    cells = []
    for r in read(results, "pandas_summary.csv"):
        if r["status"] != "ok":
            continue
        times = f"{fmt(r['median_seconds'], 1)} s ({fmt(r['min_seconds'], 1)}–{fmt(r['max_seconds'], 1)})"
        if r["engine"] == "pandas":   # peak RSS is recorded in MiB
            times += f", peak {fmt(float(r['median_peak_rss_mb']) / 1024, 1)} GiB"
        cells.append(times)
    return cells


def megabytes(n: float) -> str:
    mb = n / 1e6
    return f"{fmt(mb, 0)} MB" if mb >= 100 else f"{fmt(mb, 1)} MB"


def formats_cells(results: Path) -> list:
    rows = {(r["query"], r["format"]): r for r in read(results, "formats_query_summary.csv")}
    cells = [megabytes(float(rows[next(iter(rows))]["stored_bytes"]))]
    for query in dict.fromkeys(q for q, _ in rows):
        c, p = rows[query, "csv"], rows[query, "parquet"]
        ratio = float(c["input_bytes"]) / float(p["input_bytes"])
        cells.append(f"| {query} | {megabytes(float(c['input_bytes']))} → {megabytes(float(p['input_bytes']))} "
                     f"({fmt(ratio, 0 if ratio >= 10 else 1)}× less)")
        q = [f"{fmt(r['median_query_seconds'], 2)} s ({fmt(r['min_query_seconds'], 2)}–"
             f"{fmt(r['max_query_seconds'], 2)})" for r in (c, p)]
        cells.append(f"{q[0]} → {q[1]}")
        cells.append(f"{fmt(c['median_seconds'], 1)} s → {fmt(p['median_seconds'], 1)} s")
        cells.append(megabytes(float(p["stored_bytes"])))
    return cells


CHECKS = {"scaling": scaling_cells, "skew": skew_cells, "pandas": pandas_cells, "formats": formats_cells}


def missing(readme: str, results: Path) -> dict:
    """Cells computed from the CSVs that the README does not contain, per benchmark."""
    return {name: [cell for cell in check(results) if cell not in section(readme, SECTIONS[name])]
            for name, check in CHECKS.items()}


def main() -> None:
    gaps = missing((ROOT / "README.md").read_text(), common.RESULTS_DIR)
    for name, cells in gaps.items():
        print(f"{name}: {'ok' if not cells else f'{len(cells)} missing'}")
        for cell in cells:
            print(f"  not in README: {cell}")
    sys.exit(1 if any(gaps.values()) else 0)


if __name__ == "__main__":
    main()
