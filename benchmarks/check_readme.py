"""Check README section 18's benchmark tables against benchmarks/results/.

    .venv/bin/python -m benchmarks.check_readme            # compare, exit 1 on any difference
    .venv/bin/python -m benchmarks.check_readme --print    # print the tables the CSVs give

Rebuilds every benchmark table (header, row order and every cell) from the
CSVs and compares it with the README's table at the same position in that
benchmark's subsection, cell by cell. Bold markers are ignored. Prose numbers
outside the tables are not checked.
"""

import argparse
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


def fmt(value, digits: int) -> str:
    """Round half up from the value's decimal form (0.105 -> 0.11)."""
    return str(Decimal(str(value)).quantize(Decimal(1).scaleb(-digits), rounding=ROUND_HALF_UP))


def span(values, digits) -> str:
    return f"{fmt(statistics.median(values), digits)} s ({fmt(min(values), digits)}–{fmt(max(values), digits)})"


def count(n: float) -> str:
    """Records: 2.42M, 444K, 96.8K."""
    if n >= 1e6:
        return f"{fmt(n / 1e6, 2)}M"
    return f"{fmt(n / 1e3, 0)}K" if n >= 1e5 else f"{fmt(n / 1e3, 1)}K"


def ratio(r: float) -> str:
    return f"{fmt(r, 0 if r >= 10 else 2)}×"


def size(n: float) -> str:
    """Decimal bytes: 0, 100 MB, 1.5 GB."""
    if n == 0:
        return "0"
    return f"{fmt(n / 1e9, 1)} GB" if n >= 1e9 else f"{fmt(n / 1e6, 0)} MB"


def megabytes(n: float) -> str:
    mb = n / 1e6
    return f"{fmt(mb, 0)} MB" if mb >= 100 else f"{fmt(mb, 1)} MB"


# --- Expected tables (header + rows, as lists of cells) ---------------------------

def scaling_tables(results: Path) -> list:
    summary = {int(r["variant"].split("_")[0]): r for r in read(results, "scaling_summary.csv")}
    base = float(summary[1]["median_seconds"])
    totals = [["Workers", "Task slots", "Median (min–max)", "Speedup"]] + [
        [str(w), str(2 * w), f"{fmt(r['median_seconds'], 0)} s ({fmt(r['min_seconds'], 0)}–{fmt(r['max_seconds'], 0)})",
         f"{fmt(base / float(r['median_seconds']), 2)}×"] for w, r in sorted(summary.items())]
    stages = [r for r in read(results, "scaling_stages.csv") if r["warmup"] == "False"]
    per_stage = [["Stage", "1 worker", "2 workers", "3 workers", "Speedup at 3"]]
    for name in dict.fromkeys(r["app_name"] for r in stages):
        # app_seconds repeats on every stage row of an app: one value per run.
        med = [statistics.median({r["run"]: float(r["app_seconds"]) for r in stages
                                  if r["variant"] == f"{w}_workers" and r["app_name"] == name}.values())
               for w in sorted(summary)]
        per_stage.append([name, *[f"{fmt(m, 0)} s" for m in med], f"{fmt(med[0] / med[-1], 2)}×"])
    return [totals, per_stage]


SCENARIO_LABELS = {"real-200": "Real keys, 200 partitions", "hot-200": "Hot key, 200 partitions",
                   "hot-24": "Hot key, 24 partitions"}


def skew_tables(results: Path) -> list:
    runs = [r for r in read(results, "skew_runs.csv") if r["warmup"] == "False"]
    joins = read(results, "skew_join_stages.csv")
    table = [["Scenario", "Variant", "End to end", "Join stage", "Tasks", "Slowest / median task duration",
              "Records/task max ÷ median", "Spill"]]
    shown = set()
    for variant in dict.fromkeys(r["variant"] for r in runs):
        scenario, name = variant.rsplit("_", 1)
        rows = [r for r in joins if r["variant"] == variant]

        def med(key):
            return statistics.median(float(r[key]) for r in rows)
        tasks = {r["tasks"] for r in rows}
        table.append([
            "" if scenario in shown else SCENARIO_LABELS[scenario], name,
            span([float(r["seconds"]) for r in runs if r["variant"] == variant], 1),
            span([float(r["seconds"]) for r in rows], 1),
            tasks.pop() if len(tasks) == 1 else "–".join(sorted(tasks)),
            f"{fmt(med('max_task_ms') / 1000, 2)} s / {fmt(med('median_task_ms') / 1000, 2)} s",
            f"{count(med('max_task_shuffle_read_records'))} ÷ {count(med('median_task_shuffle_read_records'))} = "
            f"{ratio(med('max_task_shuffle_read_records') / med('median_task_shuffle_read_records'))}",
            size(med("disk_spilled_bytes"))])
        shown.add(scenario)
    return [table]


def pandas_tables(results: Path) -> list:
    summary = read(results, "pandas_summary.csv")
    runs = read(results, "pandas_runs.csv")
    compared = {}
    for r in read(results, "pandas_compare.csv"):
        compared.setdefault(r["size_pct"], []).append(r["identical"] == "True")
    table = [["Size", "Events in → silver", "Spark (3 executors, 1 GiB heap each)", "pandas, 7.5 GiB limit",
              "pandas, 2.5 GiB limit", "Outputs"]]
    for spark in (r for r in summary if r["engine"] == "spark"):
        pct = spark["size_pct"]
        cells = [f"{pct}%", f"{fmt(int(spark['interactions_in']) / 1e6, 2)}M → {fmt(int(spark['silver_rows']) / 1e6, 2)}M",
                 f"{fmt(spark['median_seconds'], 1)} s ({fmt(spark['min_seconds'], 1)}–{fmt(spark['max_seconds'], 1)})"]
        for memory in ("7.5g", "2.5g"):
            r = next((r for r in summary if r["engine"] == "pandas" and r["memory"] == memory
                      and r["size_pct"] == pct), None)
            if r is None:
                cells.append("skipped")
            elif r["status"] == "oom":
                killed = next(float(x["seconds"]) for x in runs if x["memory"] == memory
                              and x["size_pct"] == pct and x["status"] == "oom")
                cells.append(f"out of memory after {fmt(killed, 1)} s")
            else:   # peak RSS is recorded in MiB
                cells.append(f"{fmt(r['median_seconds'], 1)} s ({fmt(r['min_seconds'], 1)}–{fmt(r['max_seconds'], 1)}), "
                             f"peak {fmt(float(r['median_peak_rss_mb']) / 1024, 1)} GiB")
        outcome = compared.get(pct)
        cells.append("Spark only" if outcome is None else
                     "matched within tolerance" if all(outcome) else "differed")
        table.append(cells)
    return [table]


def formats_tables(results: Path) -> list:
    rows = {(r["query"], r["format"]): r for r in read(results, "formats_query_summary.csv")}
    table = [["Query", "Bytes read: CSV → Parquet", "Query time: CSV → Parquet", "End to end: CSV → Parquet"]]
    for query in dict.fromkeys(q for q, _ in rows):
        c, p = rows[query, "csv"], rows[query, "parquet"]
        r = float(c["input_bytes"]) / float(p["input_bytes"])
        q = [f"{fmt(x['median_query_seconds'], 2)} s ({fmt(x['min_query_seconds'], 2)}–{fmt(x['max_query_seconds'], 2)})"
             for x in (c, p)]
        table.append([query, f"{megabytes(float(c['input_bytes']))} → {megabytes(float(p['input_bytes']))} "
                             f"({fmt(r, 0 if r >= 10 else 1)}× less)",
                      f"{q[0]} → {q[1]}", f"{fmt(c['median_seconds'], 1)} s → {fmt(p['median_seconds'], 1)} s"])
    return [table]


CHECKS = {"scaling": scaling_tables, "skew": skew_tables, "pandas": pandas_tables, "formats": formats_tables}


# --- README side -----------------------------------------------------------------

def section(readme: str, header: str) -> str:
    start = readme.index(header)
    end = readme.find("\n### ", start + len(header))
    return readme[start:end if end != -1 else None]


def tables(text: str) -> list:
    """Markdown tables in order, as lists of rows of cells (separator rows dropped)."""
    found, current = [], []
    for line in text.splitlines() + [""]:
        if line.startswith("|"):
            cells = [cell.strip().replace("**", "") for cell in line.strip().strip("|").split("|")]
            if not all(set(cell) <= set("-: ") for cell in cells):
                current.append(cells)
        elif current:
            found.append(current)
            current = []
    return found


def differences(readme: str, results: Path) -> dict:
    """Per benchmark: every cell where the README's tables differ from the CSVs."""
    out = {}
    for name, build in CHECKS.items():
        expected, actual, problems = build(results), tables(section(readme, SECTIONS[name])), []
        if len(actual) < len(expected):
            problems.append(f"expected {len(expected)} tables, found {len(actual)}")
        for t, (want, got) in enumerate(zip(expected, actual), 1):
            if len(want) != len(got):
                problems.append(f"table {t}: {len(got) - 1} rows, expected {len(want) - 1}")
            for i, (want_row, got_row) in enumerate(zip(want, got)):
                label = "header" if i == 0 else f"row {i} ({want_row[0] or want_row[1]})"
                if len(want_row) != len(got_row):
                    problems.append(f"table {t} {label}: {len(got_row)} cells, expected {len(want_row)}")
                    continue
                problems += [f"table {t} {label}, column {want[0][j]!r}: README {g!r}, CSVs give {w!r}"
                             for j, (w, g) in enumerate(zip(want_row, got_row)) if w != g]
        out[name] = problems
    return out


def markdown(table: list) -> str:
    rows = ["| " + " | ".join(row) + " |" for row in table]
    return "\n".join([rows[0], "|" + "---|" * len(table[0]), *rows[1:]])


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--print", action="store_true", help="print the expected tables as Markdown")
    args = parser.parse_args(argv)
    if args.print:
        for name, build in CHECKS.items():
            for table in build(common.RESULTS_DIR):
                print(f"{SECTIONS[name]}\n\n{markdown(table)}\n")
        return
    found = differences((ROOT / "README.md").read_text(), common.RESULTS_DIR)
    for name, problems in found.items():
        print(f"{name}: {'ok' if not problems else f'{len(problems)} differences'}")
        for problem in problems:
            print(f"  {problem}")
    sys.exit(1 if any(found.values()) else 0)


if __name__ == "__main__":
    main()
