"""Draw every benchmark plot from the CSVs in benchmarks/results/.

    .venv/bin/python -m benchmarks.plots [--results-dir DIR] [--out DIR]

Reads only saved results (no cluster), so deleting the plots and rerunning
this script regenerates them byte for byte. Writes PNGs to
benchmarks/results/plots/: scaling, skew_join, skew_timeline, pandas_spark
and formats.

Colors follow one fixed categorical order (blue, orange, aqua: a palette
checked for color-vision deficiency on this surface); worker counts, being
ordered, use steps of one blue ramp. Every series is also labelled directly,
and README section 18 has each plot's numbers as a table.
"""

import argparse
import csv
from pathlib import Path
import statistics

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

from benchmarks import common  # noqa: E402
from benchmarks.check_readme import ratio  # noqa: E402  (same rounding as the README table)

SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3de"
SERIES = ["#2a78d6", "#eb6834", "#1baf7a"]          # categorical slots 1-3
BLUE_RAMP = ["#86b6ef", "#2a78d6", "#104281"]       # ordinal: 1, 2, 3 workers
MUTED = "#a3a29c"

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.titlesize": 10, "axes.titleweight": "bold",
    "axes.titlelocation": "left", "axes.edgecolor": MUTED, "axes.labelcolor": INK_2, "axes.facecolor": SURFACE,
    "figure.facecolor": SURFACE, "xtick.color": INK_2, "ytick.color": INK_2, "text.color": INK,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "axes.axisbelow": True,
    "grid.color": GRID, "grid.linewidth": 0.8, "legend.frameon": False, "lines.linewidth": 2,
    "svg.hashsalt": "plots",
})


def read(results_dir: Path, name: str) -> list:
    with (results_dir / name).open() as handle:
        return list(csv.DictReader(handle))


PLAIN = FuncFormatter(lambda value, _: f"{value:g}")   # log axes: 1, 10, 100, not 10^0


def title(fig, text: str) -> None:
    fig.suptitle(text, x=0.01, y=1.03, ha="left", fontsize=11, fontweight="bold")


def save(fig, out: Path, name: str) -> Path:
    path = out / f"{name}.png"
    # No Software/date metadata, so reruns write identical bytes.
    fig.savefig(path, dpi=150, bbox_inches="tight", metadata={"Software": None})
    plt.close(fig)
    return path


def label_bars(ax, bars, fmt="{:.1f}", fontsize=7.5):
    for bar in bars:
        ax.annotate(fmt(bar.get_height()) if callable(fmt) else fmt.format(bar.get_height()), (bar.get_x() + bar.get_width() / 2, bar.get_height()),
                    xytext=(0, 2), textcoords="offset points", ha="center", va="bottom",
                    fontsize=fontsize, color=INK_2)


# --- Scaling ---------------------------------------------------------------------

def plot_scaling(results: Path, out: Path) -> Path:
    summary = {int(r["variant"].split("_")[0]): r for r in read(results, "scaling_summary.csv")}
    workers = sorted(summary)
    median = [float(summary[w]["median_seconds"]) for w in workers]
    low = [m - float(summary[w]["min_seconds"]) for w, m in zip(workers, median)]
    high = [float(summary[w]["max_seconds"]) - m for w, m in zip(workers, median)]
    stages = [r for r in read(results, "scaling_stages.csv") if r["warmup"] == "False"]
    names = list(dict.fromkeys(r["app_name"] for r in stages))
    per_stage = {}
    for w in workers:
        for name in names:
            # One app_seconds per (run, app); stage rows repeat it.
            runs = {r["run"]: float(r["app_seconds"]) for r in stages
                    if r["variant"] == f"{w}_workers" and r["app_name"] == name}
            per_stage[w, name] = statistics.median(runs.values())

    fig, (left, right) = plt.subplots(1, 2, figsize=(9.6, 3.4), gridspec_kw={"width_ratios": [1, 1.5]})
    left.errorbar(workers, median, yerr=[low, high], color=SERIES[0], marker="o", markersize=6, capsize=3)
    left.plot(workers, [median[0] / w for w in workers], color=MUTED, linestyle="--", linewidth=1.2)
    left.annotate("linear speedup", (3, median[0] / 3), xytext=(-4, -12), textcoords="offset points",
                  ha="right", fontsize=7.5, color=INK_2)
    for w, m in zip(workers, median):
        left.annotate(f"{m:.0f} s ({median[0] / m:.2f}×)", (w, m), xytext=(6, 4), textcoords="offset points",
                      fontsize=7.5, color=INK)
    left.set(title="End to end (median, min–max)", xlabel="Spark workers", ylabel="seconds",
             xticks=workers, ylim=(0, max(median) * 1.15), xlim=(0.7, 3.6))

    width = 0.26
    for i, w in enumerate(workers):
        bars = right.bar([j + (i - 1) * width for j in range(len(names))], [per_stage[w, n] for n in names],
                         width - 0.03, color=BLUE_RAMP[i], label=f"{w} worker{'s' if w > 1 else ''}")
        label_bars(right, bars, "{:.0f}")
    right.set(title="Per stage (median application time)", ylabel="seconds",
              xticks=range(len(names)), xticklabels=names)
    right.grid(axis="x", visible=False)
    right.legend(loc="upper left")
    title(fig, "Scaling: full KuaiRand-1K pipeline, 1–3 workers on one laptop")
    return save(fig, out, "scaling")


# --- Skew ------------------------------------------------------------------------

SCENARIOS = [("real-200", "Real keys\n200 partitions"), ("hot-200", "Hot key\n200 partitions"),
             ("hot-24", "Hot key\n24 partitions")]
VARIANTS = ["naive", "aqe", "salted"]


def plot_skew_join(results: Path, out: Path) -> Path:
    rows = read(results, "skew_join_stages.csv")

    def med(scenario, variant, key):
        return statistics.median(float(r[key]) for r in rows if r["variant"] == f"{scenario}_{variant}")

    fig, (left, right) = plt.subplots(1, 2, figsize=(9.6, 3.4))
    width = 0.26
    for i, variant in enumerate(VARIANTS):
        x = [j + (i - 1) * width for j in range(len(SCENARIOS))]
        bars = left.bar(x, [med(s, variant, "seconds") for s, _ in SCENARIOS], width - 0.03,
                        color=SERIES[i], label=variant)
        label_bars(left, bars)
        ratios = [med(s, variant, "max_task_shuffle_read_records") / med(s, variant, "median_task_shuffle_read_records")
                  for s, _ in SCENARIOS]
        bars = right.bar(x, ratios, width - 0.03, color=SERIES[i], label=variant)
        label_bars(right, bars, ratio, fontsize=6.2)
    for ax in (left, right):
        ax.set_xticks(range(len(SCENARIOS)), [label for _, label in SCENARIOS])
        ax.grid(axis="x", visible=False)
    left.set(title="Join stage time (median)", ylabel="seconds")
    left.legend(loc="upper left", ncols=3)
    right.set(title="Partition skew: max ÷ median shuffle records per task", ylabel="ratio (log scale)",
              yscale="log", ylim=(0.8, 60))
    right.yaxis.set_major_formatter(PLAIN)
    right.axhline(5, color=MUTED, linestyle="--", linewidth=1.2)
    right.annotate("AQE splits a partition\nabove 5× the median", (-0.45, 5), xytext=(0, 4),
                   textcoords="offset points", va="bottom", fontsize=7.5, color=INK_2)
    title(fig, "Skew: item_daily join on KuaiRand-1K, broadcast off")
    return save(fig, out, "skew_join")


def lanes(tasks, slots_per_executor=2):
    """Assign each task to a core lane of its executor, in start order."""
    free, placed = {}, []
    for t in sorted(tasks, key=lambda t: (t["start"], t["task_id"])):
        ends = free.setdefault(t["executor"], [0] * slots_per_executor)
        lane = min(range(slots_per_executor), key=lambda i: (ends[i] > t["start"], ends[i]))
        ends[lane] = t["start"] + t["duration"]
        placed.append({**t, "lane": int(t["executor"]) * slots_per_executor + lane})
    return placed


def plot_skew_timeline(results: Path, out: Path) -> Path:
    rows = read(results, "skew_tasks.csv")
    fig, axes = plt.subplots(2, 1, figsize=(9.6, 4.2), sharex=True)
    titles = {"hot-200_naive": "naive (AQE off): one task holds the hot key",
              "hot-200_aqe": "AQE skew join: the hot partition is split over 7 tasks (one ran only 0.1 s)"}
    for ax, variant in zip(axes, titles):
        tasks = [{"task_id": int(r["task_id"]), "executor": r["executor_id"], "start": int(r["start_ms"]) / 1000,
                  "duration": int(r["duration_ms"]) / 1000, "records": int(r["shuffle_read_records"])}
                 for r in rows if r["variant"] == variant]
        typical = statistics.median(t["records"] for t in tasks)
        for t in lanes(tasks):
            hot = t["records"] > 2 * typical
            ax.barh(t["lane"], t["duration"], left=t["start"], height=0.72,
                    color=SERIES[1] if hot else SERIES[0], edgecolor=SURFACE, linewidth=0.6)
        end = max(t["start"] + t["duration"] for t in tasks)
        ax.axvline(end, color=MUTED, linestyle="--", linewidth=1)
        ax.annotate(f"stage ends {end:.1f} s", (end, 5.4), xytext=(4, 0), textcoords="offset points",
                    fontsize=7.5, color=INK_2, va="center")
        ax.set_yticks(range(6), [f"exec {e} · core {c}" for e in range(3) for c in range(2)], fontsize=7.5)
        ax.set_ylim(-0.6, 5.8)
        ax.invert_yaxis()
        ax.set_title(titles[variant])
        ax.grid(axis="y", visible=False)
    handles = [plt.Rectangle((0, 0), 1, 1, color=SERIES[0]), plt.Rectangle((0, 0), 1, 1, color=SERIES[1])]
    fig.legend(handles, ["task", "task reading > 2× the median task's shuffle records (hot key)"],
               loc="upper center", bbox_to_anchor=(0.5, -0.01), ncols=2, fontsize=7.5)
    axes[1].set_xlabel("seconds since the join stage's first task started")
    title(fig, "Join-stage task timeline, hot key at 200 partitions (run 3: the second timed run)")
    return save(fig, out, "skew_timeline")


# --- Spark vs pandas ---------------------------------------------------------------

def plot_pandas_spark(results: Path, out: Path) -> Path:
    summary = read(results, "pandas_summary.csv")
    runs = read(results, "pandas_runs.csv")
    events = {int(r["size_pct"]): int(r["interactions_in"]) / 1e6
              for r in summary if r["engine"] == "spark"}
    fig, (left, right) = plt.subplots(1, 2, figsize=(9.6, 3.6))
    series = [("spark", "", "Spark (3 executors)", SERIES[0]), ("pandas", "7.5g", "pandas, 7.5 GiB limit", SERIES[1]),
              ("pandas", "2.5g", "pandas, 2.5 GiB limit", SERIES[2])]
    for engine, memory, label, color in series:
        rows = [r for r in summary if r["engine"] == engine and r["memory"] == memory]
        ok = [r for r in rows if r["status"] == "ok"]
        x = [events[int(r["size_pct"])] for r in ok]
        y = [float(r["median_seconds"]) for r in ok]
        err = [[v - float(r["min_seconds"]) for v, r in zip(y, ok)], [float(r["max_seconds"]) - v for v, r in zip(y, ok)]]
        left.errorbar(x, y, yerr=err, color=color, marker="o", markersize=6, capsize=3, label=label)
        # A killed run has no result; its marker shows how long it ran before the kill.
        for r in runs:
            if r["memory"] == memory and r["status"] == "oom" and engine == "pandas":
                xo, yo = events[int(r["size_pct"])], float(r["seconds"])
                left.scatter([xo], [yo], marker="x", s=60, color=color, linewidths=2, zorder=3)
                right_side = xo < 6
                left.annotate(f"killed: out of memory\nafter {yo:.1f} s", (xo, yo), xytext=(8 if right_side else -8, 0),
                              textcoords="offset points", ha="left" if right_side else "right", va="center",
                              fontsize=7.5, color=INK_2)
    left.yaxis.set_major_formatter(PLAIN)
    left.legend(loc="upper left", fontsize=7.5)
    left.set(title="Runtime (median, min–max; log scale)", xlabel="events in (millions)", ylabel="seconds",
             yscale="log", ylim=(0.5, 200))

    for memory, limit_gb, color in (("7.5g", 7.5, SERIES[1]), ("2.5g", 2.5, SERIES[2])):
        ok = [r for r in runs if r["memory"] == memory and r["status"] == "ok" and r["warmup"] == "False"]
        sizes = sorted({int(r["size_pct"]) for r in ok})
        peaks = [statistics.median(float(r["peak_rss_mb"]) / 1024 for r in ok if int(r["size_pct"]) == s)
                 for s in sizes]
        right.plot([events[s] for s in sizes], peaks, color=color, marker="o", markersize=6,
                   label=f"pandas, {limit_gb:g} GiB limit")
        right.axhline(limit_gb, color=color, linestyle="--", linewidth=1.2)
        right.annotate(f"{limit_gb:g} GiB limit", (7.0, limit_gb), xytext=(0, 3), textcoords="offset points",
                       ha="center", fontsize=7.5, color=INK_2)
        for r in runs:
            if r["memory"] == memory and r["status"] == "oom":
                right.scatter([events[int(r["size_pct"])]], [limit_gb], marker="x", s=60, color=color,
                              linewidths=2, zorder=3)
    right.scatter([], [], marker="x", s=60, color=INK_2, linewidths=2, label="killed at its limit")
    right.legend(loc="upper left", fontsize=7.5)
    right.set(title="pandas peak memory (process RSS)", xlabel="events in (millions)", ylabel="GiB",
              ylim=(0, 9.8), xlim=(0, 12.6))
    title(fig, "Spark vs pandas: ingest + clean + feature subset, KuaiRand-1K samples")
    return save(fig, out, "pandas_spark")


# --- File formats -------------------------------------------------------------------

def plot_formats(results: Path, out: Path) -> Path:
    rows = read(results, "formats_query_summary.csv")
    queries = list(dict.fromkeys(r["query"] for r in rows))
    get = {(r["query"], r["format"]): r for r in rows}
    fig, (left, right) = plt.subplots(1, 2, figsize=(9.6, 3.4))
    width = 0.38
    for i, fmt in enumerate(["csv", "parquet"]):
        x = [j + (i - 0.5) * width for j in range(len(queries))]
        mb = [float(get[q, fmt]["input_bytes"]) / 1e6 for q in queries]
        bars = left.bar(x, mb, width - 0.04, color=SERIES[i], label=fmt.upper() if fmt == "csv" else "Parquet")
        label_bars(left, bars, "{:.0f}" if fmt == "csv" else "{:.1f}")
        secs = [float(get[q, fmt]["median_query_seconds"]) for q in queries]
        err = [[s - float(get[q, fmt]["min_query_seconds"]) for s, q in zip(secs, queries)],
               [float(get[q, fmt]["max_query_seconds"]) - s for s, q in zip(secs, queries)]]
        bars = right.bar(x, secs, width - 0.04, color=SERIES[i], yerr=err, ecolor=INK_2, capsize=2,
                         error_kw={"linewidth": 1})
        label_bars(right, bars, "{:.2f}")
    for ax in (left, right):
        ax.set_xticks(range(len(queries)), [q.replace("_", " ") for q in queries])
        ax.grid(axis="x", visible=False)
    left.set(title="Bytes read (log scale)", ylabel="MB", yscale="log", ylim=(1, 4000))
    left.yaxis.set_major_formatter(PLAIN)
    left.legend(loc="upper right", ncols=2)
    right.set(title="Query time (median, min–max; without start-up)", ylabel="seconds")
    title(fig, "CSV vs Parquet: the same queries on 11.76M interactions")
    return save(fig, out, "formats")


PLOTS = [plot_scaling, plot_skew_join, plot_skew_timeline, plot_pandas_spark, plot_formats]


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--results-dir", type=Path, default=common.RESULTS_DIR)
    parser.add_argument("--out", type=Path, default=common.RESULTS_DIR / "plots")
    args = parser.parse_args(argv)
    args.out.mkdir(parents=True, exist_ok=True)
    for plot in PLOTS:
        print(f"wrote {plot(args.results_dir, args.out)}")


if __name__ == "__main__":
    main()
