# spark-recsys-pipeline

A distributed data preprocessing pipeline that turns raw short-video interaction logs into a training dataset for a recommendation model.

Raw logs land in **HDFS**, **Apache Spark** cleans them and builds features across a cluster of workers, and the result is written back as partitioned **Parquet**. A **PyTorch** CTR model then trains on that data.

> **Status:** Planning. This README describes what will be built. Sections marked _(planned)_ will be filled in with real results as each phase is completed. Numbers such as memory sizes and thresholds are starting values and will be tuned once real data is loaded.

---

## Table of contents

1. [What we are building](#1-what-we-are-building)
2. [Why](#2-why)
3. [Key concepts](#3-key-concepts)
4. [Architecture](#4-architecture)
5. [Tech stack: what, where, and why](#5-tech-stack-what-where-and-why)
6. [Cluster configuration](#6-cluster-configuration)
7. [Data model](#7-data-model)
8. [How: pipeline stages](#8-how-pipeline-stages)
9. [Feature catalog](#9-feature-catalog)
10. [Data-quality checks](#10-data-quality-checks)
11. [Model training](#11-model-training)
12. [Engineering decisions](#12-engineering-decisions)
13. [Benchmark methodology](#13-benchmark-methodology)
14. [When: roadmap](#14-when-roadmap)
15. [Risks and mitigations](#15-risks-and-mitigations)
16. [Repository layout](#16-repository-layout)
17. [Running locally](#17-running-locally-planned)
18. [Results](#18-results-planned)

---

## 1. What we are building

The pipeline does four things end to end:

- **Stores** raw user–video interaction logs in a distributed file system (HDFS).
- **Processes** them in parallel with Spark: validation, cleaning, feature engineering, aggregation and a time-based train/val/test split.
- **Produces** a model-ready training dataset in Parquet, partitioned by date.
- **Trains** a click-through-rate (CTR) model (DeepFM) in PyTorch on that dataset, and reports AUC and log loss.

The whole cluster (HDFS and Spark) runs locally with Docker Compose. Anyone can reproduce it on a laptop, and it uses the same architecture as a production cluster. Only the number and size of machines differ.

### The ML task

> Given a user, a video, and the context of the moment (time, recent activity), predict the probability that the user **clicks** the video.

This is the core ranking problem behind short-video feeds and ad systems. A secondary label, **long view** (the user watched a large fraction of the video), can be swapped in with a config change.

### Dataset

[**KuaiRand**](https://kuairand.com/) is the primary dataset. It contains real interaction logs from Kuaishou, a short-video platform, including watch time, clicks, likes, follows and timestamps, plus user and video metadata. It fits the problem because:

- It is real feed-recommendation data, not synthetic.
- It comes in several sizes (`KuaiRand-Pure`, `KuaiRand-1K`, `KuaiRand-27K`). We develop on the small version and run benchmarks on the largest.
- The largest version is big enough that single-machine pandas becomes slow or runs out of memory, so Spark is justified.
- It has timestamps, so we can build sequential "user history" features and a leakage-free time split.

[KuaiRec](https://kuaisou.github.io/KuaiRec/) is a smaller fallback for fast iteration during development.

---

## 2. Why

Recommendation systems at scale depend less on model architecture than on the **data pipeline** that feeds them. Raw logs are huge, noisy and skewed: a few popular videos get most of the interactions. They have to be turned into correct, leakage-free features before any model sees them.

This project shows how to do that work at scale:

| Problem at scale | How this project addresses it |
|---|---|
| Data does not fit on one machine | HDFS for storage, Spark for parallel compute |
| Slow full-table scans | Columnar Parquet, partitioned by date |
| Hot keys (viral videos) stall joins | Key salting and broadcast joins |
| Features accidentally use future data | Window functions bounded to past events, time-based split |
| Silent data corruption between stages | Schema and data-quality checks after each stage |
| Re-running everything after one bug | Persisted bronze/silver/gold layers; each stage re-runs on its own |
| "Does it actually scale?" | Benchmarks with 1, 2 and 3 workers, compared to a pandas baseline |

---

## 3. Key concepts

A short primer on the ideas the rest of this README relies on.

### HDFS

- **NameNode**: stores the file system metadata, that is, which files exist and which blocks make up each file. It holds no file data itself.
- **DataNode**: stores the actual blocks on its local disk. Our cluster has two.
- **Block**: files are split into fixed-size blocks (128 MB by default) spread across DataNodes.
- **Replication**: each block is copied to several DataNodes (the default is 3; we use 2 because we only have two DataNodes). If one node dies, the data is still readable.
- **Data locality**: Spark prefers to schedule a task on the node that already holds the block it reads, so data does not cross the network. (In our Docker setup, Spark workers and DataNodes are separate containers, so every read goes over the Docker network. See [section 6](#6-cluster-configuration).)

### Spark

- **Driver**: the process that runs our PySpark script, builds the execution plan and schedules work.
- **Executor**: a process on a worker that runs tasks and caches data. Each worker in our cluster runs one executor.
- **Partition**: a chunk of a DataFrame. One partition is processed by one task on one core.
- **Lazy evaluation**: transformations (`filter`, `join`, `groupBy`) only build a plan. Nothing runs until an action (`write`, `count`) is called. This lets the Catalyst optimizer rewrite the whole plan first.
- **Narrow vs. wide transformations**:
  - Narrow (`filter`, `select`, `withColumn`) work within a partition and need no data movement.
  - Wide (`groupBy`, `join`, `window` with `partitionBy`) need a **shuffle**, which redistributes rows across the network by key. Shuffles are the main cost in most Spark jobs, so much of this project is about reducing or balancing them.
- **Job → stage → task**: each action creates a job, the job is split into stages at shuffle boundaries, and each stage runs one task per partition.
- **Skew**: when one key has far more rows than others, the task handling that key runs much longer than the rest, and the whole stage waits for it.
- **AQE (Adaptive Query Execution)**: Spark 3 feature that re-optimizes the plan at runtime using real statistics. It merges small shuffle partitions, splits skewed ones, and switches join strategies.

### Medallion layers

| Layer | Content | Purpose |
|---|---|---|
| **raw** | Original CSVs, untouched | Source of truth; can always re-run from here |
| **bronze** | Same data, typed and in Parquet | Fast, schema-enforced reads |
| **silver** | Cleaned and deduplicated | Trusted base for feature engineering |
| **gold** | Features joined and split | Directly consumable by model training |

---

## 4. Architecture

```
            Raw interaction logs (CSV)
                        │
                        ▼
      ┌─────────────────────────────────────┐
      │ HDFS                                │
      │ namenode + 2 datanodes              │
      │ /raw   /bronze   /silver   /gold    │
      └─────────────────────────────────────┘
                        │
                        ▼
      ┌─────────────────────────────────────┐
      │ Spark standalone cluster            │
      │ master + 3 workers                  │
      │                                     │
      │ Worker 1 ─► partitions 1..k         │
      │ Worker 2 ─► partitions k+1..2k      │
      │ Worker 3 ─► partitions 2k+1..n      │
      └─────────────────────────────────────┘
                        │
    1. Ingest       raw CSV    → bronze (typed Parquet)
    2. Clean        bronze     → silver (deduped, filtered)
    3. Features     silver     → user / item / context features
    4. Aggregate    join all features → one row per interaction
    5. Split        time-based train / val / test → gold
       (quality checks run after every stage)
                        │
                        ▼
      Gold Parquet dataset on HDFS
                        │
                        ▼
      PyTorch DataLoader → DeepFM → AUC, log loss
```

### How a stage executes

Taking the feature stage as an example:

1. `spark-submit` starts the **driver**, which connects to the Spark master.
2. The master assigns **executors** on the three workers.
3. The driver reads the silver Parquet metadata from the NameNode and plans one task per input split.
4. Executors read blocks directly from the DataNodes (on a real cluster, preferring blocks stored on their own node).
5. Narrow operations (filters, derived columns) run in place. A `groupBy user_id` triggers a shuffle, so rows with the same user end up in the same partition.
6. Results are written back to HDFS as Parquet, one file per output partition.

---

## 5. Tech stack: what, where, and why

| Layer | Technology | Where it is used | Why this choice |
|---|---|---|---|
| Storage | **HDFS** (Hadoop 3.3.6) | Holds the raw, bronze, silver and gold datasets | Distributed and fault-tolerant, and data locality lets Spark read blocks on the node where they are stored. It is the standard storage layer in the Hadoop ecosystem. |
| Compute | **Apache Spark 3.5.9** (PySpark) | Every pipeline stage in `pipeline/` | In-memory distributed processing, the Catalyst query optimizer, and the DataFrame API. AQE helps with skew and shuffle sizing. |
| File format | **Apache Parquet** (Snappy compression) | Every layer after raw | Columnar storage with compression, predicate pushdown and column pruning. It is much faster to scan than CSV and stores the schema with the data. |
| Cluster runtime | **Docker Compose** | `docker-compose.yml` | Runs the multi-node HDFS and Spark cluster on one laptop. Reproducible, and nothing to install beyond Docker. |
| Language | **Python 3.10** | Pipeline, training and tooling | One language from ETL through to model training. 3.10 is what the official Spark image ships, and the driver and executors must run the same Python version. |
| ML training | **PyTorch 2.x** | `training/` | Industry standard for recommendation models, and it has a flexible `Dataset` and `DataLoader` for streaming Parquet. |
| Parquet → tensors | **PyArrow** | `training/dataset.py` | Reads Parquet shards efficiently without going through pandas. |
| Metrics | **scikit-learn** | `training/` | Standard AUC and log-loss implementations. |
| Data quality | Custom PySpark checks | `pipeline/quality_checks.py` | Lightweight assertions on schema, null rates, row counts and value ranges, with no extra framework. |
| Configuration | **YAML** config file | `config/pipeline.yaml` | Paths, thresholds and label choice live in one place instead of being hard-coded. |
| Testing | **pytest** with a local `SparkSession` | `tests/` | Unit tests for each transformation on small hand-built DataFrames. |
| Benchmarks and plots | **pandas**, **matplotlib** | `benchmarks/` | Single-machine baseline for comparison, and plots of scaling results. |
| Monitoring | **Spark UI** and History Server | During development | Inspect stages, shuffles, skew and task timelines. Screenshots go in this README. |

**Considered and not used (for now):**

- **YARN** as the resource manager: Spark standalone mode is simpler to run in Docker and teaches the same partitioning and shuffle concepts. YARN is a possible stretch goal.
- **Hive / Delta Lake**: plain partitioned Parquet keeps the focus on Spark itself. Delta Lake is a possible extension for ACID writes and time travel.
- **Airflow**: stages are run by a simple driver script first. Orchestration can come later.
- **Spark MLlib for training**: MLlib does not support deep CTR models like DeepFM. Spark handles data, PyTorch handles the model, which is how most production systems split the work.

---

## 6. Cluster configuration

Sized to fit a laptop with 16 GB of RAM, with about 11 GB given to Docker. Built and verified in phase 1 on an Apple Silicon Mac with [OrbStack](https://orbstack.dev/).

**Images**
- **Hadoop:** the official `apache/hadoop` images are amd64-only, so `docker/hadoop/Dockerfile` builds our own from the Apache 3.3.6 release tarball (the aarch64 or x86 build, picked automatically and checked against its SHA-512) on `eclipse-temurin:11-jre`. Hadoop 3.3 supports Java 8 and 11 at runtime.
- **Spark:** `docker/spark/Dockerfile` is a thin layer over the official multi-arch `apache/spark:3.5.9-java17-python3` (Python 3.10). It adds `spark-defaults.conf`, the pipeline's Python dependencies, and an event-log directory.

All host ports bind to `127.0.0.1`. HDFS and Spark run without authentication here, so they are not reachable from the local network.

### Containers

| Service | Image base | Role | Resources | Ports |
|---|---|---|---|---|
| `namenode` | Hadoop 3.3.6 (own build) | HDFS metadata | 1 GB | 9870 (UI), 9000 (RPC) |
| `datanode-1`, `datanode-2` | Hadoop 3.3.6 (own build) | HDFS block storage | 1 GB each | 9864, 9865 (UI) |
| `spark-master` | Spark 3.5.9 | Cluster manager; also runs the driver of jobs submitted from it | 1.5 GB | 8080 (UI), 7077 (RPC), 4040 (app UI) |
| `spark-worker-1..3` | Spark 3.5.9 | One executor each (2 cores, 1 GB heap) | 2 cores, 2 GB each | 8081–8083 (UI) |
| `spark-history` | Spark 3.5.9 | History Server for finished jobs | 512 MB | 18080 (UI) |

**Data locality caveat:** Spark workers and DataNodes run in separate containers, so executors never find a block on their own "node" and every HDFS read crosses the Docker network. On one laptop this costs little, since the network is virtual. Co-locating a DataNode and a worker in one container is a possible later experiment.

### Key Spark settings

| Setting | Value | Why |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` | Enable AQE (turned off for the "naive" skew benchmark) |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Let AQE split skewed join partitions |
| `spark.sql.shuffle.partitions` | `24` initially | Default of 200 is too many for 6 cores; AQE coalesces further |
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` (default) | Small tables broadcast automatically; we also broadcast explicitly where needed |
| `spark.serializer` | `KryoSerializer` | Faster and more compact than Java serialization |
| `spark.eventLog.enabled` | `true` | Needed for the History Server and benchmark evidence |
| `dfs.replication` (HDFS) | `2` | Matches the number of DataNodes. Also set as `spark.hadoop.dfs.replication`, because Spark's HDFS client does not read the cluster's `hdfs-site.xml` and would otherwise write files with 3 replicas |

### HDFS directory layout

```
hdfs:///
├── raw/kuairand/                  # original CSVs
├── bronze/interactions/event_date=YYYYMMDD/   # standard + random logs (is_rand)
├── bronze/users/  bronze/videos/  bronze/video_stats/
├── silver/interactions/event_date=YYYYMMDD/
├── silver/users/  silver/videos/
├── features/events/event_date=YYYYMMDD/       # one row per event: labels, history, context, cross
├── features/item_daily/  features/author_daily/   # previous-day stats per (key, date)
├── features/user_static/  features/video_static/  # encoded dimension tables
├── gold/train/  gold/val/  gold/test/  gold/test_random/   # model-ready, shuffled, fixed file count
└── gold/_metadata/                # feature_spec.json, scaling_stats.json, vocab/<name>/,
                                   # vocab_sizes.json, run_stats/<stage>.json
```

---

## 7. Data model

KuaiRand is downloaded from [Zenodo](https://zenodo.org/records/10439422) (CC BY-SA 4.0) by `scripts/download_data.sh`. The columns below were confirmed against KuaiRand-Pure; `pipeline/schemas.py` will be the source of truth.

| File | Rows (Pure) | Content |
|---|---|---|
| `log_standard_4_08_to_4_21_pure.csv`, `log_standard_4_22_to_5_08_pure.csv` | 1.14M + 0.30M | Impressions served by the recommender. **Training source.** |
| `log_random_4_22_to_5_08_pure.csv` | 1.19M | Videos inserted at random (`is_rand = 1`). Different exposure distribution, kept as an optional unbiased evaluation set. |
| `user_features_pure.csv` | 27,285 | One row per user |
| `video_features_basic_pure.csv` | 7,583 | One row per video |
| `video_features_statistic_pure.csv` | 7,583 | One row per video, monthly averages. **Not used** (see below). |

Facts observed in Pure:
- Standard logs cover **2022-04-09 to 2022-05-08** (30 days). Volume is front-loaded: April 10–12 are the busiest days.
- `is_click` positive rate is about **46%**, much higher than typical ad CTR, because an impression here is a video already shown in the feed. In the random log it is about 18%.
- **Time:** `time_ms` is the precise event time. `date` and `hourmin` are coarse: `hourmin` is always a whole hour, and the pair is logged up to about ±50 minutes away from `time_ms`, which puts 0.8% of rows on a different day. The pipeline therefore derives `event_date` and `event_hour` from `time_ms` (Beijing time) and uses only those.
- **Shared timestamps:** 6% of events share an exact `time_ms` with other videos from the same user, most likely because one feed request serves several videos. History features must treat these as simultaneous (see [stage 3](#stage-3-feature-engineering-pipelinefeaturespy)).

Full profile: `docker exec spark-master spark-submit /opt/project/scripts/profile_raw.py`.

### Interaction log (main fact table)

| Column | Type | Meaning |
|---|---|---|
| `user_id` | int | User identifier |
| `video_id` | int | Video identifier |
| `date` | int (YYYYMMDD) | Coarse logged date (kept, not used) |
| `hourmin` | int (HH00) | Coarse logged hour (kept, not used) |
| `time_ms` | long | Event timestamp in milliseconds: **the source of truth for time** |
| `event_ts`, `event_date`, `event_hour` | timestamp, int, int | Derived from `time_ms` in Beijing time during ingest. `event_date` is the partition column. |
| `is_click` | int (0/1) | User clicked the video (**primary label**) |
| `is_like`, `is_follow`, `is_comment`, `is_forward`, `is_hate` | int (0/1) | Other feedback signals |
| `long_view` | int (0/1) | User watched a long portion (**alternative label**) |
| `play_time_ms` | long | Time spent watching |
| `duration_ms` | long | Video length. Null in silver when unknown (recorded as 0 for 237 videos). |
| `profile_stay_time`, `comment_stay_time` | long | Time spent on the author profile / comments |
| `is_profile_enter` | int (0/1) | User opened the author's profile |
| `tab` | int | Which app tab the event happened on |
| `is_rand` | int (0/1) | Whether the video was shown randomly (not by the recommender) |

### Dimension tables

| Table | Grain | Example columns |
|---|---|---|
| User features | one row per user | `user_active_degree` (string), `is_live_streamer`, `is_video_author`, follow/fans/friend counts and range buckets (quoted strings like `"[100,1k)"`), `register_days`, `onehot_feat0..17` |
| Video basic features | one row per video | `author_id`, `video_type`, `upload_dt` (`YYYY-MM-DD`), `upload_type`, `video_duration`, `music_id`, `tag` (numbers stored as floats) |
| Video statistics | one row per video | ~50 show/play/like/comment/share counts, each an **average of daily values over the whole month** |

**Why video statistics are not used:** each value averages the full month, including the val and test days, so using it as a feature would leak future information. Item statistics are instead computed from the interaction logs per (`video_id`, `date`), using previous days only. That self-built table is the skewed large-to-large join in stage 4.

### Output (gold) schema

One row per interaction, 74 columns. `gold/_metadata/feature_spec.json` lists every column by role, so training code reads the spec instead of hard-coding names:

```
label              : int         # is_click (or long_view, set by `label` in config)
user_id, video_id, time_ms       # event key, kept for traceability; not features
37 categorical     : int         # user_idx, video_idx, author_idx, music_idx, tab_idx,
                                 # hour_of_day, day_of_week, video/user categorical *_idx
22 numeric         : float       # log1p where heavy-tailed, standardized with train stats
9 <col>_missing    : int         # 1 where the numeric had no information yet
hist_video_idx     : array<int>  # last 20 viewed videos, left-padded with 0
event_date         : int         # for analysis; not a feature
```

Every vocabulary uses the same index layout: `0` = padding, `1` = unknown (rare, unseen, or null), `2..` = known values, most frequent first.

---

## 8. How: pipeline stages

Each stage is a standalone PySpark script that reads one layer and writes the next. `scripts/run_pipeline.sh` runs them in order with `spark-submit`.

### Stage 1. Ingest: `pipeline/ingest.py`

**Input:** `hdfs:///raw/` · **Output:** `hdfs:///bronze/`

- Upload raw CSVs to HDFS with `scripts/load_to_hdfs.sh` (`hdfs dfs -put`).
- Read with an **explicit schema** from `schemas.py` in `FAILFAST` mode, so a malformed row stops the job instead of silently becoming nulls. `inferSchema` needs an extra full pass over the data and guesses wrongly here: several ID columns are written as floats (`9.0`).
- Cast types and derive `event_ts`, `event_date` and `event_hour` from `time_ms` in Beijing time.
- Standard and random logs go into **one interactions table**, told apart by `is_rand`. They never share an event, and the bot check needs each user's full activity.
- Write Parquet **partitioned by `event_date`**, so later stages that need a date range only read those directories (partition pruning). Each date is one file, sorted by user and time.
- Ingest adds and removes no rows: a quality check compares bronze and raw row counts.
- Record row counts in `gold/_metadata/run_stats/ingest.json`.

### Stage 2. Clean: `pipeline/clean.py`

**Input:** `bronze` · **Output:** `silver`

The rules were set after profiling the raw data (`scripts/profile_raw.py`). Counts are for KuaiRand-Pure, standard and random logs together (2,622,668 rows).

| Rule | Evidence | Effect in Pure |
|---|---|---|
| Drop rows with null `user_id`, `video_id` or `time_ms` | None found; kept as a guard | 0 removed |
| Collapse duplicate events (same `user_id`, `video_id`, `time_ms`) into one row, taking the **max** of each outcome column | 1.6% of standard-log events are logged 2–6 times. Copies differ mostly in the coarse hour; about 20 later copies carry more engagement (e.g. a click), so the max keeps the most complete record | 23,483 removed |
| Drop rows with negative `play_time_ms` or `duration_ms` | None found; kept as a guard | 0 removed |
| Treat `duration_ms = 0` as **unknown (null)** and keep the row | 237 videos have no recorded length anywhere (log or video table), but their impressions are real: 82% have play time, 23% are clicked. Dropping them would remove 2% of data and bias against those videos | 65,779 changed |
| Cap `play_time_ms` at 5× `duration_ms` | Watch-ratio p99.9 is 4.94, max 68 (looping videos) | 1,923 changed |
| Drop **bot-like user-days**: ≥ 20 events with a median gap under 2 s, or more than 16 h of play | The originally planned rule (users above the 99.9th percentile of daily events) would have removed 5.4% of rows. The heaviest user-days (up to 237 events) are fast scrollers with 15 s+ median gaps and under 1 h of play, not bots | 0 removed |
| Dimension tables: missing strings → `UNKNOWN`, missing integer codes → `-1`, `tag` → `tags` (array of ints) | Nulls in 7 `onehot_feat` columns, `music_type`, `tag`; `tag` holds lists like `"12,60"` | No rows removed |

Every rule logs how many rows it removed or changed (`gold/_metadata/run_stats/clean.json`), so over-aggressive cleaning is visible. In total, cleaning removed **0.90%** of interactions.

`watch_ratio` is not stored in silver. It is a derived feature, computed in stage 3; storing it would add about 8 bytes of incompressible doubles per row (+33% silver size in Pure).

### Stage 3. Feature engineering: `pipeline/features.py`

**Input:** `silver` · **Output:** `features/`

Feature tables are built separately and joined in stage 4. The full list is in the [feature catalog](#9-feature-catalog).

| Output | Grain | Content |
|---|---|---|
| `features/events` | one row per event | Labels, user history, context and user × author features. Partitioned by `event_date` |
| `features/item_daily`, `features/author_daily` | (video or author, date) | Impressions, smoothed click rate and watch ratio over **previous days** |
| `features/user_static`, `features/video_static` | one row per user / video | IDs and categorical columns encoded to indices, plus raw numerics |
| `gold/_metadata/vocab/<name>/`, `vocab_sizes.json` | — | The encodings and embedding-table sizes |

Key techniques:
- **Window functions** for per-user history, with frames that end *strictly before* the current event's timestamp:
  ```python
  w = (Window.partitionBy("user_id")
             .orderBy("time_ms")
             .rangeBetween(Window.unboundedPreceding, -1))
  df = df.withColumn("user_hist_ctr", F.avg("is_click").over(w))
  ```
  A row-based frame (`rowsBetween(..., -1)`) would leak: 6% of events share a timestamp with other videos from the same feed request, and a row frame lets one of them see its siblings' clicks. A range frame on `time_ms` excludes everything at the same moment.
- **One shuffle for every user window.** The events are repartitioned by `user_id` once. The history, session (`user_id`, session) and user × author (`user_id`, `author_id`) windows all reuse that partitioning, because data clustered by `user_id` is also clustered by any key that includes it. The stage records the number of `user_id` shuffles in its physical plan (`run_stats/features.json`): **1**.
- **History list without an unbounded window.** `collect_list` over an unbounded frame would build a list as long as the user's whole history for every event. Instead the window looks back N + 10 rows, drops events at the current timestamp, and keeps the last N. Same-timestamp groups are at most 6 events in Pure, so 10 rows of slack is enough. The history is the last N *viewed* (shown) videos, not only clicked ones.
- **Sessions:** a new session starts after a 30-minute gap. Events at the same timestamp share a session and a position.
- **Point-in-time item and author statistics:** cumulative stats over days before the event date, never the same or later days. Click rates are smoothed toward the global click rate of *previous days*: `(clicks + 20 × prior) / (impressions + 20)`. A brand-new video gets exactly the prior instead of a noisy 0/0 or 1/1.
- **Categorical encoding:**
  - **ID features** (user, video, author, music) and `tab`: vocabularies are built from **training-period events only**. Values seen fewer than `min_count` (5) times there map to the shared unknown index. In Pure, 11,458 val/test events come from users never seen in training, and they get the unknown index, as a cold-start user would in production.
  - **Static categorical columns** (user activity level, follower buckets, the 18 anonymized `onehot_feat` columns, video type, primary tag): vocabularies are built from the dimension table itself. Rare values (`min_count`) map to unknown.
  - All vocabularies are written to HDFS and read back before use. An early version built each one from the table as encoded so far, which nested every previous join into the next query plan. The plan doubled with each of the 22 user columns, and the driver ran out of memory.
- **Numeric scaling moves to stage 4.** `log1p` and standardization need statistics from the *joined training rows*, and those only exist after the stage 4 join. Stage 3 therefore stores raw numerics.

**Sanity check: single-feature AUC on the standard log.** If any single feature predicts the click almost perfectly, that is a strong sign of leakage. None does:

| Feature | AUC alone | Null % |
|---|---|---|
| `user_hist_ctr` | 0.696 | 2.0 (first events) |
| `user_hist_avg_watch_ratio` | 0.679 | 2.0 |
| `video_ctr_prev` | 0.652 | 3.8 (first day of data) |
| `author_ctr_prev` | 0.651 | 3.8 |
| `video_avg_watch_ratio_prev` | 0.595 | 19.0 (each video's first day; unknown durations) |
| Others | 0.44–0.55 | 0 |

The nulls are real "no information yet" cases. Stage 4 fills them and adds missing-value indicators.

### Stage 4. Aggregate and join: `pipeline/build_dataset.py`

**Input:** `features/` · **Output:** a joined DataFrame (in memory)

- Start from `features/events` (one row per interaction, labels included).
- **Broadcast joins** with `user_static` and `video_static`: both are small enough to copy to every executor, so the event table is not shuffled.
- Join `item_daily` on (`video_id`, `event_date`) and `author_daily` on (`author_id`, `event_date`). How the `item_daily` join runs is configurable (`join.item_daily_strategy`): **broadcast** (default), **salted**, or plain **sortmerge**. All three give identical rows, both in unit tests and on the full Pure data (same checksum). Why broadcast is the default is in [skew handling](#skew-handling).
- Checks: no fan-out (2,599,185 rows in and out), unique event keys, and every event matched in all four tables.
- **Fill nulls and scale numerics** (moved here from stage 3, because the statistics need the joined training rows):
  - Statistics come from **training rows only** (standard log, training dates) and are saved to `gold/_metadata/scaling_stats.json`.
  - 13 heavy-tailed counts and durations get `log1p` (clipped at 0 first, so a negative count can never become −∞). Then every one of the 22 numerics is standardized: `(x − train mean) / train std`.
  - Nulls mean "no information yet" (first event, first day, unknown duration). They become the training mean, i.e. exactly 0 after scaling, and each of the 9 nullable columns gets a `<col>_missing` flag, so the model can tell "average" from "unknown".
  - Mean and std are rounded to 9 decimals, so last-bit noise in float sums cannot change the output between runs.
- Result: one row per interaction with all features and the label.

### Stage 5. Split and write gold: `pipeline/build_dataset.py`

**Output:** `hdfs:///gold/{train,val,test,test_random}/` and `gold/_metadata/feature_spec.json`

| Split | Source | Dates | Rows (Pure) | Click rate | Files |
|---|---|---|---|---|---|
| `train` | standard log | Apr 9 – Apr 29 | 1,262,136 | 46.6% | 8 |
| `val` | standard log | Apr 30 – May 3 | 78,444 | 46.2% | 2 |
| `test` | standard log | May 4 – May 8 | 72,556 | 44.5% | 2 |
| `test_random` | random log | May 4 – May 8 | 527,007 | 18.2% | 2 |

- **Time-based split.** A random split would put future events in training and past events in test, which inflates offline metrics and does not reflect how the model is used in production.
- **`test_random`** holds videos the app showed at random, not ones the recommender picked. Evaluating on it shows how well the model ranks *any* video, not just the ones the old recommender already favoured. Random-log rows on train and val dates (659,042) are not used, so training stays on the recommender's own distribution.
- **Shuffled, fixed-size files.** Rows are ordered by a seeded hash of the event key, so training batches are not time-ordered and every run writes the same bytes. Each split gets a fixed number of files (`gold.files`), so training readers are not slowed by many tiny files and DataLoader workers get even shares. (A first version reduced the hash to 8 values before repartitioning. Spark hashed those 8 values again, collisions left half the files empty, and train got 4 files instead of 8. A file-count check and a unit test now guard against this.)
- **`feature_spec.json`** lists every categorical column with its embedding size, the numerics, the flags, the history sequence, the label, and each split's dates and row count.

### Stage 6. Data-quality gates: `pipeline/quality_checks.py`

Called at the end of every stage. See [data-quality checks](#10-data-quality-checks). A failed check stops the pipeline with a clear message.

### Stage 7. Training: `training/`

See [model training](#11-model-training).

---

## 9. Feature catalog

| Group | Feature | Definition | Technique |
|---|---|---|---|
| **User** | `user_hist_cnt` | Number of the user's earlier events | Window count, range frame ending before this timestamp |
| | `user_hist_ctr`, `user_hist_like_rate` | Click and like rate over earlier events (null if none) | Window sums / count |
| | `user_hist_avg_watch_ratio` | Mean of `play_time / duration` over earlier events with a known duration | Window avg |
| | `hist_video_idx`, `hist_len` | Last 20 viewed videos (encoded, left-padded) and how many are real | `collect_list` over a bounded row frame, same-timestamp events filtered out |
| | `user_active_days` | Distinct active days before today | `dense_rank` of date − 1 |
| | user static features | Activity level, follower / fan / friend counts and buckets, registration age, 18 anonymized categoricals | `features/user_static`, joined in stage 4 |
| **Item** | `video_impr_prev`, `video_ctr_prev` | Video impressions and smoothed click rate over previous days | Daily aggregate + cumulative window ending at yesterday |
| | `video_avg_watch_ratio_prev` | Mean watch ratio over previous days | Same |
| | `author_ctr_prev` (+ impressions, watch ratio) | The same for the video's author | Same, by `author_id` |
| | `video_age_days` | Event date − upload date | Date arithmetic |
| | video static features | Video, author and music IDs, video type, upload type, music type, primary tag, duration | `features/video_static`, joined in stage 4 |
| **Context** | `hour_of_day`, `day_of_week` | From the event time / date (Beijing time) | Column expressions |
| | `secs_since_last_event` | Gap since the user's previous *timestamp* (siblings ignored) | Window max over the range frame |
| | `session_position` | Position in the current session (new session after a 30 min gap) | Running sum of session starts, then a window per session |
| | `tab_idx` | App tab | Encoded raw column |
| **Cross** | `user_author_prev_impr`, `user_author_prev_clicks` | How often this user saw / clicked this author before | Window partitioned by user and author |

## 10. Data-quality checks

| Check | Applied after | Rule (starting value) | On failure |
|---|---|---|---|
| Schema match | every stage | Column names and types equal the expected schema | Fail |
| Non-empty output | every stage | Row count > 0 | Fail |
| Row-count equality | ingest | Bronze rows = raw CSV rows (ingest only changes types) | Fail |
| Row-count drift | clean | Rows removed ≤ 10% of input | Fail |
| Primary-key uniqueness | clean | No duplicate (`user_id`, `video_id`, `time_ms`); dimension keys unique | Fail |
| No nulls in keys | clean | IDs, `time_ms`, `event_ts`, `event_date`, `play_time_ms` | Fail |
| Value ranges | clean | Watch ratio in [0, 5]; `is_click`, `long_view`, `is_rand` in {0, 1} | Fail |
| Join fan-out | features | Event features have exactly one row per silver event (the video join adds none) | Fail |
| Leakage: same-timestamp events | features | Events of one user at one timestamp have identical history features | Fail |
| Leakage: first events | features | Every user's first event has no history; every video's / author's first day has no previous-day stats | Fail |
| Value ranges | features | Rates within [0, 1]; history length within [0, N] and every list exactly N long; ages, gaps, positions positive | Fail |
| Static tables | features | One row per silver user / video; no null indices | Fail |
| Join fan-out and matches | build_dataset | Rows after all joins = event rows; event keys unique; every event matched in user, video, item-daily and author-daily tables | Fail |
| Scaling | build_dataset | No nulls or NaN/infinity in numerics; on training rows every scaled column has mean 0 and std ≤ 1 | Fail |
| Row accounting | gold | train + val + test = all standard-log rows; test_random = all random-log test rows | Fail |
| Split integrity | gold | max(train date) < min(val date), max(val date) < min(test date) | Fail |
| No nulls, same schema | gold | Every split, every column | Fail |
| Indices fit vocabularies | gold | Every categorical and history index < its embedding size | Fail |
| File count | gold | Each split has its configured number of files | Fail |
| Label balance | gold | Positive rate within the expected band (separate band for the random log) | Warn |

The join fan-out check catches a common and silent bug: a dimension table with duplicate keys multiplies rows during a join.

---

## 11. Model training

### Data loading

- `training/dataset.py` implements a PyTorch `IterableDataset` that reads gold Parquet files with PyArrow and yields whole batches (categorical indices, scaled numerics plus missing flags, the history sequence, the label). Column roles come from `gold/_metadata/feature_spec.json`, so no feature names are hard-coded.
- Files are sharded across `DataLoader` workers so each worker reads different files; tests check that every row is read exactly once with 0, 2, or more workers than files. With `shuffle=True`, file order and rows within each file are reshuffled every epoch, deterministically from (seed, epoch).
- Reading is fast enough that workers are not needed: about 2.4M rows/s in-process, so one training epoch (1.26M rows) is read in 0.5 s. On macOS each worker is a new process that re-imports PyTorch (3–5 s), so training defaults to `num_workers=0`.
- The gold data is copied from HDFS to local disk before training (`hdfs dfs -get`), which keeps the training code simple. Reading directly from HDFS through PyArrow is a stretch goal.

### Models

| Model | Purpose | Details |
|---|---|---|
| Logistic regression | Baseline | One-hot / embedding-sum of categorical features + numeric features |
| **DeepFM** | Main model | Shared embeddings feed two parts: a **factorization machine** that learns pairwise feature interactions, and an **MLP** that learns higher-order interactions. Outputs are summed and passed through a sigmoid. |

User history (`user_last_n_videos`) is included by mean-pooling the history video embeddings. Attention pooling (as in DIN) is a stretch goal.

### Training setup

- Loss: binary cross-entropy.
- Optimizer: Adam; embedding size, MLP layers and learning rate tuned on the validation split.
- Early stopping on validation AUC.
- Final metrics reported once on the test split.

### Metrics

- **AUC**: how well the model ranks clicked items above non-clicked ones. The main metric for ranking.
- **Log loss**: how well-calibrated the predicted probabilities are.

---

## 12. Engineering decisions

These are the parts of the project that go beyond "it runs." Each one will be backed by Spark UI evidence or a benchmark.

### Partitioning
- **On disk:** partitioned by `date`, so time-range reads skip whole directories.
- **In memory:** repartitioned by `user_id` before user-level window aggregations. Each user's events land in one partition and several window functions share a single shuffle.
- **Output files:** controlled file count per partition to avoid the "small files problem," where HDFS NameNode memory and read overhead grow with the number of files.

### Skew handling
Viral videos create hot keys in the (`video_id`, `event_date`) join. Measured in Pure before choosing a strategy:

- **Keys are very skewed:** the busiest (video, date) has 3,430 events against a median of 9 (about 380×), and the top 5 keys hold 0.6% of all events.
- **Partitions are not:** a 24-way shuffle by that key puts thousands of keys in each partition, and the busiest partition holds only 1.08× the median.
- **`item_daily` is 3 MB**, so a broadcast join needs no shuffle at all.

So the default is **broadcast**. Pure timings for the stage: broadcast 12 s, sort-merge 14 s, salted 18 s (salting copies `item_daily` 8× for no benefit here). Salting is kept, with an equivalence test, for when both sides are too big to broadcast, as with KuaiRand-27K.

| Approach | How it works |
|---|---|
| Naive join | Plain sort-merge join with AQE disabled. Expect one or a few very slow tasks when partitions are skewed. |
| AQE skew join | Spark detects oversized partitions at runtime and splits them. |
| Manual salting | Each event gets a salt `0..S-1` from a hash of its key (not `rand()`, so every run puts each row in the same bucket). The other side is replicated `S` times, once per salt value, and the join is on (key, salt). A hot key's load is spread over `S` partitions. |

For the phase 6 benchmark to show a real effect, it needs partitions that are actually skewed: broadcast disabled, plus a larger dataset (1K or a slice of 27K) or more shuffle partitions so that hot keys dominate their partition. We record the stage time and the max/median task duration from the Spark UI.

### Broadcast joins
Small tables (user and video features, `item_daily` in Pure, vocabularies, per-date global click rates) are broadcast to every executor with `F.broadcast(...)`, so the large event table is not shuffled for those joins.

### Caching
DataFrames reused by several feature builders (the cleaned interaction table) are `persist()`-ed and `unpersist()`-ed when no longer needed, to avoid recomputing them from HDFS.

### Leakage prevention
- Window frames end strictly before the current event's timestamp (`rangeBetween`), so events from the same feed request never see each other. A unit test checks this, and it fails if the frame is switched to `rowsBetween` (verified by making that change).
- Item statistics use previous days only.
- Vocabularies and scaling statistics come from the training period only.
- Train/val/test split is by time.

### Reproducibility
- All paths, thresholds and seeds live in `config/pipeline.yaml`.
- **Same input, same bytes out.** Rebuilding silver and features from scratch gives identical values in every column (checked with per-column checksums across 3 rebuilds). This needed one fix: after a shuffle, Spark does not fix the order of rows that tie in a window's sort key (same-timestamp events), and floating-point addition depends on order. Averages of watch ratios therefore differed in the last bits between rebuilds. They are now summed as exact decimals and converted to doubles only at the end. Integer sums and counts were already exact. Vocabulary ties are broken by value, and history lists are ordered by (`time_ms`, `video_id`), so both are deterministic too.
- The gold dataset is also identical across runs (checksums of every split). Scaling statistics are rounded to 9 decimals, and the training shuffle is a seeded hash, not `rand()`.
- Every stage writes its stats (row counts, timings, and a 12-character hash of the full config) to `gold/_metadata/run_stats/<stage>.json`, so any output can be traced to the settings that produced it.

---

## 13. Benchmark methodology

| Benchmark | What varies | What stays fixed | Measured |
|---|---|---|---|
| **Scaling** | 1, 2, 3 workers | Data, config, cores per worker | End-to-end runtime and per-stage runtime |
| **Skew** | Naive vs. AQE vs. salting | Everything else | Join stage time, max and median task time |
| **Spark vs. pandas** | Engine; data size (10%, 25%, 50%, 100%) | Same transformations | Runtime and peak memory; where pandas fails |
| **File format** | CSV vs. Parquet input | Same query | Scan time and bytes read |

Rules:
- Each configuration runs **3 times**, and we report the median.
- The first run after cluster start is discarded (warm-up).
- Results are saved as CSVs in `benchmarks/results/` and plotted with matplotlib.
- Because everything runs on one laptop, scaling results show the effect of parallelism, not true multi-machine network cost. This limitation is stated alongside the results.

---

## 14. When: roadmap

| Phase | Scope | Est. time | Deliverable | Done when |
|---|---|---|---|---|
| **0. Setup** | Repo, README, config file, dataset download script | 0.5 day | This README, `scripts/download_data.sh` | Data downloaded locally |
| **1. Cluster** | Docker Compose with HDFS (namenode + 2 datanodes), Spark (master + 3 workers) and History Server | 1 day | `docker-compose.yml`, `docker/` | A test job reads a file from HDFS and all UIs load |
| **2. Ingest and clean** | Explicit schemas, bronze and silver layers, quality checks | 1 day | `schemas.py`, `ingest.py`, `clean.py`, `quality_checks.py` | Silver data written and all checks pass |
| **3. Features** | User, item, context and cross features; encoding; scaling | 1.5 days | `features.py`, unit tests | Feature tables written; leakage tests pass |
| **4. Build dataset** | Joins, broadcast, salting, time split, gold layer | 1 day | `build_dataset.py` | Gold train/val/test written; split integrity check passes |
| **5. Training** | Parquet → PyTorch loader, LR baseline, DeepFM | 1 day | `training/` | DeepFM beats the LR baseline on validation AUC |
| **6. Benchmarks and polish** | Scaling, skew, pandas and format benchmarks; plots; Spark UI screenshots | 1 day | `benchmarks/`, final README | Results section filled in |

**Total: about 7 days.** Each phase ends in a working, runnable state.

**Stretch goals:**
- Run on YARN instead of standalone mode.
- Orchestrate the stages with Airflow.
- Use Delta Lake for the silver and gold layers.
- Read training data directly from HDFS in PyTorch.
- Attention-based user history (DIN).
- Deploy to a cloud cluster (AWS EMR or GCP Dataproc) and compare cost and runtime.

---

## 15. Risks and mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Laptop runs out of memory with 6+ containers | Cluster crashes | Small container limits (section 6); develop on KuaiRand-Pure / 1K; only benchmark on the full data |
| Full dataset too large for local disk | Cannot load into HDFS | Use a subset of days; document the subset size |
| Dataset columns differ from what is listed here | Schema errors | Confirm columns in phase 0; `schemas.py` is the source of truth |
| Hadoop/Spark container version mismatch | HDFS reads fail | Pin image versions; verify with a test job in phase 1 |
| Scaling benchmark flat on one machine | Weak result | Report honestly; explain CPU-bound vs. I/O-bound behavior; optional cloud run |
| Leakage bug inflates AUC | Misleading results | Leakage unit tests, split integrity check, and sanity check that AUC is realistic |

---

## 16. Repository layout

```
spark-recsys-pipeline/
├── docker-compose.yml        # HDFS + Spark cluster
├── docker/                   # Dockerfiles and Hadoop/Spark config files
├── config/
│   └── pipeline.yaml         # paths, thresholds, label, seeds
├── scripts/
│   ├── download_data.sh      # fetch KuaiRand / KuaiRec
│   ├── load_to_hdfs.sh       # hdfs dfs -put raw files
│   ├── smoke_test.py         # phase 1 cluster check
│   ├── profile_raw.py        # data profile behind the cleaning rules
│   ├── run_pipeline.sh       # spark-submit all stages in order
│   └── run_tests.sh          # pytest inside the spark-master container
├── pipeline/
│   ├── common.py             # config, Spark session, run stats
│   ├── encoding.py           # vocabularies: build and apply (0 = PAD, 1 = UNK)
│   ├── schemas.py            # explicit Spark schemas
│   ├── ingest.py             # raw → bronze
│   ├── clean.py              # bronze → silver
│   ├── features.py           # silver → features
│   ├── build_dataset.py      # features → gold (join + split)
│   └── quality_checks.py     # reusable checks
├── training/
│   ├── dataset.py            # Parquet → PyTorch IterableDataset
│   ├── models.py             # DeepFM, LR baseline
│   └── train_deepfm.py       # training loop and evaluation
├── benchmarks/
│   ├── pandas_baseline.py
│   ├── scaling.py
│   ├── skew.py
│   └── results/              # CSVs and plots
├── tests/                    # pytest unit tests for transformations
└── README.md
```

---

## 17. Running locally

**Requirements:** Docker (OrbStack or Docker Desktop) with about 11 GB of memory allocated, [uv](https://docs.astral.sh/uv/) for the training environment, and about 20 GB of free disk. The pipeline and its tests run inside the cluster, so they need no local Python setup.

Steps 1–4 (the whole data pipeline, raw CSV to gold) and 6 work today. In step 5, the environment setup and gold export work; the training command is _(planned)_.

```bash
# 1. Start the cluster (first run builds the images, ~3 min)
docker compose up -d

# 2. Download the data and load it into HDFS
./scripts/download_data.sh pure
./scripts/load_to_hdfs.sh pure

# 3. Check that Spark can read and write HDFS on all workers
docker exec spark-master spark-submit /opt/project/scripts/smoke_test.py

# 4. Run the pipeline: ingest, clean, features, build_dataset (~2.5 min on Pure),
#    or chosen stages: ./scripts/run_pipeline.sh build_dataset
./scripts/run_pipeline.sh

# 5. Train (on the host)
#    One-time setup: a Python 3.10 virtual environment with pinned packages.
#    uv downloads Python 3.10 for this project only; system Pythons are untouched.
uv venv --python 3.10 .venv
uv pip install --python .venv -r requirements-train.txt
#    Copy gold from HDFS to data/gold/ (checks row counts against feature_spec.json)
./scripts/export_gold.sh
#    (planned) train the models
.venv/bin/python training/train_deepfm.py --config config/pipeline.yaml

# 6. Run tests: pipeline tests inside the spark-master container,
#    training tests in the host venv
./scripts/run_tests.sh
.venv/bin/python -m pytest training/tests
```

Web UIs:

| UI | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| HDFS DataNodes | http://localhost:9864, http://localhost:9865 |
| Spark master | http://localhost:8080 |
| Spark workers | http://localhost:8081 – 8083 |
| Spark application (while a job runs) | http://localhost:4040 |
| Spark History Server | http://localhost:18080 |

---

## 18. Results _(planned)_

To be filled in after phase 6:

- Pipeline runtime with 1, 2 and 3 workers (plot)
- Skew handling: naive join vs. AQE vs. salting (stage times, task-time distribution)
- Spark vs. pandas baseline across data sizes
- CSV vs. Parquet scan performance
- Model metrics: DeepFM vs. logistic regression (AUC, log loss)
- Spark UI screenshots of key stages
