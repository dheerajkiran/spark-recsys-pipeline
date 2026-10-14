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
- **Data locality**: Spark prefers to schedule a task on the node that already holds the block it reads, so data does not cross the network.

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
4. Executors read blocks directly from the DataNodes, preferring local blocks.
5. Narrow operations (filters, derived columns) run in place. A `groupBy user_id` triggers a shuffle, so rows with the same user end up in the same partition.
6. Results are written back to HDFS as Parquet, one file per output partition.

---

## 5. Tech stack: what, where, and why

| Layer | Technology | Where it is used | Why this choice |
|---|---|---|---|
| Storage | **HDFS** (Hadoop 3.3) | Holds the raw, bronze, silver and gold datasets | Distributed and fault-tolerant, and data locality lets Spark read blocks on the node where they are stored. It is the standard storage layer in the Hadoop ecosystem. |
| Compute | **Apache Spark 3.5** (PySpark) | Every pipeline stage in `pipeline/` | In-memory distributed processing, the Catalyst query optimizer, and the DataFrame API. AQE helps with skew and shuffle sizing. |
| File format | **Apache Parquet** (Snappy compression) | Every layer after raw | Columnar storage with compression, predicate pushdown and column pruning. It is much faster to scan than CSV and stores the schema with the data. |
| Cluster runtime | **Docker Compose** | `docker-compose.yml` | Runs the multi-node HDFS and Spark cluster on one laptop. Reproducible, and nothing to install beyond Docker. |
| Language | **Python 3.11** | Pipeline, training and tooling | One language from ETL through to model training. |
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

Planned starting values, sized to fit a laptop with 16 GB of RAM. They will be tuned in phase 1.

### Containers

| Service | Image base | Role | Resources | Ports |
|---|---|---|---|---|
| `namenode` | Apache Hadoop 3.3 | HDFS metadata | 1 GB | 9870 (UI), 9000 (RPC) |
| `datanode-1`, `datanode-2` | Apache Hadoop 3.3 | HDFS block storage | 1 GB each | 9864 (UI) |
| `spark-master` | Apache Spark 3.5 | Cluster manager | 1 GB | 8080 (UI), 7077 (RPC) |
| `spark-worker-1..3` | Apache Spark 3.5 | Executors | 2 cores, 2 GB each | 8081–8083 (UI) |
| `spark-history` | Apache Spark 3.5 | History Server for finished jobs | 512 MB | 18080 (UI) |

### Key Spark settings

| Setting | Value | Why |
|---|---|---|
| `spark.sql.adaptive.enabled` | `true` | Enable AQE (turned off for the "naive" skew benchmark) |
| `spark.sql.adaptive.skewJoin.enabled` | `true` | Let AQE split skewed join partitions |
| `spark.sql.shuffle.partitions` | `24` initially | Default of 200 is too many for 6 cores; AQE coalesces further |
| `spark.sql.autoBroadcastJoinThreshold` | `10MB` (default) | Small tables broadcast automatically; we also broadcast explicitly where needed |
| `spark.serializer` | `KryoSerializer` | Faster and more compact than Java serialization |
| `spark.eventLog.enabled` | `true` | Needed for the History Server and benchmark evidence |
| `dfs.replication` (HDFS) | `2` | Matches the number of DataNodes |

### HDFS directory layout

```
hdfs:///
├── raw/kuairand/                  # original CSVs
├── bronze/interactions/date=YYYYMMDD/
├── bronze/users/
├── bronze/videos/
├── silver/interactions/date=YYYYMMDD/
├── features/user/  features/item/  features/context/
├── gold/train/  gold/val/  gold/test/
└── gold/_metadata/                # vocab maps, split boundaries, run stats
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
- `is_click` positive rate is about **46%**, much higher than typical ad CTR, because an impression here is a video already shown in the feed.

### Interaction log (main fact table)

| Column | Type | Meaning |
|---|---|---|
| `user_id` | int | User identifier |
| `video_id` | int | Video identifier |
| `date` | int (YYYYMMDD) | Event date |
| `hourmin` | int (HHMM) | Event time of day |
| `time_ms` | long | Event timestamp in milliseconds |
| `is_click` | int (0/1) | User clicked the video (**primary label**) |
| `is_like`, `is_follow`, `is_comment`, `is_forward`, `is_hate` | int (0/1) | Other feedback signals |
| `long_view` | int (0/1) | User watched a long portion (**alternative label**) |
| `play_time_ms` | long | Time spent watching |
| `duration_ms` | long | Video length |
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

One row per interaction:

```
label           : int        # is_click (or long_view)
user_idx        : int        # encoded user ID
video_idx       : int        # encoded video ID
author_idx      : int
<categorical>_idx : int      # all other encoded categorical features
<numeric>       : float      # scaled numeric features
hist_video_idx  : array<int> # last N watched videos (padded)
date            : int        # kept for partitioning, dropped at training time
```

---

## 8. How: pipeline stages

Each stage is a standalone PySpark script that reads one layer and writes the next. `scripts/run_pipeline.sh` runs them in order with `spark-submit`.

### Stage 1. Ingest: `pipeline/ingest.py`

**Input:** `hdfs:///raw/` · **Output:** `hdfs:///bronze/`

- Upload raw CSVs to HDFS with `hdfs dfs -put`.
- Read with an **explicit schema** from `schemas.py`. `inferSchema` needs an extra full pass over the data and can guess types wrongly.
- Cast types and build a proper `event_ts` timestamp from `time_ms`.
- Write Parquet **partitioned by `date`**, so later stages that need a date range only read those directories (partition pruning).
- Record row counts in a run-stats file.

### Stage 2. Clean: `pipeline/clean.py`

**Input:** `bronze` · **Output:** `silver`

| Rule | Reason |
|---|---|
| Drop exact duplicate events (`user_id`, `video_id`, `time_ms`) | Logging retries produce duplicates that inflate counts |
| Drop rows with null IDs or timestamps | Cannot be joined or ordered |
| Drop rows with `play_time_ms < 0` or `duration_ms <= 0` | Physically impossible values |
| Cap `play_time_ms / duration_ms` at a maximum (e.g. 5×) | Looping videos create extreme outliers |
| Remove bot-like users (event rate above the 99.9th percentile per day) | Bots distort popularity and CTR features |
| Fill missing categorical values with an explicit `UNKNOWN` | Keeps rows and makes missingness a learnable signal |

Every rule logs how many rows it removed, so over-aggressive cleaning is visible.

### Stage 3. Feature engineering: `pipeline/features.py`

**Input:** `silver` · **Output:** `features/`

Three feature groups are built separately and joined in stage 4. The full list is in the [feature catalog](#9-feature-catalog).

Key techniques:
- **Window functions** for per-user history, with frames that end at the row *before* the current one:
  ```python
  w = (Window.partitionBy("user_id")
             .orderBy("time_ms")
             .rowsBetween(Window.unboundedPreceding, -1))
  df = df.withColumn("user_hist_ctr", F.avg("is_click").over(w))
  ```
- **Repartition by `user_id`** once before all user window functions, so they share one shuffle.
- **Point-in-time item statistics**: item popularity features use only days before the event date, never the same or later days.
- **Categorical encoding**: IDs are mapped to contiguous integers (`0..V-1`) for embedding tables. IDs seen fewer than `min_count` times in the training period map to a shared `<UNK>` index. Vocabularies are built **from the training period only** and saved to `gold/_metadata/`.
- **Numeric scaling**: `log1p` for heavy-tailed counts, then standardization using training-period statistics.

### Stage 4. Aggregate and join: `pipeline/build_dataset.py`

**Input:** `silver` + `features/` · **Output:** a joined DataFrame (in memory)

- Join interactions with user features on `user_id`.
- **Broadcast join** with video metadata: this table is small enough to copy to every executor, so the large interaction table is not shuffled.
- Join with daily video statistics, computed in stage 3 from the logs, on (`video_id`, `date`). This is large-to-large, and viral videos create hot keys, so it uses **salting** (see [engineering decisions](#12-engineering-decisions)).
- Result: one row per interaction with all features and the label.

### Stage 5. Split and write gold: `pipeline/build_dataset.py`

**Output:** `hdfs:///gold/{train,val,test}/`

- **Time-based split**, for example: first ~70% of days → train, next ~15% → val, last ~15% → test. The exact date boundaries are saved to `_metadata`.
- A random split would put future events in training and past events in test, which inflates offline metrics and does not reflect how the model is used in production.
- Write Parquet with a controlled number of files (`repartition` before write) so training readers are not slowed down by thousands of tiny files.

### Stage 6. Data-quality gates: `pipeline/quality_checks.py`

Called at the end of every stage. See [data-quality checks](#10-data-quality-checks). A failed check stops the pipeline with a clear message.

### Stage 7. Training: `training/`

See [model training](#11-model-training).

---

## 9. Feature catalog

| Group | Feature | Definition | Technique |
|---|---|---|---|
| **User** | `user_hist_ctr` | Click rate over the user's prior events | Window, unbounded preceding to -1 |
| | `user_hist_cnt` | Number of prior events | Window count |
| | `user_avg_watch_ratio` | Mean of `play_time / duration` over prior events | Window avg |
| | `user_like_rate` | Like rate over prior events | Window avg |
| | `user_last_n_videos` | Last N (e.g. 20) video indices, padded | `collect_list` over a bounded window |
| | `user_active_days` | Distinct active days before the event | Window over daily aggregates |
| | user static features | Activity level, registration age, follower buckets | Join with user table |
| **Item** | `video_ctr_prev_days` | Video click rate over previous days | Daily aggregate + lag |
| | `video_impr_prev_days` | Video impressions over previous days | Daily aggregate + lag |
| | `video_avg_watch_ratio` | Mean watch ratio over previous days | Daily aggregate + lag |
| | `video_age_days` | Event date minus upload date | Date arithmetic |
| | `author_ctr_prev_days` | Author-level click rate over previous days | Aggregate by author + lag |
| | video static features | Duration bucket, type, music ID, tag | Broadcast join |
| **Context** | `hour_of_day`, `day_of_week` | From event timestamp | Column expressions |
| | `secs_since_last_event` | Gap since the user's previous event | `lag` over user window |
| | `session_position` | Position in the current session (new session after 30 min gap) | Window + running sum |
| | `tab` | App tab | Raw column |
| **Cross** | `user_author_prev_clicks` | How often this user clicked this author before | Window partitioned by user and author |

---

## 10. Data-quality checks

| Check | Applied after | Rule (starting value) | On failure |
|---|---|---|---|
| Schema match | every stage | Column names and types equal the expected schema | Fail |
| Non-empty output | every stage | Row count > 0 | Fail |
| Row-count drift | clean | Rows removed ≤ 10% of input | Fail |
| Primary-key uniqueness | clean | No duplicate (`user_id`, `video_id`, `time_ms`) | Fail |
| Null rate | features, gold | Each feature ≤ 1% nulls after fill | Fail |
| Value ranges | features | Rates within [0, 1]; counts ≥ 0 | Fail |
| Label balance | gold | Positive rate within expected band for each split | Warn |
| Split integrity | gold | max(train date) < min(val date) < min(test date) | Fail |
| Join fan-out | aggregate | Row count after joins equals row count before | Fail |

The join fan-out check catches a common and silent bug: a dimension table with duplicate keys multiplies rows during a join.

---

## 11. Model training

### Data loading

- `training/dataset.py` implements a PyTorch `IterableDataset` that reads gold Parquet files with PyArrow in row-group batches.
- Files are sharded across `DataLoader` workers so each worker reads different files.
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
Viral videos create hot keys in the (`video_id`, `date`) join. We compare three approaches on the same data:

| Approach | How it works |
|---|---|
| Naive join | Plain sort-merge join with AQE disabled. Expect one or a few very slow tasks. |
| AQE skew join | Spark detects oversized partitions at runtime and splits them. |
| Manual salting | Add a random salt `0..S-1` to the hot side's key, replicate the other side `S` times with each salt value, and join on (key, salt). Load is spread over `S` partitions. |

For each, we record the stage time and the max/median task duration from the Spark UI.

### Broadcast joins
Small tables (video metadata, vocab maps) are broadcast to every executor with `F.broadcast(...)`, so the large interaction table is not shuffled for those joins.

### Caching
DataFrames reused by several feature builders (the cleaned interaction table) are `persist()`-ed and `unpersist()`-ed when no longer needed, to avoid recomputing them from HDFS.

### Leakage prevention
- Window frames end at the previous row.
- Item statistics use previous days only.
- Vocabularies and scaling statistics come from the training period only.
- Train/val/test split is by time.

### Reproducibility
- All paths, thresholds and seeds live in `config/pipeline.yaml`.
- Every run writes its stats (row counts, timings, config hash) to `gold/_metadata/`.

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
│   └── run_pipeline.sh       # spark-submit all stages in order
├── pipeline/
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

## 17. Running locally _(planned)_

**Requirements:** Docker Desktop with at least 10 GB of memory allocated, Python 3.11, and about 20 GB of free disk.

```bash
# 1. Start the cluster
docker compose up -d

# 2. Download the data and load it into HDFS
./scripts/download_data.sh
./scripts/load_to_hdfs.sh

# 3. Run the pipeline (all stages)
./scripts/run_pipeline.sh

# 4. Train
python training/train_deepfm.py --config config/pipeline.yaml

# 5. Run tests
pytest tests/
```

Web UIs:

| UI | URL |
|---|---|
| HDFS NameNode | http://localhost:9870 |
| Spark master | http://localhost:8080 |
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
