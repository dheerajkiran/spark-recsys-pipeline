# Phase 7 — data, recommendation, and model contracts

**Status: proposed implementation specification.** Updated November 2, 2026. This is a student portfolio build: rehearse locally, perform one short AWS recording with a target of less than **$10 total**, export evidence, and remove every cloud resource. The spending target is not an enforceable billing cap. The contracts below describe work for Phase 7; they are not features already provided by the repository. Use the [phase overview](README.md), [AWS architecture](architecture.md), [frontend plan](frontend-and-demo.md), and [checkpoint plan](checkpoints.md) together.

## 1. Preserve the research result; build a separate product data path

The existing [Spark schemas](../../pipeline/schemas.py) describe KuaiRand, including integer user/video IDs and source-specific attributes. [Encoding](../../pipeline/encoding.py) already reserves `0 = PAD` and `1 = UNK`; [dataset construction](../../pipeline/build_dataset.py) fits scaling statistics on training data. These are useful foundations.

The current [inference preview](../../serving/preview.py) deliberately scores three recorded profiles using historical training rows. Its item IDs do not identify the playable Commons catalog. Production work must therefore add a versioned platform schema, source adapter, feature builder, and ranker. Keep the existing KuaiRand configuration, frozen results, and historical preview reproducible; do not relabel those results as performance on a customer catalog.

The first supported product is item ranking from recorded exposures and interactions. Customers upload data, not executable training code or arbitrary model files. Start with the existing DeepFM family plus a deterministic baseline, and keep the platform input schema independent of the model's internal tensors. Customer workflows are demonstrated with a small controlled project; this deployment is not an always-running commercial service.

### Recording fixture and limits

Create a seeded, clearly labelled synthetic fixture: approximately 100 users, 100 playable catalog item IDs, and 10,000 mature exposures with linked click/watch events. Record the generator seed, rules, timestamps, and file digests. Its IDs must match the demo catalog, rather than borrowing KuaiRand IDs. Synthetic relevance patterns can demonstrate real feature construction and inference; their measured metrics are not evidence of real-world personalization quality or customer impact.

Use historical fixture timestamps so label windows have matured; do not wait a day during the recording or disable label checks. Bound the complete uploaded dataset to 50 MiB and the model artifact to 10 MiB, with measured local rehearsal enforcing those limits. Permit at most one tiny AWS Spark/training workflow during the recording. Full KuaiRand training, cloud benchmarks, hyperparameter searches, repeated retraining, and automatic schedules remain outside this recording budget.

## 2. Canonical dataset contract: `recommendation-v1`

A dataset upload contains UTF-8 CSV files with headers: required `items.csv` and `events.csv`, plus optional `users.csv`. A versioned JSON manifest declares the schema, file sizes, checksums, timestamp range, and label policy. Publish downloadable examples and the equivalent OpenAPI request schemas before implementing customer uploads.

| File | Required fields | Optional, explicitly supported fields |
|---|---|---|
| `items.csv` | `item_id`, `published_at`, `active` | `creator_id`, `topic`, `duration_ms` |
| `events.csv` | `event_id`, `event_type`, `user_id`, `item_id`, `occurred_at`, `impression_id` | `request_id`, `session_id`, `watch_ms` |
| `users.csv` | `user_id` | `created_at` |

Rules for the first contract:

- IDs are nonempty strings of at most 128 characters. Preserve leading zeros and case; never infer numeric IDs from CSV content. External IDs are unique only within a project.
- `event_type` is one of `impression`, `click`, or `watch`. Every click/watch references a real impression for the same project, user, and item. An impression uses its own stable `impression_id`; every event has a distinct `event_id`.
- Timestamps are RFC 3339 values with an explicit offset, normalized to UTC. Reject naive dates. Persist both `occurred_at` and server-assigned `received_at` for online events.
- `active` is an explicit boolean. `duration_ms` and `watch_ms` are nonnegative integers when present. Apply documented maximum lengths, row counts, file sizes, and timestamp tolerances at validation.
- Reject conflicting duplicate IDs, unknown item references, malformed values, and impossible event ordering. Identical repeated events are deduplicated. Return row numbers and safe field-level errors; do not silently discard a large fraction of an upload.
- Project/organization ownership comes from the authenticated principal and stored upload session. A client-provided project ID never grants access to another project.
- User identity is pseudonymous. Email addresses and names are unnecessary model inputs. Mutable item metadata needs validity timestamps or versioned snapshots; current metadata cannot silently be joined into old training examples.

Imported rows may lack a historical `received_at`; label this limitation in the dataset report. Such data supports offline experiments but cannot prove replay fidelity to what an online system knew at an earlier time. Do not invent receipt timestamps that imply that guarantee.

### Online events use the same meaning

An example event body, with project ownership resolved from the authenticated route and credential:

```json
{
  "event_id": "evt_click_0042",
  "event_type": "click",
  "user_id": "viewer_17",
  "item_id": "clip_12",
  "occurred_at": "2026-11-02T19:05:12Z",
  "impression_id": "imp_0041",
  "request_id": "rec_0039"
}
```

The API validates and durably accepts bounded batches before returning an acceptance ID. Delivery and worker retries must be idempotent using `(project_id, event_id)`; this provides one accepted logical event despite repeated delivery. Do not claim exactly-once network delivery. Track accepted, duplicate, rejected, and late events separately.

A recommendation response is not an impression. The demo records an impression only after the card meets the agreed visibility rule, proposed as at least 50% visible for one continuous second. Repeated callbacks reuse the same impression ID. Instrument a click only for an exposed item. Automated imports must describe how their source measured exposure.

## 3. Click labels, maturity, and eligibility

The proposed first objective is **a click after an observed impression**. Autoplay, watch time, and likes are not interchangeable with a click. A later watch-based objective requires its own target definition and dataset/model version.

Proposed initial settings, to be confirmed at the contract checkpoint:

| Policy | Starting value | Meaning |
|---|---|---|
| Click attribution window | 30 minutes | A linked click must occur after the impression and within this interval |
| Accepted event lateness | 24 hours | Delay allowance before an exposure's label becomes final |
| Learned-model eligibility | 1,000 mature impressions, with at least 20 positive and 20 negative labels in each split | A demo eligibility floor; it does not establish adequate statistical power |

For a mature impression, `label = 1` if a valid linked click exists in the attribution window; otherwise `label = 0`. An exposure is mature only when its attribution window and lateness allowance have elapsed relative to the extraction watermark. Keep unresolved exposures out of supervised training. Never create negative examples from catalog items that were not exposed.

Preserve late events with a flag and counts. A published dataset is immutable; a changed lateness policy or corrected source creates a new version. Customers with only clicks, no exposure records, or insufficient label support receive an explanation and the baseline path rather than fabricated CTR training data.

Freeze chronological train/validation/test cutoffs in the dataset configuration before selecting models. Purge exposures whose attribution interval crosses a split boundary. Fit vocabularies, imputation values, and scalers on the training period only. Use validation for model/epoch selection and the predeclared promotion criteria; it must not contribute fitted preprocessing statistics. Keep the final test set outside tuning and promotion decisions; evaluate it once after the choice is frozen. A split with one label class has no valid ROC-AUC and must show `not available`, not a substituted score.

## 4. Stable feature identity and point-in-time correctness

Model artifacts contain the exact feature order, dtypes, transformations, vocabularies, unknown-value handling, and history length used during training. Map external string IDs into project-local indices using that model version's encoder. Preserve PAD/UNK semantics from the existing implementation. Unknown users/items use the defined fallback or UNK path; never reuse another project's embedding indices.

Build the first online feature set from what is actually available in both training and serving: bounded user history, prior exposure/click counts, prior item popularity, supported item metadata, and request-time context. Add richer features only after both paths implement them. Missing KuaiRand-only attributes need a new feature specification and retraining, not arbitrary zero-filled inputs to the old checkpoint.

Every recommendation request captures one context timestamp and one feature snapshot/watermark for all candidates. Histories and aggregates include only earlier events available by that request; exclude the impression being predicted and its future outcomes. Order history by `(occurred_at, event_id)` with a documented rule for equal timestamps. Bound history length and apply a versioned late-event policy. A model with previous-day popularity features must use previous-day features online as well.

Use a shared pure transformation specification where practical. Golden parity fixtures must compare offline Spark results with online feature vectors for the same project, timestamp, candidate set, encoders, and snapshot. Define tolerances for floating-point values and require exact categorical/history indices. Passing a forward-model test alone does not establish feature parity.

## 5. Recommendation request and ranking contract

The first API accepts a project-scoped user, a bounded result limit, and a small allowlist of context fields. Clients cannot submit arbitrary SQL filters, artifact paths, or internal feature tensors.

```json
{
  "user_id": "viewer_17",
  "limit": 10,
  "context": {"surface": "home"}
}
```

The implementation should:

1. Resolve project identity and pin its active deployment/version for the entire request.
2. Retrieve a bounded set of available project items; apply availability, publication-time, surface, and recent-exposure rules before scoring.
3. Construct features from one request context; score the candidate batch on CPU; use stable item-ID ordering for tied scores.
4. Apply a documented diversity rule if enabled, then return up to the requested number of items. Fewer results are valid when the catalog is exhausted.
5. Record request/model/feature versions and timing without recording raw feature vectors or credentials in application logs.

Propose a candidate ceiling of 200 and an output ceiling of 20 for the initial API; validate these against measured latency before release. Do not score an unbounded catalog on every request. A response should identify `request_id`, `strategy`, `model_version` when applicable, `feature_version`, ranked item IDs, and a safe fallback reason.

For sparse projects, unknown users, model unavailability, or insufficient compatible features, serve a deterministic project-local popularity/topic baseline. A baseline has its own version and returns `strategy: baseline`; it never produces invented DeepFM probabilities. New items need a bounded exposure policy rather than disappearing permanently because they have no engagement history. All modes enforce the same project and availability filters.

## 6. Immutable datasets and AWS migration

S3 replaces HDFS as durable production storage. Keep logical raw/bronze/silver/gold layers, scoped by organization, project, dataset, and run identifiers. Use generated internal identifiers in keys, not unsanitized customer filenames.

```text
projects/{project_id}/datasets/{dataset_id}/raw/
projects/{project_id}/runs/{run_id}/bronze|silver|gold/
projects/{project_id}/runs/{run_id}/manifest.json
projects/{project_id}/models/{model_version}/
```

A manifest records source object versions and SHA-256 digests, schema/label versions, extraction watermark, split cutoffs, row/rejection counts, feature and encoder digests, configuration hash, code commit, container digest, runtime versions, and output file inventory. Checksums are explicit; do not treat an S3 ETag as a universal content SHA-256.

Write each attempt into a unique run prefix. Publish a completed manifest only after all quality gates succeed, then transactionally mark that run ready in the database. Readers consume only an explicitly ready run and its inventory. Do not port directory rename, shared overwrite, or local temporary-folder assumptions to S3. Conditional writes can prevent a manifest key from being overwritten; handle concurrent-write failures explicitly. [AWS conditional-write documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)

Package pipeline modules/configs for EMR Serverless and replace Docker-exec launch scripts with the workflow driver. Select and pin an EMR release after testing Spark/Python compatibility; the current local Spark version does not automatically imply a matching EMR runtime. [AWS Spark job compatibility](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/jobs-spark.html)

The current [training loader](../../training/dataset.py) uses local `Path` and PyArrow files. A dedicated CPU training Lambda should stage only its validated gold manifest/shards into encrypted ephemeral storage, check capacity/checksums, train, upload artifacts, and exit. Rehearse the exact trusted Python container locally. Initially cap the training function at 600 seconds and 4 GiB memory, with 3–5 epochs at most; choose the final epoch count from rehearsal before recording. These are proposed experiment settings, not a claim that this workload already fits. Lambda's ordinary function execution limit is 900 seconds; leaving time below that boundary does not guarantee completion. [AWS Lambda quotas](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html)

Set the EMR Serverless application to no preinitialized capacity, a maximum of two vCPUs/four GiB across a one-vCPU/two-GiB driver and matching executor, and a 15-minute job timeout. Confirm Spark overhead fits this shape in rehearsal/configuration review. Local success does not establish AWS runtime compatibility; if the cloud gate fails, capture the failure and stop rather than repeatedly increasing resources. Larger datasets or training beyond 600 seconds require a later Fargate/SageMaker design and a separately reviewed budget.

Make work directories, model locations, and Python executables configurable; remove production dependence on a developer's `.venv`, checkout, and `docker cp`. Keep the recording backend serverless: API Gateway HTTP API, Node Lambda, Python ranker/training Lambda functions, S3, DynamoDB on-demand, and Step Functions. Do not provision continuously running RDS, ECS, or GPU resources for this phase's recording.

## 7. Registry, evaluation, promotion, and rollback

| Entity | Key relationship and required state |
|---|---|
| Dataset version | Project ownership, immutable manifest digest, validation status |
| Training job | Dataset/version, idempotency key, configuration, execution attempt, stage/status |
| Model version | Training job, artifact/feature/encoder digests, metrics, validation decision |
| Deployment | Project, desired version, ready serving version, rollout status, previous version |

Implement these records in DynamoDB on-demand with project-scoped keys such as `PK = PROJECT#{project_id}` and typed sort keys. Validate referenced project/dataset/job/model ownership in application code; DynamoDB does not provide relational foreign-key enforcement. Persist event deduplication records and idempotency payload digests for the application's configured retention period. A repeated submission with the same idempotency key and payload returns the same job; reuse with a different payload returns a conflict. A worker restart reconciles a recorded attempt rather than launching uncontrolled duplicate jobs. Failed/cancelled jobs do not publish ready datasets or change active models.

Use conditional writes for unique event/job claims and compare-and-swap deployment-pointer updates. Use transactions only where multiple related records must commit together; the built-in transaction client token's ten-minute window is not a replacement for application-retained idempotency records. Read the active deployment pointer consistently from its base-table key, then read versioned immutable feature/model records; an eventually consistent index is not the authority for an immediate promotion. [DynamoDB transaction and idempotency semantics](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html)

Only platform-produced, trusted artifacts are eligible for loading. Validate project ownership, sizes, checksums, expected tensor dimensions, finite values, feature/encoder compatibility, and supported runtime before constructing the model. Retain explicit CPU loading and `weights_only=True`; this reduces supported unpickling behavior but does not make arbitrary uploaded checkpoints safe. [PyTorch serialization guidance](https://docs.pytorch.org/docs/stable/notes/serialization)

Compare DeepFM with the same project's deterministic baseline and LR on the same frozen data contract. Report sample sizes, date ranges, class balance, AUC where defined, log loss, and cold-start coverage. Compute ranking metrics only when complete, documented candidate/exposure groups support the metric; report missing groups and selection bias. Offline improvement does not establish online engagement lift or calibrated probabilities.

Promotion requires valid artifacts, compatibility/parity tests, validation criteria fixed before training, and serving smoke checks. Deployments move through requested, warming, ready, active, failed, or rolled-back states. Validate and initially load the exact version before switching the project pointer. Each API request pins that deployment version and passes it to the internal ranker; every ranker invocation verifies its returned version. A warm Lambda environment may cache a verified model, but new environments must load/validate the pinned artifact independently. One successful warm-up does not make every future invocation warm. Retain the previous healthy version and test rollback without retraining. Automatic promotion on every completed job is outside the first release.

For the single-cloud-training recording, seed a trusted compatible version A produced by the local platform trainer, with provenance explicitly marked local. The one AWS training run produces version B. Validate and promote B, then roll back to A; this demonstrates version control without implying two cloud training runs. Both versions must pass the same artifact and serving checks.

Record true cold and warm inference timings separately. The HTTP API integration timeout is 30 seconds, so initialization and scoring must complete inside the API's end-to-end deadline or return the documented baseline/unavailable response. Do not bypass this limitation with fabricated scores or claim an always-warm service. If the trusted model cannot fit that boundary, the synchronous learned-ranking release gate fails; reduce the supported artifact/runtime or revise the later architecture before claiming readiness. [API Gateway HTTP API quotas](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-quotas.html)

## 8. Evidence required before this contract is considered implemented

The [checkpoints](checkpoints.md) must attach reproducible evidence for:

- Conflicting duplicates, out-of-order events, attribution boundaries, immature labels, late arrivals, and single-class splits.
- Cross-project item IDs, uploads, jobs, artifacts, and recommendation requests being rejected or isolated, including attempts using another project's valid identifier.
- Offline/online feature parity and exact independent predictions, with frozen holdout access audited.
- Missing/corrupt artifacts, insufficient disk, worker retry, cancellation, failed quality gates, and no partial publication.
- Candidate limits, timeouts, fallback behavior, unseen users/items, inactive items, and catalog exhaustion under representative load.
- Concurrent promotion, version-labelled responses during rollout, replica failure, and a demonstrated rollback.

Keep these reports alongside dataset/model manifests and deployment evidence. Export the recording, manifest digests, observed cost/resource inventory, and teardown verification before deleting the AWS environment and artifacts. Run no later cloud jobs. Recruiters can inspect the saved recording and local reproduction path, while technical reviewers can reproduce the documented contracts and limits.
