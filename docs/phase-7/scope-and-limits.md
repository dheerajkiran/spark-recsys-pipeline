# Checkpoint 7.1 scope, limits, and regression inventory

Date: November 10, 2026. The deliverable is a frozen, locally checkable product/data/API contract and reproducible synthetic fixture. A working customer service, web dashboard, feature pipeline, trained platform model, authorization backend, and AWS deployment are later checkpoints.

## Supported journey

An owner signs in, creates a project, declares and uploads supported CSV files, finalizes the upload, observes validation, starts a bounded asynchronous feature/training job, inspects evaluation, registers a compatible version, explicitly deploys it, requests recommendations, records actual exposures/interactions, and can roll back. A viewer can inspect project state and request recommendations. A developer key has only catalog/event/recommendation scopes. A separate demo session exercises the shared recommendation path.

Initial API schemas and example messages describe that journey. They do not imply that the endpoints already exist. Structured errors, real job states, idempotency, project authorization, and version/strategy identity are requirements for their implementation.

## Fixed limits and provenance

| Dimension | Contract / recording bound |
|---|---|
| HTTP contract | `/v1`, OpenAPI 3.1.0; strict inputs and safe structured errors |
| Input schema | `recommendation-v1`, UTF-8 CSV; items/events required, users optional |
| Upload | 50 MiB total CSV bytes; at most 100,000 rows per CSV file |
| API JSON | 256 KiB; batches up to 100 items/events |
| Pagination | Default 20, maximum 100; opaque bounded cursor, maximum 2,048 characters |
| Ranking | At most 200 candidates and 20 returned items |
| Fixture | Two project datasets; each 100 synthetic users, eight distinct real clips, 10,000 synthetic impressions plus linked events |
| Label policy | `click-v1`; 30-minute attribution, 24-hour lateness, mature labels only |
| HTTP idempotency | Seven days of retained original operation/resource/payload identity |
| Event deduplication | At least 30 days of project/event-ID identity and canonical payload digest |
| Model/training | At most 10 MiB model; staged CSV-derived input bounded to 50 MiB; CPU only; later rehearsal must establish runtime fit |
| Recording workloads | One active expensive job; one normal Glue/training workflow, at most 1,000 rankings and 5,000 other API calls |
| Session | Target four hours from resource creation to stop/teardown start; follow removal to completion |
| AWS budget | User maximum $100 of reported credits, comfortably below exhaustion; provisional conservative allowance $20, refreshed before deployment |
| Evidence | Small sanitized reports/manifests in Git; raw datasets, model weights, full logs, state, and credentials excluded |

Synthetic fixture outcomes, genuine later browser events, and historical KuaiRand results have distinct provenance. Unknown users, insufficient classes, and incompatible models have a documented baseline/unavailable path; undefined AUC stays unavailable. No fabricated CTR negatives or claims of immediate click-triggered retraining.

## Existing interfaces to preserve

| Existing area | Entry points / verification | Compatibility obligation |
|---|---|---|
| HDFS/Spark research | `scripts/run_pipeline.sh`, `scripts/run_tests.sh`, `pipeline/`, `config/pipeline*.yaml` | Preserve KuaiRand schemas, output meaning, splits, preprocessing, and reproducibility |
| Research training | `training/train_deepfm.py`, `training/models.py`, `training/dataset.py`; `pytest training/tests` | Existing feature-spec/checkpoint/result contracts remain intact |
| Historical inference | `serving/preview.py`; `pytest serving/tests` | Preserve frozen artifact checks, projected historical reads, and source disclosure |
| Playable catalog | `catalog/import_commons.py`, `catalog/schema.sql`; `pytest catalog/tests` | Preserve metadata schema, attribution, bounded import, and local catalog identity |
| Local feed | `frontend/server.mjs`, `frontend/package.json`; `npm test`, `npm run check` in frontend | Preserve dependency-free preview, playback, pagination, and historical panel |
| Benchmarks | `benchmarks/`, committed results/plots, `benchmarks/check_readme.py` | Preserve measured workload/results and their research scope |

This checkpoint adds separate files and a separate Python environment; it does not extract or change these implementations. Run targeted contract checks and a lightweight existing preview regression. Full Spark/training/benchmark reruns become necessary when later checkpoints change the corresponding behavior, rather than being evidence that new contracts implement those systems.

## Deferred work

No arbitrary schemas, uploaded executable models, real customer onboarding, payments, GPU/large-scale training, streaming retraining, public training endpoints, ongoing hosting, or production SLA. DynamoDB/IAM enforcement, atomic cloud publication, browser instrumentation, feature parity, and measured runtime feasibility remain explicit exit criteria of their later checkpoints.
