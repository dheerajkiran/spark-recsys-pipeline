# Phase 7 implementation checkpoints

**Status:** All implementation checkpoints below are planned. The phase documentation exists; the application and AWS work described here has not been performed.

Read the [phase brief](README.md), [architecture](architecture.md), [data/ML contracts](data-and-ml-contracts.md), [frontend plan](frontend-and-demo.md), and [budget runbook](budget-and-recording.md) together. The user-approved constraint is **less than $10 total AWS spend**, one recording session, then teardown. Checkpoints 7.1–7.10 are local work; infrastructure documents and plans do not authorize an unattended cloud deployment.

## How to use a checkpoint

Use the sequence below, with parallel work only after its dependencies are stable. Each checkpoint produces a runnable increment, a small reviewable change or series of changes, updated documentation, and evidence of its exit criteria. Record the commit, commands, results, limitations, and next dependency in its completion note. Mark work complete only after the behavior has been observed.

The named folders and commands below are proposed deliverables. They are not commands available in the current repository unless already linked to existing code. Do not claim that a future test script or endpoint has run.

| Checkpoint | Deliverable | Main dependency | Current state |
|---|---|---|---|
| 7.1 | Product/data/API contracts and bounded demo fixture | Phase brief | Planned |
| 7.2 | Local platform foundation and CI | 7.1 | Planned |
| 7.3 | Identity, project isolation, and API credentials | 7.2 | Planned |
| 7.4 | Upload validation, ingestion, and durable job records | 7.3 | Planned |
| 7.5 | Canonical Spark feature build and S3 manifests | 7.4 | Planned |
| 7.6 | Bounded training, evaluation, and model registry | 7.5 | Planned |
| 7.7 | Recommendation API and controlled version deployment | 7.6 | Planned |
| 7.8 | React customer dashboard and developer experience | 7.3–7.7 | Planned |
| 7.9 | Shared-API demo and interaction feedback | 7.7–7.8 | Planned |
| 7.10 | Disposable infrastructure, delivery, and full rehearsal | 7.1–7.9 | Planned |
| 7.11 | Short AWS verification and recording | 7.10 and cost gate | Planned |
| 7.12 | Teardown, final costs, and portfolio evidence | 7.11 | Planned |

## 7.1 — Freeze scope and observable contracts

**Outcome:** Everyone can identify the supported customer journey and distinguish it from the historical research preview.

Build:

1. Record architecture decisions for React/TypeScript, serverless AWS, DynamoDB access patterns, and a small CPU-only model. Keep a separate section for future continuously operated production requirements.
2. Define project ownership/roles, resource identifiers, API errors, pagination, request IDs, idempotency rules, and an initial versioned OpenAPI contract. Dashboard identity, developer keys, and public-demo routes have different permissions.
3. Freeze the canonical item/event schemas and the exposure-to-click label policy in [data-and-ml-contracts.md](data-and-ml-contracts.md). Initial proposals are a 30-minute attribution window and 24-hour accepted-lateness window; make their exact meaning executable and versioned before building features.
4. Create a deterministic fixture generator, a tiny playable catalog with compatible item IDs, and two project datasets. A candidate fixture is about 100 users, 100 items, and 10,000 mature impressions; generated files are reproducible from a seed rather than silently committed under the current `*.csv` ignore rule.
5. Record budgets for data size, requests, job count, execution time, and evidence retention. Inventory the existing tests and interfaces that must keep working.

Verify:

- Example requests and responses validate against the contract; malformed identifiers, unknown fields, and invalid event relationships have defined outcomes.
- Every training fixture split has mature labels and sufficient classes; every demo recommendation references an actual catalog item.
- Synthetic interactions, historic KuaiRand results, and real browser interactions have distinct provenance labels.

**Artifacts:** `contracts/`, a fixture generator, ADRs, a scope/decision record, and the initial access-pattern table.

**Exit:** The API/data contract and demo limits are documented with no ambiguous promise of arbitrary datasets, live retraining, or proven engagement lift.

## 7.2 — Create the local platform foundation

**Outcome:** A fresh clone can build and run the platform locally without an AWS account.

Build:

1. Introduce the proposed web/API/ranker/job directories incrementally. Extract reusable model and transformation code without breaking the original entry points.
2. Implement transport-independent API/domain modules, a local HTTP adapter, configuration validation, and a Lambda adapter using the same request/response contract.
3. Package a trusted CPU Python image with explicit dependency versions. Configure work directories and artifact paths instead of assuming the developer's `.venv`, home directory, or gold export exists.
4. Define local storage adapters and a documented development environment, for example DynamoDB Local and a local/S3-compatible artifact adapter. Use a local workflow runner for the same job transition rules; label differences from actual AWS service semantics.
5. Add TypeScript/lint/build checks, relevant Python checks, contract checks, and secret/state ignore rules to CI. Build images locally for the recording deployment; do not add paid CI cloud builds as a prerequisite.

Verify:

- Local startup fails with a clear configuration error when required configuration is absent.
- Existing pipeline/training/preview checks still pass for behavior touched by the extraction.
- Adapter contract tests exercise validation, serialization, structured errors, and configuration boundaries; mocks do not stand in for the final AWS proof.

**Artifacts:** Local startup instructions, dependency lock files, container definitions, CI workflow, and configuration examples with no real credentials.

**Exit:** The shared platform foundation runs locally and is reproducible without existing ignored data/checkpoints.

## 7.3 — Enforce identity and project isolation

**Outcome:** The system knows who is calling and which project they may access.

Build:

1. Define the Cognito issuer/audience contract and validate JWT signature, issuer, audience, expiry, and scopes at the appropriate boundary. Local test tokens are accepted only in an explicitly local configuration.
2. Implement project creation and membership. Derive identity from verified credentials and check authorization for every dataset, job, item, model, and deployment operation. A project ID supplied in a URL is never sufficient authority.
3. Add application-managed developer keys with random high-entropy secrets, hashed persistence, scoped permissions, expiry/revocation, one-time display, and sanitized audit records. API Gateway HTTP API does not supply the REST API usage-plan mechanism; implement the needed application checks.
4. Establish DynamoDB keys and query patterns that always include the authenticated project scope; use conditional writes for ownership and state changes.
5. Implement owner-controlled account setup for the recording session. Disable public sign-up and anonymous training; the public demo receives only its deliberately narrow routes.

Verify:

- Two projects with colliding external user/item IDs cannot read or modify one another's resources.
- Expired/wrong-audience tokens and revoked/wrong-scope API keys fail; credentials do not appear in logs or persisted browser state.
- A caller cannot select an arbitrary S3 key, workflow ARN, model artifact, or project by modifying a request body.

**Artifacts:** Identity/authorization modules, access tests, the permission matrix, and a short credential lifecycle guide.

**Exit:** Negative authorization tests cover every resource family before upload or job APIs are exposed.

## 7.4 — Implement datasets, events, and durable jobs

**Outcome:** A customer can supply a bounded dataset and receive a real validation/job result.

Build:

1. Add dataset creation, scoped presigned upload, explicit finalization, schema validation, content checksums, and human-readable error reports. Upload completion does not mean the dataset is valid.
2. Validate object ownership, actual size, allowed format, expected files, foreign item references, event IDs, timestamps, and duplicate policy on the server. Accept data only under the authorized project/dataset prefix.
3. Implement item/event APIs with stable request and impression IDs. A returned recommendation is not automatically an exposure; a viewport/playback rule establishes an impression. Match click events to actual exposed items.
4. Persist job state and a durable job-start outbox atomically using DynamoDB transactions. A stream consumer starts the workflow idempotently using a stable execution name; retries reuse the same job rather than starting new compute.
5. Add explicit accepted/running/succeeded/failed/cancelled states, stored failure reasons, retry counters, lease/attempt handling, and a reconciliation path for a missed or duplicated stream delivery. Implement bounded quotas and polling intervals.

Verify:

- Incomplete/oversized/malformed uploads cannot advance into Spark or training.
- Duplicate finalize/start/event requests, a consumer crash, and out-of-order callbacks do not create duplicate expensive work or double-count events.
- Dashboard polling reads durable state, and a worker failure reaches a useful terminal state without revealing another project's paths.

**Artifacts:** Upload/event endpoints, validation report schema, state-transition tests, outbox/reconciler logic, and local workflow evidence.

**Exit:** A valid dataset becomes immutable/ready, invalid data stays rejected, and starting a job has a durable identity before compute begins.

## 7.5 — Port the platform Spark path to S3-compatible storage

**Outcome:** One small canonical project dataset produces compatible, reproducible gold features.

Build:

1. Add a platform data adapter and feature spec. Existing KuaiRand-specific schemas, date ranges, and metadata cannot simply be reused for arbitrary customer catalogs.
2. Retain chronological splits, train-only encoders/scalers, bounded prior history, and mature exposure labels. Define event-time/arrival-time policy and apply the same policy to training and serving.
3. Add a packaged Spark entry point with explicit project/dataset/run/config arguments. The recorded cloud workflow must execute its platform stages in one bounded EMR job, instead of assuming `docker exec namenode` or HDFS exists on EMR Serverless. Package Python modules and dependencies in advance; a non-VPC EMR worker must not depend on fetching arbitrary external package indexes at runtime. If one job cannot support the stages, revisit the workflow and budget before deployment.
4. Write to immutable run prefixes and publish a completion manifest only after all expected files, row counts, feature metadata, and digests pass checks. S3 prefix rename is not an atomic commit protocol.
5. Prepare a small EMR release/configuration profile: a proposed one-driver/one-executor 2-vCPU/4-GB total ceiling, no preinitialized workers, a 15-minute job timeout, and explicit auto-stop. Validate usable Spark heap plus overhead against actual worker limits.

Verify:

- Local fixture outputs pass schema, row-count, split, leakage, repeatability, and label-maturity checks.
- A partially written prefix is never selectable as a ready dataset; retries cannot overwrite a published version.
- Original research paths remain runnable, and package/dependency/Python/Spark version compatibility is recorded for the chosen EMR release.

**Artifacts:** Platform Spark entry point, storage/config adapter, package manifest, fixture gold outputs, and a documented cloud resource envelope.

**Exit:** The local canonical dataset is correct, the S3 publish protocol is tested, and one future EMR invocation is fully specified. Cloud execution waits until 7.11.

## 7.6 — Package training, evaluation, and artifact registration

**Outcome:** A compatible model can be reproduced and inspected independently of the workstation that trained it.

Build:

1. Wrap the existing CPU trainer with platform-specific feature specs and allowlisted settings. Stage only the requested immutable dataset into bounded ephemeral storage.
2. Use a separate training Lambda handler in the trusted Python image. Proposed limits are a 50-MiB staged dataset, 10-MiB model, 3–5 epochs, 4-GiB memory if the account permits it, and a 600-second timeout; reduce fixture size/settings if rehearsal fails.
3. Compare a documented baseline and DeepFM using validation for selection. Keep held-out evaluation separate and frozen after selection. Represent undefined metrics or insufficient labels honestly.
4. Write a model bundle with weights, encoder mappings, scaler statistics, feature spec, training configuration, source dataset manifest, code/image identity, digest, and evaluation provenance.
5. Register immutable versions with compatible/failed/eligible metadata. Registration never automatically changes the active serving version. Decline uploaded executable model artifacts. Produce trusted version A locally for the later rollback demonstration and record its local provenance; the single AWS training run will produce version B.

Verify:

- Two runs with the same recorded seed/config/data produce equivalent outputs within declared numerical tolerances.
- The trainer rejects target leakage, mismatched input dimensions, absent split files, corrupt weights, and unsupported model settings.
- Local container rehearsal measures peak resource use and time. It must leave substantial room under the 600-second limit; a larger job requires a later design/budget change rather than an automatic infrastructure upgrade.

**Artifacts:** Training/validation handlers, model manifest schema, measured local resource report, evaluation report, and registry logic.

**Exit:** A tiny synthetic fixture yields a trusted, compatible model version; its metrics are labeled synthetic and kept separate from original KuaiRand research results.

## 7.7 — Serve recommendations and control model deployment

**Outcome:** The API returns a real ranked list for a specified authorized project and current request context.

Build:

1. Implement bounded candidate selection from the project's available catalog, shared-context feature assembly, historical exposure filters, and deterministic tie-breaking. A proposed session limit is 200 candidates and 20 returned items.
2. Serve baseline recommendations when a project/user/item has insufficient information. Return the actual strategy, deployment generation, model version when used, and request ID.
3. Build the Python Lambda ranker around exact-version artifact loading, digest checks, evaluation mode, bounded `/tmp` caching, and cache keys that include project/version identity. No request selects an arbitrary local path or S3 URL.
4. Add explicit promotion and rollback. Check compatibility, load/warm the candidate, then conditionally update the active deployment generation. Resolve a single generation at request start and pin it through scoring; never return stale-version scores labelled as the new version.
5. Return controlled loading/unavailable responses if the intended model cannot load. Keep the previous eligible version or a clearly labelled baseline according to the contract; do not silently invent scores.

Verify:

- Offline/online feature parity and direct model forward results agree for a known request fixture.
- Candidate lists exclude unavailable/foreign-project items; unknown IDs use the documented mapping/fallback.
- Promotion races, concurrent requests, corrupted artifacts, and rollback cannot create mixed-version responses.
- Local measurements distinguish warm inference from initialization. The cloud cold-start path must later fit within API Gateway HTTP API's 30-second integration limit; the target is not assumed satisfied.

**Artifacts:** Recommendation/deployment APIs, parity and routing tests, request-level provenance, and a local latency/resource report.

**Exit:** A developer can submit a real request, receive explainable version/strategy metadata, and observe an intentional rollback using the same API contract.

## 7.8 — Build the React dashboard and developer experience

**Outcome:** A customer can complete the supported journey without manual database edits or hidden terminal commands.

Build:

1. Implement the React/TypeScript application, preserving useful Loop visual identity while organizing shared components, layouts, routing, and a generated/typed API client.
2. Deliver sign-in, project overview, dataset upload/validation, job details, model/evaluation views, deployment/rollback, API credential management, and usage/status screens.
3. Connect actual upload progress, validation reports, persisted job stages, and model deployment results. Do not substitute timers or invented progress percentages for backend state.
4. Add copyable API examples, error explanations, one-time credential display, revocation feedback, and request IDs. Mask keys when recording and keep credentials out of URLs and persistent storage.
5. Implement responsive layouts, keyboard/focus behavior, accessible labels/contrast, and loading/empty/error/unauthorized/unavailable states. Clear cached server data when identity or project changes.

Verify:

- Browser tests against the actual local API cover the valid journey and representative invalid/denied/failed paths.
- A narrow mobile viewport, a desktop viewport, and a keyboard-only path complete the supported workflow.
- Screenshots show measured results and real resource states; the UI explains synthetic fixture provenance and cold-start/unavailable conditions plainly.

**Artifacts:** Application screens, component/design notes, browser checks, and reviewed screenshots.

**Exit:** The entire owner journey works locally; the customer does not need to know S3 keys, Lambda names, or container paths to use it.

## 7.9 — Connect the public demo and feedback path

**Outcome:** The public demonstration exercises the same recommendation engine as the developer API.

Build:

1. Seed the isolated demo project with the compatible item IDs and synthetic mature interactions from 7.1. Keep the old KuaiRand historical inference panel clearly separated or accessible as research evidence.
2. Replace topic-only feed ordering with the shared recommendation API. Display a concise current-strategy/model indicator and preserve useful playback/search/save interactions.
3. Emit stable, deduplicated exposure/click events only when the documented browser exposure condition is met. A later explicit training job may use them; a click does not immediately retrain the model.
4. Provide narrow server-side demo routes, expiry and request limits, and a session disable switch. Do not put a privileged customer API key in the bundle or browser.
5. Add a recruiter-oriented tour and recording path showing the product, API, and engineering evidence without overwhelming ordinary product screens with implementation details.

Verify:

- The feed and developer request use the same ranker, compatible items, active version, and strategy metadata.
- Repeated viewport events are not double-counted, and activity cannot escape the demo project.
- Cold users, unavailable media, empty catalogs, and inference failures produce usable, honest states.

**Artifacts:** Integrated demo, event-path evidence, attribution/source notes, and a locally rehearsed recording script.

**Exit:** All three experiences work locally as one platform, with no invented model personalization or unlabelled synthetic metrics.

## 7.10 — Prepare disposable AWS infrastructure and delivery

**Outcome:** The cloud session can be created, verified, recorded, and removed with a bounded, reviewed procedure.

Build:

1. Add Terraform for the chosen serverless environment: private S3/CloudFront UI, API Gateway HTTP API, Cognito, scoped Lambda functions, DynamoDB on-demand/Streams, ECR, Step Functions, bounded EMR Serverless, and limited logs. Use CloudFront pay-as-you-go without a flat-rate subscription so the distribution can be removed during teardown. Add a dead-letter queue only for defined failure handling.
2. Configure scoped IAM, GitHub OIDC trust limited to the deployment repository/environment/ref, and separate read/plan/apply/evidence/destroy capabilities. Avoid static AWS access keys in CI.
3. Create local image/build/package workflows and a manual cloud delivery workflow; ordinary pushes run checks without applying infrastructure or training models.
4. Add expiry, application quotas, workflow/job timeouts, reconciliation limits, no provisioned concurrency or initial EMR capacity, and short log retention. Implement an independently invokable session stop procedure; an optional timed stop is defense in depth, not a billing guarantee.
5. Write evidence export and resource inventory/teardown scripts. Verify state files, credentials, raw datasets, model weights, and unredacted logs remain ignored before generating them.
6. Fill the regional price/usage worksheet with current rates, deployment overhead, anticipated requests, taxes, and a reserve. Validate new-account quotas without assuming Free Tier credits.

Verify:

- Infrastructure format/validation and policy/config checks pass; the planned resource list contains no unexpected always-on services or recurring purchases.
- The local end-to-end rehearsal and recording fit the chosen fixture and time envelope. Package size, runtime architecture, and Python/Spark compatibility are explicit.
- Destroy ordering covers versioned S3 objects/delete markers, ECR images, CloudFront disable/delete propagation, jobs/executions, Cognito, logs, state storage, and partially created resources.

**Artifacts:** Terraform, CI workflow, a filled cost preflight, deployment/stop/destroy runbooks, and local rehearsal evidence.

**Exit:** A cleanly specified one-session deployment has an estimated usage cost of at most $5 with enough remaining room to target less than $10 total. If that estimate or any resource gate fails, reduce scope or stay local; do not launch hoping credits will cover it.

## 7.11 — Run the short AWS proof and recording

**Outcome:** Capture genuine deployed evidence of all three experiences within the approved workload envelope.

Build/run:

1. Choose the intended account/region, record the current price/usage worksheet, and confirm the one-session resource plan. Start the session timer when cloud resources are created, not when filming begins.
2. Deploy the verified images and infrastructure. Create only the test identities/projects and tiny dataset; keep expensive endpoints owner-controlled and public training disabled. Seed the trusted, locally trained version A with its declared provenance.
3. Verify HTTPS, authorization, upload validation, one bounded EMR feature build, one small training/evaluation run producing version B, exact-version inference, promotion to B and rollback to A, and the shared-API feed. The recording distinguishes the local seed from the new AWS-trained version.
4. Record a short product tour plus a deeper engineering walkthrough. Capture sanitized request/version IDs, job timing, feature/model manifests, cold/warm behavior, and representative failure evidence.
5. Use the session stop procedure when finished or when a gate fails. Target at most four hours from first creation to stop/teardown start; infrastructure deletion and residual-cost review must still be followed through to completion.

Verify:

- Record actual account quotas, service release/configs, runtime/image/code digests, job counts, and billed resource utilization where available.
- Distinguish a browser timeout from a failed background job and reconcile before any retry. No unbounded retry loop or second training sweep is allowed.
- If AWS behavior fails a release gate, preserve the failure, stop the session, and document the limitation. Do not present a local replay as a successful AWS execution.

**Artifacts:** Recordings, screenshots, sanitized cloud verification report, workflow/model evidence, and provisional usage cost calculation.

**Exit:** The cloud evidence is genuine and exported. Any unmet acceptance criterion remains explicitly incomplete; teardown proceeds regardless of success.

## 7.12 — Tear down and publish the evidence package

**Outcome:** The repository remains useful after the cloud environment is gone, with spend and residual resources accounted for.

Build/run:

1. Disable public ingress/demo mutation and new job starts, cancel/stop active executions and EMR jobs, and drain or disable background triggers. Verify no initialization capacity remains.
2. Download and verify the recordings, safe result summaries, configuration/manifests, and required logs before deleting their cloud copies. Revoke test API keys and discard test identities during teardown.
3. Destroy the environment, including object versions, images, distributions, log groups, alarms, queues, tables/backups, and any bootstrap state resources. Preserve state locally outside Git until cleanup has been verified, then follow the state-retention decision.
4. Inspect the intended account/region and any global resources; compare inventory against the deployment resource list. Retry failed removals deliberately and record the reason for any remaining resource.
5. Reconcile delayed billing/usage, including taxes and ancillary services, against the under-$10 target. Record final versus pending charges; a successful `destroy` command alone does not prove the final bill.
6. Finish the root README, architecture screenshots, recording link, short case study, relevant measured results, tradeoffs, and known limitations. Keep bulk video out of the source tree when a GitHub release/attachment or external recording link is more suitable; publication is a separate explicit step.

Verify:

- Small committed evidence contains no secrets, raw customer data, fake counters, or unsupported scale/availability claims.
- A reviewer can reproduce the local journey from a fresh clone and understand that the AWS deployment was temporary and is now removed.
- All planned cloud resources are removed or any specific exception is documented with its expected continuing cost and resolution.

**Artifacts:** Teardown inventory, cost closeout, portfolio README/case study, recording links, and final checkpoint record.

**Exit:** The phase can be called complete only when application acceptance criteria, evidence export, and teardown verification are finished. Do not mark the budget goal achieved until billing has settled sufficiently to support the stated cost.

## Cross-checkpoint release gates

| Gate | What must be demonstrated | Evidence location, to be created |
|---|---|---|
| Contracts | Real fixtures and client/server schemas agree | `contracts/` and contract-check output |
| Isolation | Cross-project requests and colliding IDs fail safely | Authorization tests and sanitized request trace |
| Durable work | Duplicate delivery, worker crash, timeout, and cancellation preserve correct state | Job/idempotency tests and workflow report |
| ML correctness | Mature labels, point-in-time features, feature parity, compatible exact-version scores | Dataset/model manifests and ML checks |
| Frontend | Real API journey, keyboard/mobile layouts, and honest state transitions | Browser report and screenshots |
| Cloud bounds | Supported quotas, small artifacts, bounded job resources, cold-start limit, reviewed costs | Session preflight and cloud report |
| Closure | Evidence verified locally and resources/billing inspected | Teardown inventory and cost closeout |

## Checkpoint completion record template

Copy this for each completed checkpoint; retain failures and follow-ups that affect the next step.

```text
Checkpoint:
Status: planned | in progress | complete
Commit / PR:
Outcome and changed behavior:
Acceptance criteria observed:
Commands and results:
Evidence paths:
Local / AWS / synthetic / historical data scope:
Known limitations and unresolved items:
Next checkpoint dependency:
Cloud resources and estimated/actual charges, if any:
```

No fixed completion dates are promised. Work is sequenced by verified outcomes so the AWS recording is the final small proof, rather than the environment in which development happens.
