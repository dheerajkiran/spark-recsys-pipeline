# Phase 7 — Frontend, dashboard, and public demo

**Status: planned.** This document describes the frontend to build during Phase 7; it does not describe a deployed service. Use React + TypeScript + Vite for the new application while preserving the current Loop identity. The canonical implementation order and completion gates live in [checkpoints.md](checkpoints.md).

**Deployment constraint:** this is a student portfolio project with a target below **$10 total for one short AWS demonstration**, followed by teardown. Build and rehearse locally first; target at most four hours from resource creation to stop/teardown start, then follow resource removal through to verification. The enduring recruiter experience is the GitHub documentation, screenshots, recorded tour, and reproducible local application; an always-running public website is outside this phase's deployment commitment. The [budget and recording plan](budget-and-recording.md) governs any AWS run.

Read alongside the [phase brief](README.md), [AWS architecture](architecture.md), and [data and model contracts](data-and-ml-contracts.md).

## 1. Product goal

Provide three experiences backed by the same recommendation platform:

| Experience | Primary audience | Successful outcome |
|---|---|---|
| Public demo | Recruiters, hiring managers, engineers, and prospective customers | Understand the product quickly through the recorded AWS tour or local demo and inspect the evidence behind the recommendation flow. |
| Developer experience | An application developer | Create a scoped credential, submit supported data, request recommendations, and diagnose a failed request. |
| Customer dashboard | A project owner or operator | Upload a dataset, follow validation and training, review evaluation, deploy a model version, and observe its use. |

The demo is a dedicated project that exercises the shared API. It must not become a parallel implementation with hand-written recommendations or privileged browser credentials. The dashboard controls asynchronous jobs; the browser never runs Spark or training. All three experiences will be implemented and rehearsed locally, then demonstrated during the bounded AWS session. During that session, the frontend will be available through its temporary hosted URL; after teardown, documentation will identify it as a recorded deployment.

## 2. Starting point and migration

The current [Loop frontend](../../frontend/README.md) already provides a responsive feed, search, filters, pagination, media attribution, keyboard controls, and a historical model-preview panel. Preserve those interaction patterns and the warm paper, charcoal, and orange visual identity from [styles.css](../../frontend/styles.css). Extract reusable design tokens and improve low-contrast text as components move into the new application.

Important changes are functional:

- Browser-local likes, follows, and interests become explicit account or anonymous-session state where supported; the UI must accurately explain their persistence.
- SQLite-backed local catalog requests move behind the shared project-scoped API.
- The local Node/Python preview bridge is replaced in the production path by the deployed recommendation API and ranker.
- Model weights, training features, and dataset exports remain outside the frontend bundle.

Keep the existing local preview reproducible while the replacement is built. Port one complete journey at a time, compare it with the original, and retire local-server behavior only after its intended replacement passes verification. Avoid combining a framework migration with changes to the established research results.

## 3. Information architecture

Routes below are proposed navigation, not implemented endpoints. Freeze names with the API and frontend contracts before implementation.

| Area | Proposed routes | Required content |
|---|---|---|
| Public introduction | `/`, `/architecture` | Product purpose, actual capability status, recorded tour/local-run instructions, system diagram, repository, and measured results with methodology links. During the AWS session, the demo action can point to the temporary deployment. |
| Demo | `/demo` | Playable feed, session reset, ranking strategy/version, media attribution, and a short guided tour. |
| Developer guide | `/developers` | Supported schema, authentication, runnable request examples, pagination/errors, and links to versioned OpenAPI. |
| Account entry | `/login`, `/auth/callback` | Cognito sign-in, callback handling, and actionable sign-in failure states. |
| Project overview | `/app/projects`, `/app/projects/:projectId` | Project creation/selection, setup checklist, active deployment, recent jobs, and usage freshness. |
| Data | `…/datasets`, `…/datasets/:datasetId` | Sample downloads, uploads, schema version, validation summary, rejected-row examples, and dataset lineage. |
| Training | `…/jobs`, `…/jobs/:jobId` | Real stage states, timestamps, configuration, attempts, bounded logs, cancellation, and recovery actions. |
| Models | `…/models`, `…/models/:versionId` | Training lineage, validation results, baseline comparison, eligibility, and deployment history. |
| Deployment and access | `…/deployments`, `…/api-keys`, `…/usage`, `…/settings` | Active version, rollout/rollback status, scoped keys, request/error usage, project permissions, and retention settings. |

A recruiter should have a short path in GitHub: understand the purpose, watch the short tour, inspect the architecture, then follow a measured result into its source evidence. A technical reviewer can run the sample project locally and follow an end-to-end run. The recording also shows the deployed frontend, authenticated dashboard, and API calls during the AWS session. Public sample access never grants permission to start arbitrary training jobs.

## 4. Core journeys

### Create a project and upload data

1. Show the supported input contract and a small valid example before the upload control. The first release supports one documented schema; it does not promise arbitrary CSV understanding.
2. Request a server-created dataset/upload identity for the authorized project. File names are display metadata, not tenant identifiers or arbitrary storage paths.
3. Upload directly to the allocated S3 object using a short-lived presigned operation. Keep progress, interruption, retry, and expiration visible. Restrict bucket CORS to the application origins and required methods/headers. AWS documents [presigned uploads](https://docs.aws.amazon.com/AmazonS3/latest/userguide/PresignedUrlUploadObject.html) and [S3 CORS rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ManageCorsUsing.html).
4. Ask the API to finalize the upload. The server verifies object identity, expected size, and the selected checksum contract before enqueueing validation. Browser validation improves feedback; the server enforces acceptance and quotas.
5. Display `uploaded`, `validating`, `ready`, or `rejected` from server state. A transfer reaching 100% does not mean the dataset is training-ready.

Keep a partially completed upload recoverable. Do not mark it usable after a tab refresh unless finalization and validation have actually completed. Use the exact checksum behavior defined in the data contract; never assume an S3 ETag is a full-file checksum.

### Train, evaluate, and deploy

Show a review screen with dataset version, supported model configuration, quota impact, and validation policy. Submit a stable idempotency key so a double click or network retry does not create two paid jobs.

The job page uses persisted stage states and timestamps. Poll initially, pause polling when the page is hidden where appropriate, and retry transient failures with bounded backoff. Show indeterminate progress when total work is unknown; do not invent a percentage from elapsed time. Distinguish cancellation requested, cancellation completed, and failure.

The model page identifies the dataset, feature specification, code revision, training run, and evaluation split. Compare only compatible measured results. A deployment action is available only when server-side eligibility checks pass. The UI shows `deploying` until artifact validation and an exact-version serving probe succeed, then the backend activates the deployment pointer. Recommendation requests separately expose model-loading or unavailable states when a later Lambda environment loads that artifact; one probe does not keep every environment warm. Changing a dropdown alone never means the model has served a request. Record cold and warm request latency separately. Rollback selects an eligible previous version and displays the completed transition. See [architecture.md](architecture.md) for version selection and warm-runtime caching behavior.

### Request recommendations through the demo or API explorer

Use the same request/response contract in the public demo and authenticated developer explorer. Display the effective strategy and model version returned by the server. A popularity or cold-start fallback is labelled as such and remains a useful experience; it must not be presented as a DeepFM prediction.

For exposure logging, retain recommendation request and impression identifiers. Record impressions only when the agreed visibility rule is met, and associate clicks/watch activity with items actually exposed. Retry events with stable event IDs for deduplication. Scrolling, a paused video, or a failed media load must not fabricate positive interactions. Freeze the visibility and watch-event rules with the [data contract](data-and-ml-contracts.md).

The demo uses a bounded anonymous session and a dedicated project. Any demo project credential stays on the server. Visitors may submit narrowly scoped, deduplicated session impression/click events; dataset, catalog, and model mutations and training controls remain unavailable. Reset clears the visitor session; it cannot reset another visitor's project or model. For the short AWS session, keep the audience controlled and quotas low; collect only the interactions needed to demonstrate the contract.

The existing KuaiRand preview remains explicitly labelled **historical research inference**. Its item IDs are unrelated to the playable catalog, and its in-sample scores are not live-feed quality measurements. Real model-ranked demo content requires project-compatible items, features, and interactions first. Preserve this boundary from the [current inference contract](../../serving/README.md).

## 5. Frontend structure and identity

Proposed application structure:

```text
apps/web/src/
  app/             # routes, providers, error boundaries
  features/        # projects, datasets, jobs, models, deployments, demo
  components/      # shared forms, navigation, tables, charts, feedback
  api/             # generated OpenAPI types/client and request adapter
  auth/            # Cognito integration and session lifecycle
  styles/          # design tokens and global accessibility styles
  test/            # fixtures and cross-feature browser scenarios
```

Use typed, versioned OpenAPI contracts rather than independent copies of server interfaces. Keep server state in one query/cache layer; local UI state covers filters, dialog state, and drafts. Project changes must invalidate project-specific cache entries. Durable account preferences belong to the API; storage in the browser is reserved for intentional presentation preferences or explicitly bounded demo-session state.

For the proposed browser client, use Cognito authorization code flow with PKCE, including callback/state validation and a fresh verifier for each sign-in. A public client has no client secret. Keep tokens out of URLs, analytics, logs, and general application storage; choose and test a refresh/session policy during implementation. A future cookie-session/BFF alternative requires its own CSRF/session design. AWS describes [PKCE](https://docs.aws.amazon.com/cognito/latest/developerguide/authorization-endpoint.html) and [client credential handling](https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-security-best-practices.html).

Every backend request authorizes the authenticated principal against the requested project. Hiding a button, a project ID in the route, or a client-side role check does not grant access. API keys are created with scopes, displayed once when issued, masked subsequently, and never embedded in public documentation. Copyable examples use placeholders by default; copying a live key is a deliberate authenticated action. Do not persist the plaintext key in local storage or a reusable query cache, and clear it from rendered state after the issuance dialog closes. Test that a later visit shows only the masked summary. Show that summary in the recording, rather than the one-time credential.

## 6. Visual and interaction requirements

Use the existing Loop identity consistently across the feed and dashboard: readable typography, restrained accent color, generous spacing, real media, and clear hierarchy. Dense operational tables need filtering, legible status text, and progressive disclosure. Avoid decorative metrics, artificial engagement counters, and architecture details in ordinary customer task flows.

Each major screen needs loading, empty, partial, unavailable, unauthorized, validation-error, and retry states where applicable. Failed requests must not leave stale model-version labels or show another project's cached data. Errors describe a next action and provide a safe request ID for support/debugging, without exposing stack traces or credentials.

Target [WCAG 2.2 AA](https://www.w3.org/TR/WCAG22/) for applicable flows, with keyboard navigation, visible focus, semantic controls, accessible labels, contrast checks, reduced motion, and non-color status indicators. Dialogs must manage focus and restore it on close. Keyboard video shortcuts must not intercept typing in forms. Pause off-screen media, keep playback muted until user action, and provide useful failure/retry controls.

## 7. Build and release

Vite produces a static production bundle; its [build documentation](https://vite.dev/guide/build) supports this deployment shape. Publish immutable hashed assets to a private S3 origin through CloudFront. Use [Origin Access Control](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/private-content-restricting-access-to-s3.html), with the exact bucket/origin policy defined in infrastructure code.

The short-run backend follows the low-cost serverless topology in [architecture.md](architecture.md). The initial deployment uses separate default CloudFront and API Gateway HTTPS hostnames, with API CORS restricted to the exact frontend origin. Disable caching for authenticated and project-specific API responses. Configure SPA route handling only for frontend paths so an API failure is never rewritten into `index.html`. A later same-origin CloudFront API behavior is an optional ADR, not part of the initial topology. Keep a locally recoverable frontend build and verify API compatibility before rollback. Client configuration contains public identifiers and endpoints only; AWS credentials, signing secrets, model artifacts, and customer data never enter the bundle.

## 8. Recording and teardown handoff

Prepare the recording before provisioning anything. Use a small licensed or explicitly synthetic fixture, matching item IDs, and prechecked model artifacts. Synthetic data and tiny-run metrics must be labelled; they demonstrate workflow correctness and do not establish real-user relevance or production-scale performance.

Rehearse this sequence locally, then record the same sequence on AWS:

1. Open the deployed introduction and explain the problem and bounded deployment scope.
2. Browse the demo; show the effective strategy/version and one genuine interaction event.
3. Sign in, open the sample project, and upload the small fixture. Show server validation rather than only the file transfer.
4. Show the implemented job lifecycle, measured evaluation output, and deployment selection. If a stage was precomputed locally, identify it instead of implying it ran on AWS.
5. Request recommendations through both the UI and an API example; demonstrate one useful rejected request or unavailable/fallback state.
6. Show the architecture and evidence that connects a response to its dataset/model/code version. Record deployment time and region, then follow the teardown runbook.

Prepare a safe recording workspace: sign in off-camera where appropriate, hide terminal history and notifications containing credentials, and use placeholder keys in published request examples. Never publish a token, presigned URL, live API key, or a recording that depends on its secrecy. Export screenshots and a short readable tour with captions before teardown; crop or redact credentials before publishing.

After teardown, update GitHub's project introduction with the recording, capture date, exact demonstrated scope, local reproduction steps, and a clear note that the temporary AWS environment was removed. Remove or relabel any live-demo links and status indicators. Keep the recording and documentation useful without an active AWS account or hosted endpoint.

## 9. Acceptance and review evidence

| Verification | Minimum review evidence |
|---|---|
| End-to-end journey | Local rehearsal covers a fresh account/project, valid upload, a real job, eligible deployment, and recommendations through both API and demo/explorer; AWS recording identifies exactly which of those steps ran in the short deployment. |
| Isolation and identity | Switching projects clears scoped state; expired sign-in is handled; unauthorized routes and direct API calls fail without leaking another project's data. |
| Failure handling | Rejected schema, interrupted upload, failed/cancelled job, unavailable ranker, and revoked key produce actionable UI states. |
| Responsive layout | Rendered review at approximately 390×844, 768×1024, and 1440×900; no obstructed controls or unintended page overflow. |
| Accessibility | Keyboard walkthrough of sign-in, upload, job review, deployment, and demo; automated checks supplemented by manual focus and screen-reader review. |
| Browser behavior | Meaningful browser tests on a pinned Chromium version for the release; Firefox/WebKit smoke checks where available. State the tested matrix rather than claiming unmeasured browser coverage. |
| Contract integrity | Generated client matches the deployed API version; fallback/version labels match real responses; no secrets in built assets. |

Record screenshots, a short narrated walkthrough, deployed commit/version, measured request latency with workload details, and links to tests and architecture decisions. Include teardown completion and the final budget evidence defined in the phase plan. These let recruiters understand the product and let technical leads verify its implementation. Treat performance and reliability numbers as evidence only after measurement; a polished interface and a short successful cloud run are not evidence of high availability or sustained production operation.
