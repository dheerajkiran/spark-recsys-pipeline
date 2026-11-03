# Budget, recording, evidence, and teardown

**Deployment mode:** one temporary student-portfolio AWS session. **User target:** less than **$10 total**, followed by teardown and no ongoing hosted service. **Status:** planned runbook; no resources or charges have been created by this documentation.

This document is a release constraint for [checkpoints 7.10–7.12](checkpoints.md), not a promise that a cloud provider will enforce a $10 account spending ceiling. Prices, account quotas, existing account usage, taxes, traffic, retries, and resources that fail to delete affect the bill. AWS Budgets notifications use delayed billing data and are not a real-time circuit breaker. [AWS budget guidance](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html)

## 1. Work locally, pay only for the final proof

Complete the API, dashboard, demo, data validation, feature processing, training, model routing, failure handling, and recording rehearsal locally. Build container images locally. Preserve the original research pipeline and model results without rerunning the full dataset in AWS.

The recording deployment uses a tiny fixture that is already known to fit the resource limits. There is one normal cloud feature-build/training workflow, a small number of browser/API requests, a short model promotion/rollback demonstration, and then removal. Seed trusted version A from local training, produce version B in the single AWS training run, promote B, then roll back to A; preserve the distinct local/cloud provenance. Do not use AWS as the development environment or leave resources online for recruiter visits. Recruiters receive a recording and reviewable repository evidence.

Target at most **four hours from first cloud resource creation to stop/teardown start**, including infrastructure provisioning, verification, and filming. Teardown continues until resource removal is verified, even if CloudFront deletion or another service takes longer than the filming window. Storage and other residual resources may remain billable during that interval and belong in the estimate.

## 2. Workload envelope

These are proposed implementation limits. They must be enforced in configuration, API/domain validation, and job launch settings, rather than only printed here.

| Dimension | Recording-session envelope |
|---|---|
| Environment | One AWS account and one selected region; `us-east-1` is the initial pricing assumption |
| Audience | Owner-controlled test identities, two isolation-test projects, one isolated public-demo project |
| Data | Seeded fixture around 100 users/100 items/10,000 mature impressions; exact size fixed after local rehearsal |
| Training input | At most 50 MiB staged per training run; bounded files/rows and checked uncompressed size |
| Artifacts | Model bundle at most 10 MiB; total live dataset/artifact object storage target below 1 GiB |
| Spark | One EMR job; one 1-vCPU/2-GB driver plus one 1-vCPU/2-GB executor; proposed application ceiling 2 vCPU/4 GB |
| Spark lifetime | At most 15 minutes execution; no initial/preinitialized capacity; explicit auto-stop |
| Training | One CPU training invocation, proposed 4 GiB/600 seconds/3–5 epochs; smaller settings if account quotas or local profiling require it |
| Model requests | At most 1,000 ranking requests; at most 200 candidates and 20 results per request |
| Other API traffic | At most 5,000 calls including polling/retries; pause polling when a tab is hidden or a job is terminal |
| Job launches | One active expensive workflow globally for the session, with stable idempotency keys; no automatic training sweep |
| Workflow | Finite retries and a proposed 45-minute end-to-end timeout; reconcile late results before retrying |
| Logs | Structured/redacted summaries, target below 100 MiB, one-day retention, then explicit deletion |
| Static/media delivery | Small selected clips/assets, target below 1 GiB of AWS-served download traffic; no video transcoding or bulk media import |
| Runtime | Standard on-demand Lambda; no provisioned concurrency, managed-instance fleet, paid warm pool, or idle Spark workers |

The Spark heap plus memory overhead must fit the selected worker size; 2 GB of worker memory is not 2 GB of usable Spark heap. Set driver/executor settings explicitly, pin one executor, and verify the chosen EMR release supports the configuration. EMR bills workers while allocated and has a minimum billable duration; it also charges for preinitialized capacity if configured. [EMR pricing](https://aws.amazon.com/emr/pricing/)

New AWS accounts can have reduced Lambda memory/concurrency quotas. Check the intended account before relying on 4 GiB training memory or reserved concurrency. Lambda's normal function duration limit is 15 minutes; the proposed 600-second training timeout is deliberately lower. [Lambda quotas](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html)

If the rehearsed fixture does not fit, reduce the fixture/settings and update the contract evidence. Do not automatically add Fargate, a GPU, a larger EMR cluster, or a permanently warm inference endpoint.

## 3. Cost preflight and reserve

Use standard on-demand rates for the selected region and architecture. **Assume zero Free Tier credits and zero promotional credits** when deciding whether to launch. Actual credits can reduce the final bill, but they are not a design dependency. Avoid commitments, subscriptions, paid domains, and recurring purchases. In particular, choose **CloudFront pay-as-you-go, with no flat-rate plan**: a subscribed distribution cannot be deleted until that plan has been cancelled and its current billing cycle ends. [CloudFront deletion requirements](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/HowToDeleteDistribution.html)

The following is a conservative **allocation worksheet**, not an AWS quote or a measured bill. Fill in actual rates and expected usage before checkpoint 7.11.

| Category | Initial allowance | What the estimate must include |
|---|---:|---|
| Bounded EMR run | $1.00 | All driver/executor billed time, initialization/idle time, any billable disk, and finite retries |
| All Lambda functions | $0.75 | API, ranker initialization and scoring, training, stream/reconciliation handlers, auth work, and allocated ephemeral storage |
| HTTP/workflow/identity | $0.50 | API Gateway, Step Functions transitions/retries, Cognito usage/features; disable SMS and unneeded paid options |
| Object/database/registry storage | $0.75 | S3 requests/versions, DynamoDB units/indexes/transactions/Streams consumers, ECR images, and any bootstrap state storage |
| Frontend and transfer | $0.75 | CloudFront requests/transfer, S3 reads, media download, and any regional transfer charges |
| Logs, alarms, and cleanup overhead | $0.50 | Ingestion/storage, bounded telemetry, stop mechanism, deletion delays, and residual storage |
| Unallocated usage headroom | $0.75 | Small metering differences and incidental usage identified by preflight |
| **Maximum planned usage subtotal** | **$5.00** | Reduce scope if the regional calculation exceeds this subtotal |
| Taxes/currency/payment headroom | **up to $2.00** | Replace with the account's actual tax/currency assumptions |
| Failure/cleanup contingency | **up to $2.00** | Use only after checking consumed resource estimates; not permission for another run |
| **Planning envelope** | **$9.00** | Leaves margin below the user's $10 target; not an enforced account cap |

Do not force an expense into an allowance when the regional estimate is higher. A launch requires a refreshed estimate, including taxes and reserve, that still targets less than $10. If the account has unrelated resources, separate their costs and record whether the user's target covers only this project or the whole account; never hide them in a portfolio cost claim.

### Working estimate for building and the one-time run

As of November 2, 2026, a reasonable planning range for a successful tiny AWS recording session is **$1–$3 in usage before tax**, assuming the workload envelope above, local image builds, no paid subscriptions, and prompt complete teardown. This is an engineering estimate with headroom, not a quote or measured bill. Keep the **$5 usage allowance** and **$9 total reserve envelope** rather than treating the expected range as a hard limit.

| Example component | Assumed use | Illustrative cost before other services |
|---|---|---:|
| EMR Serverless compute | 2 vCPU + 4 GB for 15 minutes | About $0.032 |
| CPU training Lambda | 4 GB for 600 seconds, once | About $0.040 |
| Inference Lambda | 1,000 requests × 2 GB × 2 seconds | About $0.067 |
| HTTP API | 5,000 small metered calls | About $0.005 |

These examples use US East pricing: AWS publishes EMR example rates of `$0.052624/vCPU-hour` and `$0.0057785/GB-hour`, Lambda x86 compute at `$0.0000166667/GB-second`, and an HTTP API example at `$1/million` calls. The assumed inference duration is not measured; initialization, dispatcher/API functions, retries, workflows, DynamoDB, identity, images, storage, logs, and delivery add to these component values. [EMR rate example](https://docs.aws.amazon.com/solutions/latest/scene-intelligence-with-rosbag-on-aws/cost.html), [Lambda pricing](https://aws.amazon.com/lambda/pricing/), [API Gateway pricing](https://aws.amazon.com/api-gateway/pricing/)

The local build requires **no additional paid development software** under this plan, assuming an existing capable computer, internet connection, and development tools. Developer time, electricity, existing subscriptions, and any separately metered AI assistance are excluded. No hired-developer or labor estimate is included. After every project resource is removed, the target recurring AWS charge is **$0**; previously incurred charges can still appear later as billing settles.

### Calculations to record

- Lambda: `sum(invocations × billed seconds × configured GB × regional GB-second rate)` plus requests, initialization behavior, extra ephemeral storage, and any explicitly enabled feature charges. Billed duration includes cold-start work as applicable; use measurements rather than counting only forward-pass time.
- EMR: aggregate **billed** vCPU-hours and memory-hours × current regional rates, plus billable storage/initial capacity. Record `billedResourceUtilization` after the run where available; an execution timeout is not the same as an exact billable-duration limit.
- API Gateway: metered request units × regional rate plus transfer; upload bulk data directly to scoped S3 rather than through an API body.
- Step Functions: actual Standard-workflow transitions, including retries and error paths, × regional rate.
- DynamoDB: actual read/write units including item-size, transactional, consistency, and index multipliers; storage and selected optional features separately. Do not count a transaction as a single ordinary write.
- Storage/registry/edge/logs: actual size, request volume, retention duration, image size, transfer destination, and any minimum or feature-specific charges.

For scale only, AWS's published US East x86 Lambda example rate of `$0.0000166667/GB-second` would make **one 4-GB, 600-second training invocation approximately $0.04 in compute**, before associated requests/storage/services. That is one component, not a total deployment quote. Reconfirm the applicable runtime and rate before launch. [Lambda pricing](https://aws.amazon.com/lambda/pricing/)

Price worksheet sources, to revisit on the deployment date:

- [API Gateway](https://aws.amazon.com/api-gateway/pricing/)
- [Step Functions](https://aws.amazon.com/step-functions/pricing/)
- [DynamoDB](https://aws.amazon.com/dynamodb/pricing/)
- [S3](https://aws.amazon.com/s3/pricing/), [ECR](https://aws.amazon.com/ecr/pricing/), and [CloudFront](https://aws.amazon.com/cloudfront/pricing/)
- [Cognito](https://aws.amazon.com/cognito/pricing/) and [CloudWatch](https://aws.amazon.com/cloudwatch/pricing/)

Save the price date, region, selected service options, rates, input quantities, projected subtotal, reserve, and stop conditions in a sanitized preflight note. These documents do not yet contain a deployment-date quote.

## 4. Spend controls that must exist before launch

1. **Prevent unintended launches.** Ordinary CI checks never apply infrastructure or start a model run. Cloud delivery is a deliberate manual operation targeting the reviewed account/region.
2. **Bound expensive work.** Enforce data size, model settings, one active job, stable execution identity, fixed capacity, job timeout, workflow timeout, and finite retries. A browser refresh must not create a new training job.
3. **Bound public use.** Disable public registration and training; narrow demo routes; set gateway throttles, application request budgets, expiry, and an immediate disable switch. These controls reduce exposure but do not create a dollar-accurate hard cap.
4. **Avoid idle resources.** The primary plan creates no NAT gateway, ALB, RDS database, GPU, ECS service, provisioned Lambda concurrency, EMR initial capacity, paid custom domain, or persistent paid cache. Avoid placing Lambda/EMR in a VPC when same-region managed service endpoints suffice for this workload.
5. **Keep telemetry small.** Do not log bodies, datasets, feature arrays, tokens, or per-item model internals. Cap local load generation and use low-volume structured logs.
6. **Stop independently.** Prepare a tested stop command that disables new jobs/demo mutations, turns off stream/schedule triggers as needed, and cancels active workflows and EMR jobs. A timed stop can supplement it but must be scoped and tested; it cannot replace supervised teardown.
7. **Track time and estimated use directly.** The session owner watches the wall clock and launched work. Do not wait for a billing alert before stopping. A small budget alert can provide additional notification; confirm any selected alert/action feature's cost.

Set a stop decision when the estimated consumed-plus-unfinished usage reaches the $5 planned-usage envelope, when an unplanned resource appears, or when the four-hour session window is reached. Preserve evidence and tear down rather than broadening the deployment.

## 5. Local rehearsal before the paid session

Rehearse the exact recording script against the same API/domain code, model format, and fixture generator. The local transport/storage/workflow adapters must identify where cloud semantics differ; real Cognito authorization, IAM, S3 policies, Lambda initialization, and EMR execution are verified during the paid proof.

The rehearsal should produce:

- A fresh-clone startup path with reproducible fixture generation and the full upload/train/deploy/recommend workflow.
- Passed cross-project authorization, duplicate job/event, corrupt artifact, point-in-time feature, and rollback checks.
- A measured container training time comfortably below 600 seconds, a bounded model, and a memory/disk report.
- A dry recording with no typing surprises, broken media links, missing artifacts, or speculative metrics.
- Validated infrastructure/configuration, complete resource inventory, and a reviewed teardown dependency order.
- An evidence export that can be opened locally before its cloud source is deleted.

Lambda cold-start performance must be measured on AWS; local container startup is insufficient proof. HTTP API integrations have a 30-second maximum timeout. A slow initial load must produce an honest loading/unavailable path or require a design change, not an assertion that a local test established cloud readiness. [HTTP API quotas](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-quotas.html)

## 6. Recording plan

Prepare two complementary recordings or one recording with chapter markers. Editing can remove waiting periods, but label time jumps and preserve the underlying measured job duration and execution evidence.

### Product tour — approximately 2–3 minutes

| Segment | Show | What it establishes |
|---|---|---|
| Opening | Product name, one-sentence purpose, three experiences | A recruiter can understand the project quickly |
| Demo | Playable feed, selected profile, actual ranking strategy/version | A working user experience calling the shared engine |
| Customer workflow | Create project, upload fixture, validation, job status, results | A complete self-service path with real backend state |
| Developer integration | Masked test credential and a successful recommendation request | The platform can serve another application |
| Closing | Architecture/evidence links and temporary-deployment note | Where reviewers can inspect the engineering |

### Engineering walkthrough — approximately 5–8 minutes

Show the project boundary, an invalid upload, an unauthorized request, one real feature/training job, immutable dataset/model identity, measured evaluation with synthetic-data disclosure, exact serving version, deliberate rollback, and a few sanitized logs/request IDs. Mention the chosen limits and why a serverless recording deployment fits the student budget. Do not spend the recording navigating unrelated AWS consoles.

The footage must never expose a reusable API key, password, session token, presigned URL, Terraform state, or secret-bearing terminal environment. Use a masked display and a prepared test client; revoke test credentials at the end. Display sample commands with placeholders when a raw credential would otherwise enter the recording.

The dataset/model screen should say that the fixture interactions are synthetic. The separate KuaiRand results may demonstrate research quality, but do not present those results as the evaluation of the new demo catalog.

## 7. Evidence package that survives teardown

Keep small, reviewed artifacts in `docs/evidence/phase-7/` when implemented. Keep raw datasets, model weights, full unredacted logs, state, and credentials outside Git. Use a recording link or GitHub release/attachment for the video instead of committing a large binary to the source history; do not introduce a paid storage subscription for this step.

| Artifact | Required content |
|---|---|
| Product overview | Screenshot, supported journeys, short recording link, and status |
| Architecture | Actual deployed services, logical diagram, decisions, deferred production upgrades |
| Cloud session report | Date, region, code/image identifiers, fixture scope, job IDs, success/failure, measured limits |
| Data/model report | Synthetic/historical provenance, dataset/model digests, evaluation method and results, active version/rollback |
| API evidence | Sanitized request/response examples, error contract, isolation and idempotency results |
| UI evidence | Desktop/mobile/keyboard screenshots and real loading/error/failure states |
| Cost record | Preflight quantities/rates, resource utilization, provisional versus settled charges, credits/taxes treatment |
| Teardown record | Planned resource list, actual inventory, removals, exceptions, timestamps, follow-up billing check |

Open the exported recording and verify the manifests/reports locally **before** removing their cloud source. A recording with a wrong model version or unsupported claim is not repaired by a successful Terraform teardown.

Suggested repository wording after a successful session:

> Built and deployed a project-scoped recommendation platform with a React dashboard, developer API, Spark feature workflow, and versioned PyTorch inference. Verified on AWS with a small synthetic fixture on [actual date], recorded the workflow, and removed the cloud environment afterward. See the measured results and limitations in the session report.

Replace every bracketed field with observed evidence. State the final cost only after it is supportable from billing/usage records. Do not claim an always-live service, real users, an SLA, or conversion improvements.

## 8. Teardown order and residual-resource review

Implement scoped inventory/export/stop/destroy scripts during checkpoint 7.10. Do not paste broad account-wide delete commands into a demo runbook. Every deletion targets this environment's known resource identifiers and tags.

1. **Freeze new work.** Disable job starts and public-demo mutations, revoke integration test keys, and stop browser polling/recording clients.
2. **Stop active work.** Stop workflow executions, explicitly cancel EMR jobs, stop the EMR application, and disable event-source mappings/reconciliation schedules. Stopping an orchestration execution does not cancel an already-running synchronous Lambda invocation; wait for training/ranking/worker invocations to finish or reach their configured timeouts before removing the S3/DynamoDB resources they use. Inspect downstream work directly rather than assuming it has stopped.
3. **Export evidence.** Verify local recordings, sanitized logs, dataset/model manifests, cloud job resource utilization, and deployment configuration. Keep a private copy of Terraform state until inventory is clear.
4. **Remove application resources.** Delete API routes/stages, Lambda functions/versions/layers as applicable, Cognito test resources, workflows, EMR applications, queue/dead-letter resources, and the DynamoDB tables/Streams consumers created for the session.
5. **Remove storage and delivery.** Disable/delete the CloudFront distribution; remove S3 objects, versions, delete markers, failed multipart uploads, and ECR images/repositories for the environment. Deletion may require waiting for dependent resources to finish propagating.
6. **Remove supporting resources.** Remove this environment's log groups, alarms, schedules, IAM policies/roles, and any backup/export resources. Check the chosen topology did not accidentally create NAT, endpoints, public IPv4 allocations, or other unexpected billable resources.
7. **Finish bootstrap cleanup.** Remove only session-created state buckets/locks and OIDC/bootstrap resources that are not shared with another project. If any resource is intentionally retained, document why, its billing behavior, and its owner; it is not silently treated as deleted.
8. **Verify inventory.** Inspect regional resources in the deployed region and global services such as CloudFront/IAM. Compare against Terraform state and the preflight list, including resources created before a failed apply.
9. **Check delayed costs.** Revisit billing/usage after metering has appeared, then again if charges are still pending. Annotate pending costs instead of declaring the final total prematurely.

Terraform state and provider credentials can contain sensitive configuration. Store them privately outside Git and never use screenshots of raw state as portfolio evidence. S3 lifecycle rules and DynamoDB TTL are retention aids, not immediate-delete or complete-teardown guarantees.

## 9. Failure and budget fallback

| Problem | Response |
|---|---|
| Preflight estimate plus reserve cannot target under $10 | Reduce assets/data/work or keep the presentation local; do not launch an unbounded estimate |
| EMR/Lambda quota or dependency mismatch | Stop before the recording run, preserve the error, adjust locally; no automatic paid upgrade |
| Model cold start exceeds the HTTP integration limit | Show honest unavailable/loading behavior, document the result, and return to local design work |
| Training reaches its timeout | Record failure and stop; do not automatically launch another full run |
| Browser times out while a workflow continues | Inspect durable job/execution identity and reconcile before any retry |
| Infrastructure apply fails halfway | Inventory the partial stack and tear it down; failure does not mean nothing was created |
| Evidence export fails | Keep ingress/work stopped, recover only the bounded required artifacts, then finish teardown |
| Removal fails | Record the specific resource, expected continuing cost, and next removal attempt; continue until resolved |
| Final bill has not settled | Publish the provisional usage estimate as provisional and leave cost closeout open |

A local recording remains useful if a cloud gate fails, but it must be labelled local and cannot close the AWS proof checkpoint. The budget constraint takes priority over creating a more impressive-looking cloud topology.
