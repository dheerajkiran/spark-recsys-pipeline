# Initial permissions and storage access patterns

Checkpoint 7.1 design, November 10, 2026. These are enforcement targets for 7.2–7.7, not a deployed database or authorization implementation. Machine-readable per-operation permissions are in [OpenAPI](../../contracts/openapi.json); the contract tests require an explicit permission policy on every operation.

## Permission matrix

| Operation family | Owner JWT | Viewer JWT | Integration key | Demo session |
|---|---|---|---|---|
| List/read authorized project data, datasets, jobs, models, deployments | Yes | Yes | No | No |
| Create a separate project as its owner | Yes | Yes | No | No |
| Upload/refresh/finalize; start/cancel training; deploy/rollback | Yes | No | No | No |
| Create/list/revoke credentials | Yes | No | No | No |
| Write catalog items | Yes | No | `catalog:write` | No |
| Record project events | Yes | No | `events:write` | Own demo session only |
| Request recommendations | Yes | Yes | `recommendations:read` | Fixed demo project only |
| Read masked credential summaries | Yes | No | No | No |
| Health; create bounded demo session | Public | Public | Public | Public |

An unauthenticated caller fails with `401`. A verified principal outside the requested project receives `404`; a project member lacking mutation permission receives `403`. These checks precede child-resource lookup. A project owner is not a global administrator. The public demo never accepts a caller-selected project or grants upload/training/model permissions.

## Query and transaction targets

Key forms below describe logical records; checkpoint 7.3 will choose the physical table/index layout. An authorized project context is mandatory even when a globally formatted child ID looks unique. Never scan all projects to satisfy a customer request.

| Access pattern | Keys or index | Required condition / publication rule |
|---|---|---|
| List a principal's projects | Membership index keyed by verified principal | Return only current memberships; verify project membership again for operations |
| Get project/membership | `PROJECT#id`, `META` / `MEMBER#principal` | Principal must match membership; mutation requires owner |
| Get/list datasets | `PROJECT#id`, `DATASET#id` / typed prefix | Immutable ready manifest identity; generated object keys only |
| Start/get/list jobs | `PROJECT#id`, `JOB#id` / typed prefix | Transaction claims job plus outbox; active-job quota and dataset readiness |
| Dispatch/reconcile pending work | Scoped pending-outbox index | Stable execution identity, bounded leases/attempts, no recursive stream trigger |
| Retry a mutation | Project/actor/operation/idempotency key | Canonical payload digest matches; original status/resource retained seven days |
| Upsert/get catalog item | `PROJECT#id`, `ITEM#external_id` | Batch bounded; existing IDs scoped to this project; version mutable metadata |
| Accept event | Project plus event ID | Conditional logical event/digest and outbox transaction; 30-day replay retention |
| Read exposure identity | Project plus impression/request ID | User/item/session must match the actual exposed recommendation |
| Read model registry | `PROJECT#id`, `MODEL#id` | Immutable dataset/artifact/spec digests; registration does not deploy |
| Promote/rollback | `PROJECT#id`, `DEPLOYMENT` | Compatible exact-version probe then compare-and-swap generation |
| Read feature snapshot | Project plus immutable snapshot ID | Complete published inventory; one cutoff for all request candidates |
| Publish snapshot pointer | `PROJECT#id`, `FEATURE_SNAPSHOT` | Immutable snapshot ready; conditional generation update |
| Resolve integration key | Key identifier to digest/project/scopes | Digest verified, not expired/revoked; no plaintext persistence |
| Read demo session | Fixed demo project plus opaque session identity | Expiry, narrow request/event limits; no other visitor's state |

All list routes use opaque bounded cursors tied to principal/project, route, and filter identity. Changing projects invalidates pagination and client caches. Stable ordering and the snapshot boundary must survive inserts; expired or mismatched cursors return `400`.

Idempotency identity includes authenticated actor, authorized project, operation, and key; canonicalized payload fields are covered. A payload mismatch gives `409`. Key issuance replay returns the existing summary and `secret: null`; it cannot reveal a secret twice. Upload refresh with a new key may replace expired targets only for the same pending dataset and declared file identities. Transaction retries cannot create a second logical job. Event requests with the same ID/payload deduplicate, while the same ID with changed content conflicts. Expiration is a documented retry limit, not an assertion of exactly-once network delivery.

Authorization, transactional outbox, conditional publication, cursor signing, key hashing/revocation, snapshot storage, and negative cross-project HTTP tests remain implementation work. The two fixtures provide colliding external IDs for those future tests; validating the fixture files alone does not prove request isolation.
