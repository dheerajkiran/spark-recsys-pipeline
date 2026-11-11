# Identity, project access, and credential lifecycle

This guide describes the checkpoint 7.3 implementation. The [checkpoint ledger](checkpoints.md) and completion record hold its verification status. These local commands create no AWS resources. The research preview continues to have its separate interface.

## Run a private local account setup

Use the repository's Node 22 runtime (`>=22.13.0 <23`) and run these commands from the repository root. Choose a fresh absolute data directory: initialization refuses to replace an existing signing key.

```sh
npm ci
umask 077
npm run local:admin --workspace @loop/api -- init /tmp/loop-platform-73
npm run local:admin --workspace @loop/api -- token /tmp/loop-platform-73 owner-a
npm run local:admin --workspace @loop/api -- token /tmp/loop-platform-73 owner-b
npm run local:admin --workspace @loop/api -- token /tmp/loop-platform-73 viewer
```

Initialization writes a private signing JWK, public JWKS, and `local.env` below `/tmp/loop-platform-73/local-auth`. Token commands write one-hour tokens to the named subject's `.token` file. Tokens and signing material are local credentials; keep them outside the repository and recording evidence. The HTTP service reads only the public JWKS. The local operator command can create identities and assign memberships because its caller controls that private directory; there is no public registration or membership mutation endpoint.

Start the server in another terminal:

```sh
node --env-file=/tmp/loop-platform-73/local-auth/local.env --import tsx services/api/src/main.ts
```

The default address is `http://127.0.0.1:4100`. `GET /health` is public. Without explicit authentication configuration, protected routes remain closed with `401`.

## Create projects and establish access

In the first terminal, create a project as `owner-a`, retain its response, and list that principal's projects:

```sh
AUTH_A="Authorization: Bearer $(cat /tmp/loop-platform-73/local-auth/owner-a.token)"
curl --fail-with-body --silent --show-error \
  --header "$AUTH_A" --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: create-project-a' \
  --data '{"name":"Local project A"}' \
  --output /tmp/loop-platform-73/project-a.json \
  http://127.0.0.1:4100/v1/projects
PROJECT_ID="$(node -p "JSON.parse(require('node:fs').readFileSync('/tmp/loop-platform-73/project-a.json', 'utf8')).project.project_id")"
curl --fail-with-body --silent --show-error --header "$AUTH_A" \
  'http://127.0.0.1:4100/v1/projects?limit=20'
```

Creation returns `201` and makes the verified principal the project's owner. Repeating the same create request with the same idempotency key returns that project. A changed payload under that key returns `409`. The actor, operation, and project scope are part of mutation identity; retained replay identity lasts seven days.

The second token has no membership in this project. This read returns `404`, including when the project exists:

```sh
curl --silent --show-error --include \
  --header "Authorization: Bearer $(cat /tmp/loop-platform-73/local-auth/owner-b.token)" \
  "http://127.0.0.1:4100/v1/projects/$PROJECT_ID"
```

Assign the third identity a viewer membership through the local operator tool:

```sh
npm run local:admin --workspace @loop/api -- member /tmp/loop-platform-73 viewer "$PROJECT_ID" viewer
curl --fail-with-body --silent --show-error \
  --header "Authorization: Bearer $(cat /tmp/loop-platform-73/local-auth/viewer.token)" \
  "http://127.0.0.1:4100/v1/projects/$PROJECT_ID"
```

Viewers can read authorized project state and request recommendations. They cannot issue keys or mutate owner-controlled resources; those requests return `403`. A viewer may create a separate project and become its owner. Owner demotion is deliberately unsupported. The [permission matrix](access-patterns.md) and each OpenAPI operation's `x-permissions` govern authorization before child-resource lookup.

## Issue, use, and revoke a developer key

Only a project owner may issue, list, or revoke keys. Select from `catalog:write`, `events:write`, and `recommendations:read`; integration credentials never grant dataset upload, training, deployment, membership, or key administration permissions. Set expiry in the future and no more than 30 days after issuance.

This example saves the one-time response to a private local client file. The API's persisted identity state contains the digest and masked summary, never the full secret.

```sh
EXPIRES_AT="$(node -p 'new Date(Date.now() + 86400000).toISOString()')"
curl --fail-with-body --silent --show-error \
  --header "$AUTH_A" --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: issue-read-key-a' \
  --data "{\"name\":\"local-demo\",\"scopes\":[\"recommendations:read\"],\"expires_at\":\"$EXPIRES_AT\"}" \
  --output /tmp/loop-platform-73/local-auth/key-issued.json \
  "http://127.0.0.1:4100/v1/projects/$PROJECT_ID/api-keys"
KEY_ID="$(node -p "JSON.parse(require('node:fs').readFileSync('/tmp/loop-platform-73/local-auth/key-issued.json', 'utf8')).key.key_id")"
INTEGRATION_KEY="$(node -p "JSON.parse(require('node:fs').readFileSync('/tmp/loop-platform-73/local-auth/key-issued.json', 'utf8')).secret")"
curl --silent --show-error --include \
  --header "X-API-Key: $INTEGRATION_KEY" --header 'Content-Type: application/json' \
  --data '{"user_id":"local-user","limit":5}' \
  "http://127.0.0.1:4100/v1/projects/$PROJECT_ID/recommendations"
```

The recommendation request currently returns `503` after credential, project, scope, and input validation: ranking is a later checkpoint. Using the key against another project returns `404`; an operation outside its scopes returns `403`. Send exactly one credential type per request. Mixed bearer/key/demo headers fail authentication.

The secret uses 32 random bytes plus a random key identifier. Verification reads the current lookup record on each request, compares SHA-256 digests with a timing-safe comparison, and checks expiry and revocation. No authenticated key identity is cached between requests. Idempotent issuance replay returns the existing summary with `secret: null`, including after revocation; a lost issuance response therefore requires issuing a new key and revoking the old one. Concurrent retries disclose the secret only to the request that commits the issuance.

List masked summaries, then revoke:

```sh
curl --fail-with-body --silent --show-error --header "$AUTH_A" \
  "http://127.0.0.1:4100/v1/projects/$PROJECT_ID/api-keys?limit=20"
curl --fail-with-body --silent --show-error \
  --header "$AUTH_A" --header 'Content-Type: application/json' \
  --header 'Idempotency-Key: revoke-read-key-a' --data '{}' \
  "http://127.0.0.1:4100/v1/projects/$PROJECT_ID/api-keys/$KEY_ID/revoke"
```

Revocation updates the project summary and authentication lookup atomically. Subsequent requests using that secret return `401`. A request already authorized before a concurrent revocation can finish; this is not cancellation of in-flight work. Audit records contain action, hashed actor, request/resource identifiers, and permitted scope/role metadata. They exclude bearer tokens and full key secrets. Responses carry `Cache-Control: no-store`.

## JWT trust and Cognito configuration

The verifier uses pinned `jose` 6.2.17. It accepts only RS256 access tokens with a configured issuer, recognized key identifier, verified signature, valid issuance/expiry/not-before timing, `token_use: access`, matching `client_id`, a human `username`, and all configured scopes. JWT time checks allow 30 seconds of clock difference, including the extra issuance-time check; the 24-hour token-lifetime ceiling remains unchanged. Principal identity is `issuer#sub`; a body field or decoded gateway claim does not establish identity. Token-selected `jku`, `x5u`, and embedded keys never choose the trust source.

Local mode fixes issuer `http://loop.local` and client `loop-local`, uses an explicit public JWKS path, and defaults to the `loop/api` scope. Both the signer and verifier reject Lambda execution. Local and Cognito configuration cannot be combined.

For the future Cognito deployment, use `LOOP_AUTH_MODE=cognito`, explicit `LOOP_STORAGE=dynamodb`, `LOOP_DYNAMODB_TABLE`, `AWS_REGION`, `LOOP_COGNITO_ISSUER`, `LOOP_COGNITO_CLIENT_ID`, and an at-least-32-byte `LOOP_CURSOR_SECRET`. Cognito configuration cannot use the default local file store: both environment loading and application construction reject that combination before serving requests. The issuer must be a standard HTTPS Cognito user-pool URL. Keys are obtained only from that issuer's derived `/.well-known/jwks.json` endpoint, with bounded fetch time and cache settings. `LOOP_JWT_SCOPES` is a unique space-separated scope list, defaulting to `loop/api`.

Cognito access tokens bind their app client through `client_id`; this is checked even when `aud` is absent. If resource binding is used, configure `LOOP_COGNITO_AUDIENCE` to require the expected `aud` as an additional check. ID tokens are rejected. The owner-controlled user pool, disabled self-registration, authorization-code/PKCE client, `loop/api` resource-server scope, and seeded recording accounts remain infrastructure work for 7.10 and cloud verification for 7.11. No user pool or AWS account setting is created by this checkpoint.

JWT signature verification does not query Cognito revocation state. Browser logout or pool-side revocation is not an immediate API access-token revocation guarantee here; an otherwise valid token can remain accepted until expiry. Use short access sessions in the later deployment. The application-managed developer-key revocation check is separate.

## Failure responses and diagnostics

Unexpected application and adapter failures return `500 internal_error`. Explicitly classified storage or signing-key dependency failures return `503 unavailable`; invalid credentials still return `401`. Invalid generated responses and corrupt persisted records are internal failures. DynamoDB validation/serialization errors remain internal failures, while service/network outages retain a typed dependency cause.

Each unexpected failure emits one JSON error record to stderr, correlating its request ID with the response. Logs contain only the error level, request ID, safe error class name, and bounded stack source locations. Headers, bodies, thrown messages, raw error objects, and causes are excluded because they may contain credentials. Logger failure cannot change the HTTP outcome. Failed lazy verifier initialization is cleared so a later request can retry setup; startup configuration errors still fail startup.

## Persistence, transactions, and operational limits

The local adapter writes `<LOOP_DATA_ROOT>/identity/records.json` with mode `0600` beneath a `0700` directory. It checks every expected revision before publishing an atomic snapshot. An exclusive `write.lock` serializes mutations across instances and processes. Lock acquisition times out after five seconds; an existing lock is never silently deleted. After a crashed writer, stop the server and all operator processes, inspect the recorded PID and snapshot, and only remove the lock once no writer can still be active. Retain a backup before manual recovery.

`LOOP_STORAGE=dynamodb` selects the AWS SDK adapter, pinned to SDK 3.1167.0. It requires `LOOP_DYNAMODB_TABLE` and `AWS_REGION`; the existing table must have string partition/sort keys `PK` and `SK`. In local authentication mode, an explicit loopback-only HTTP `LOOP_DYNAMODB_ENDPOINT` is mandatory and fixed dummy credentials are used, so local tests do not consult workstation AWS credentials. Cognito mode rejects local endpoints. Table creation, IAM, encryption, cloud deployment, and teardown remain infrastructure tasks.

| Partition | Sort-key families | Purpose |
|---|---|---|
| `PRINCIPAL#<sha256(issuer#sub)>` | `PROJECT#<project_id>`, `SEQUENCE`, `IDEMPOTENCY#<digest>` | Principal's project index, insertion order, project-create replay identity |
| `PROJECT#<project_id>` | `META`, `MEMBER#<principal_digest>`, `KEY#<key_id>`, `KEY_SEQUENCE`, `IDEMPOTENCY#<digest>`, `AUDIT#...` | Project ownership, membership, masked credentials, mutation replay, audit |
| `KEY#<key_id>` | `LOOKUP` | Digest, project binding, scopes, expiry, revocation; accessed only after parsing a credential identifier |

Gets and base-table queries use strongly consistent reads. Queries require one partition and a sort-key prefix; no table scan or global membership scan is used. Project creation, membership changes, issuance, and revocation use conditional transactions. Key administration also conditions on the owner membership revision, preventing a role change from racing through an issuance/revocation commit. Local and DynamoDB tests exercise competing writers, isolated key collisions, and all-or-none failure behavior.

HTTP list responses default to 20 items and cap at 100. Encrypted/authenticated cursors bind route, project path, principal, insertion snapshot, and a 15-minute expiry; cursor-secret rotation invalidates existing cursors. New inserts are excluded from the captured snapshot even when wall-clock timestamps match. Current adapters still materialize the authorized partition's matching records before slicing a response. This is a bounded-response implementation for the small portfolio dataset, not proof of bounded database work at large account scale. Idempotency expiry is checked explicitly; physical cleanup of old replay/audit records is not provisioned here.

Uploads, item/event ingestion, jobs, models, deployments, and recommendations have enforced permission boundaries but return `503` to authorized callers until their implementation checkpoints. Public demo session creation also returns `503`; demo credentials are not accepted before 7.9. No request in this checkpoint starts training, Spark, a workflow, or an AWS deployment.

## References

Runtime behavior is implemented in [auth.ts](../../services/api/src/auth.ts), [identity-service.ts](../../services/api/src/identity-service.ts), [identity-store.ts](../../services/api/src/identity-store.ts), and [dynamo-identity-store.ts](../../services/api/src/dynamo-identity-store.ts). Package versions are fixed in [the API manifest](../../services/api/package.json) and [root lock file](../../package-lock.json).

Official references reviewed November 11, 2026:

- [jose 6.2.17 JWT verification](https://github.com/panva/jose/blob/v6.2.17/docs/jwt/verify/functions/jwtVerify.md) — version-pinned verification API.
- [Cognito access-token claims](https://docs.aws.amazon.com/cognito/latest/developerguide/amazon-cognito-user-pools-using-the-access-token.html) — access-token purpose, `client_id`, optional resource audience, and scope semantics.
- [Cognito revocation and JWT-library limitations](https://docs.aws.amazon.com/cognito/latest/developerguide/token-revocation.html) — signature/expiry verification does not establish pool-side revocation status.
- [DynamoDB transactional APIs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html) and [query behavior](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html) — conditional all-or-none writes, scoped queries, read consistency, and database-page boundaries.
