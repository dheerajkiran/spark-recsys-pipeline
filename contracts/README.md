# Phase 7.1 platform contracts

This is an executable specification, not a running platform. It freezes the first supported upload/train/deploy/recommend journey so later API, Spark, ranker, and frontend implementations share one target. No account, API service, database, model, or AWS resource is created here.

The versioned HTTP contract is [openapi.json](openapi.json), using [OpenAPI 3.1](https://spec.openapis.org/oas/v3.1.0.html). Shared data definitions are in [schemas/data.schema.json](schemas/data.schema.json), using [JSON Schema 2020-12](https://json-schema.org/draft/2020-12/json-schema-validation). Examples and validation run entirely against local files. A reference checker enforces relationships and timing that JSON Schema alone cannot express.

## Run from a fresh clone

Use Python 3.10 for the checked dependency lock. Keep this environment separate from research training:

```bash
python3.10 -m venv .venv-contracts
.venv-contracts/bin/python -m pip install -r requirements-contracts.lock
.venv-contracts/bin/python -m contracts.check
.venv-contracts/bin/python -m pytest contracts/tests -q
```

If the existing research environment is already installed, `.venv/bin/python -m venv .venv-contracts` also creates the separate environment. Installing dependencies requires package-index access; after installation, the checks need no network, AWS credentials, Docker, Spark, PyTorch, ignored research data, or model checkpoints. `requirements-contracts.txt` records direct dependencies; `requirements-contracts.lock` pins their resolved versions for the tested Python 3.10 environment.

To retain reproducible sample files for the next checkpoint:

```bash
.venv-contracts/bin/python -m contracts.fixtures.generate --output data/platform-fixtures
.venv-contracts/bin/python -m contracts.check --fixtures data/platform-fixtures
```

The generator writes two project directories, each with a manifest, `items.csv`, `events.csv`, `users.csv`, and an auxiliary `media.json`. Generated files stay under ignored `data/`; CSV files are not silently committed. An identical rerun is safe; different existing output is refused. Choose a new directory to change the seed or size. It stages and validates complete output before publishing it locally. This local directory operation does not establish S3 publication semantics.

## Supported fixture

- Seed `7101`; two distinct project/dataset identities with deliberately colliding external user/item/event IDs to support later isolation tests.
- Per project: 100 synthetic users, the eight distinct bundled playable videos, and 10,000 synthetic impressions plus linked clicks/watch events.
- Fixed event period October 1–12, 2026; train starts October 1, validation October 8, test October 10, test ends October 12. Extraction watermark: October 15, 2026, UTC.
- Synthetic publication and receipt timestamps are identified as synthetic. Metrics on these fixtures cannot establish real-user relevance, historical replay fidelity, or engagement lift.
- The default fixture must pass the eligibility floor: at least 1,000 mature impressions overall and 20 examples of each class in every split. A smaller diagnostic fixture can validate while reporting `eligible: false`.

Eight distinct existing clips replace the planning candidate of roughly 100 playable items. This avoids requiring an ignored catalog database, network import, or repeated video assets disguised as distinct catalog content. [fixtures/catalog.json](fixtures/catalog.json) connects the item IDs to local playback/poster paths, byte digests, source pages, and existing license references. It does not copy the old preview's fictional creators or engagement counts. Existing media evidence remains in [MEDIA.md](../frontend/assets/MEDIA.md).

## Data and event meaning

Uploads use UTF-8 CSV with a header. Required files are `items.csv` and `events.csv`; `users.csv` is optional. Media metadata is a separate demo catalog, not an additional customer-upload format. File inventory records full-byte SHA-256, byte lengths, and row counts. ETags are not accepted as file digests.

External identifiers are case-sensitive strings matching `[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`. Leading zeros are preserved. Internal resources use a typed prefix and 32 lowercase hexadecimal characters, for example `prj_`, `dts_`, `job_`, and `mdl_`. External IDs are project-local, and an ID in a body never grants project access.

CSV booleans are exactly `true`/`false`. Numeric duration fields are nonnegative decimal integers. Unknown columns/properties, invalid timestamps, conflicting duplicates, missing item/user references, and mismatched impressions fail with safe field/row details. Identical duplicates are counted and deduplicated. Online events omit `received_at`; the server assigns it. Offline events may include it under the manifest's declared availability mode.

For an impression at time `t`, `click-v1` means:

| Rule | Frozen behavior |
|---|---|
| Attribution | A matching click occurs strictly after `t` and at or before `t + 1,800 seconds` |
| On-time arrival | Receipt is at or before that event's own occurrence plus 86,400 seconds |
| Late events | Retained and counted, excluded from supervised labels and reference histories |
| Mature exposure | `t + 1,800 + 86,400 seconds <= extraction watermark` |
| Negative label | A mature, observed impression with no qualifying click; unexposed items are never negatives |
| Splits | Chronological half-open intervals; purge attribution windows that touch or cross the next boundary |
| Available history | Occurrence strictly before request time; receipt at or before the pinned snapshot cutoff |
| Equal timestamps | Stable event-ID ordering; events at the prediction instant remain excluded |

Only the click event's actual availability contributes a prior click; a prior impression does not expose its future outcome. A snapshot cutoff cannot follow request time. Imported events without receipt times use `event_time_only` and cannot certify arrival replay. See [the decision record](../docs/phase-7/decisions/0001-platform-v1.md) for the intended snapshot publication protocol.

The browser exposure rule is at least 50% of the card visible for one continuous second while the document is visible and media is playable. A recommendation response or failed media load is not an impression. Clicks require the recorded exposure identity. Watch duration accumulates only visible, actively playing time; paused/background intervals do not count, and a watch event carries `watch_ms` bounded by the known clip duration. Browser implementation and verification belong to 7.9.

## HTTP and permissions

The OpenAPI operations carry `x-permissions` for owner, viewer, integration, demo, or public access. This is an executable contract inventory, not proof that authentication is implemented. The [access-pattern and permission tables](../docs/phase-7/access-patterns.md) define the corresponding enforcement targets.

Dashboard credentials require verified JWT identity plus project membership. Integration keys are project-bound with `catalog:write`, `events:write`, and/or `recommendations:read`; they cannot upload datasets, train, or deploy. Demo credentials bind to one server-selected project and session. Only health and demo-session creation are public.

Authentication failures return `401`; a verified caller outside the project gets `404`; a known project member lacking the operation's permission gets `403`. Malformed input is `400`, state/idempotency conflict `409`, oversized content `413`, throttling `429`, and unavailable inference `503`. Responses carry request IDs and safe errors, without secrets or arbitrary server paths. Unknown request fields and query parameters are rejected. List cursors bind to route, project, and filters; expired or mismatched cursors fail with `400`.

Mutations marked with the required `Idempotency-Key` header retain canonical route/project/actor/payload identity for seven days. A same-payload replay returns the original status/resource; a different payload returns `409`. Key issuance is an explicit response exception: replay returns the existing masked summary with `secret: null`, rather than retaining plaintext to reveal it again. Expired keys do not guarantee replay. Upload-target refresh uses a new idempotency key to obtain replacement URLs for the same pending dataset/file identities; replaying an old key does not extend an expired URL. Event deduplication retains `(project_id, event_id)` and its canonical payload digest for at least 30 days; the same ID with a conflicting payload fails. This is logical deduplication, not exactly-once transport. TTL is eventual cleanup, not an authorization or retry guarantee.

## Scope and limits

The first API accepts at most 100 events/items per batch, 256 KiB request JSON, list pages of 20 by default and 100 maximum, 200 ranking candidates, and 20 returned items. The data checker permits at most 100,000 rows per CSV file and 50 MiB of uploaded CSV bytes in aggregate. Manifest/auxiliary metadata are separately bounded. HTTP body-size checks, quotas, cursor signing, storage transactions, and actual authorization are later implementation work.

Cloud limits remain one active expensive workflow, a tiny CPU model of at most 10 MiB, and one short recording session followed by teardown. The user-authorized credit maximum is $100, aiming comfortably below it; the provisional conservative AWS allowance is $20, with estimates refreshed before deployment. The separate unallocated buffer under the overall ceiling is defined in the [budget runbook](../docs/phase-7/budget-and-recording.md).

Research pipeline/transformation, training, catalog, and historical-preview interfaces remain in their existing folders. The checkpoint's [scope and regression inventory](../docs/phase-7/scope-and-limits.md) records what must keep working. Nothing here maps a playable clip to an old KuaiRand embedding or claims a live personalized feed.
