# ADR 0001 — First platform contract and bounded demonstration

Date: November 10, 2026. Status: accepted for checkpoint 7.1; service behavior remains to be implemented.

## Context and decision

The research pipeline and saved KuaiRand results are complete. The local Loop preview has eight bundled playable videos, a separately imported catalog, and historical DeepFM inference over unrelated item IDs. Phase 7 needs a compatible item/event path, observable APIs, isolation, and reproducible local evidence before using AWS.

Keep React/TypeScript/Vite for the new frontend, modular Node/TypeScript application logic with local/Lambda adapters, and Python/PyTorch for CPU training and ranking. Use the existing models through a new platform feature specification; retain the original KuaiRand entry points. The cloud demonstration uses private S3, DynamoDB on-demand, API Gateway HTTP API, Cognito, Lambda, Step Functions, and one bounded Glue job. These choices are design decisions; installation, adapters, infrastructure, and runtime feasibility belong to later checkpoints.

Freeze the HTTP interface in `contracts/openapi.json` and data definitions in `contracts/schemas/data.schema.json`. JSON Schema checks structural input; the Python reference checker specifies cross-row identities, availability, labels, and split boundaries. The reference code is not a substitute for later Spark/online parity tests against independent implementations.

## Project identity and permissions

An account is an authenticated human principal; projects are the ownership boundary. A project has owner and viewer memberships. Both can read permitted project state; only owners mutate control-plane state. Integration keys have only catalog-write, event-write, and recommendation-read scopes. They cannot upload datasets, start jobs, manage memberships/keys, or deploy models. The demo uses a server-selected project with session-only recommendation/event authority.

Resolve and verify principal membership or key/session binding before constructing project-prefixed storage keys. Return `404` for inaccessible projects/resources and `403` for an existing member's insufficient role. Every child resource must be looked up inside that authorized project. The first release uses shared tables and function roles; it does not claim IAM isolation for each tenant.

## Labels and event availability

The objective is a click after an observed impression. Freeze `click-v1` with attribution `(t, t + 30 minutes]`, accepted receipt lateness of at most 24 hours measured from each event's own occurrence, and maturity at `t + 30 minutes + 24 hours <= extraction watermark`. Reject impossible relationships; retain late events with counts but exclude them from supervised labels/reference histories. Purge exposures whose attribution window touches or crosses a split boundary. Fit preprocessing only on training data in 7.5; final test data does not select or promote models.

For feature history, use an event only if its occurrence precedes the request and its receipt is at or before the captured snapshot cutoff. A click's later arrival cannot be inferred from its earlier impression. Tie-break histories by event time and event ID. The initial browser exposure rule requires half the card visible for a continuous second in a visible document with playable media. Record watch duration only during visible active playback and bound it by clip duration. Those browser rules are specified here and will be implemented in 7.9.

Uploaded receipt timestamps are source claims, not server-certified historical facts. The seeded fixture's receipt timestamps are synthetic and explicitly labelled. Inputs without receipt times use `event_time_only` and cannot prove what a historical online system knew.

## Coherent feature snapshots

Choose a bounded immutable snapshot for the initial demonstration. A snapshot has project ID, feature-policy/version, immutable snapshot ID/digest, an availability cutoff, and source event/export identities. A snapshot is published only when its complete small catalog/history/aggregate inventory is ready. A conditional write then advances a project-scoped pointer. Event outbox delivery may arrive out of order; it cannot mutate already published snapshots.

A recommendation resolves one snapshot and deployment generation at request start, then reads only those identities for every candidate. New accepted events become eligible for a later explicitly published snapshot. Requests never mix mutable per-user and per-item summaries from different update progress. The snapshot cutoff cannot exceed request time. Later implementations must test partial publication, duplicate delivery, stale pointers, and concurrent promotion. This checkpoint fixes the protocol; it does not implement DynamoDB snapshot publication.

This introduces explicit feature staleness and bounded snapshot rebuilding. That is acceptable for the recording workload and easier to verify than claiming transactionally consistent streaming aggregates. Continually operated production may need incremental snapshots, larger history stores, and measured freshness guarantees; those require a separate decision and workload.

## Fixture and media

Use two synthetic projects, each with 100 users, eight distinct playable bundled item IDs, and 10,000 observed impressions plus linked clicks/watches, generated from seed 7101. Preserve colliding external IDs across projects so later authorization tests cannot accidentally depend on global uniqueness. The two projects have different internal identities and generated interactions.

Reduce the earlier candidate of roughly 100 playable items to the eight real clips already present. A separate catalog manifest contains local media/poster paths, byte digests, source links, and existing attribution references. The model dataset uses the same item IDs without fictional preview creators or engagement statistics. This keeps a fresh clone reproducible with no external import. A larger licensed catalog is an explicit fixture change, not an undocumented local dependency.

Fixed historical dates mature the labels without waiting during recording. Every default split must meet the published class floor. Evaluation remains synthetic workflow evidence; no user engagement, commercial readiness, or production model quality is implied.

## Operational bounds and alternatives

Keep one active expensive workflow, strict API/batch/data limits, seven-day HTTP idempotency records, and 30-day logical event deduplication. Dataset publication uses immutable S3 inventories plus registered completion manifests in later checkpoints. A local directory rename is only a fixture-generation convenience.

The reported $100 credit balance is available to this project, with instructions to spend comfortably below it. Maintain the cost worksheet, buffer, Free-plan verification, short deployment, and teardown. Credit headroom is not a reason to add idle compute, public training, GPUs, or uncontrolled retries. No cloud deployment occurs in 7.1.

Arbitrary input schemas, uploaded Python/model executables, arbitrary URLs/paths, continuously hosted service, production-scale capacity, and commercial tenancy guarantees remain deferred. The contract freezes the supported path rather than treating those capabilities as implicit.
