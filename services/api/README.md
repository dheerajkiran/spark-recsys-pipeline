# Local API foundation

From the repository root, run `npm ci`, then set `LOOP_DATA_ROOT` to an absolute writable directory and run `npm run dev`. `LOOP_HOST` defaults to `127.0.0.1` and `LOOP_PORT` to `4100`. The server rejects startup with a clear message if the data root is missing or relative. `GET /health` is the only public route; `/v1/*` returns the contract's structured `401 unauthenticated` response until identity is implemented in checkpoint 7.3.

The domain function in `src/application.ts` is shared by the local Node HTTP adapter and the API Gateway HTTP API payload 2.0 Lambda adapter. Both return JSON and `X-Request-ID`. The Lambda handler is an adapter factory; deployment wiring is deferred to the AWS phase.

`LocalStorage` persists job metadata as JSON and artifacts by SHA-256 under `LOOP_DATA_ROOT`. `LocalWorkflowRunner` exercises queued → running → succeeded/failed transitions with one active job per process. This is a single-process development implementation: it does not provide cross-process compare-and-swap, distributed leases, retries, DynamoDB consistency, or Step Functions semantics. Those must be proven before replacing it in the cloud.
