import assert from 'node:assert/strict';
import { once } from 'node:events';
import { request, type Server } from 'node:http';
import { test } from 'node:test';
import type { ApiRequest, ApiResponse } from '../src/application.js';
import { createHttpServer } from '../src/http.js';
import { createLambdaHandler, type HttpApiEvent } from '../src/lambda.js';

type Handler = (input: ApiRequest) => ApiResponse | Promise<ApiResponse>;
const success = (): ApiResponse => ({ status: 200, headers: { 'content-type': 'application/json' }, body: '{"ok":true}' });
const event = (extra: Partial<HttpApiEvent> = {}): HttpApiEvent => ({
  version: '2.0', rawPath: '/health', requestContext: { http: { method: 'GET' } }, ...extra,
});

async function withServer(handler: Handler, run: (port: number, server: Server) => Promise<void>): Promise<void> {
  const server = createHttpServer(handler).listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('No local socket');
  try { await run(address.port, server); }
  finally {
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
}

function send(port: number, path: string, chunks: Buffer[] = [], headers: Record<string, string | string[]> = {}) {
  return new Promise<{ status: number; body: Record<string, unknown>; requestId: string | undefined }>((resolve, reject) => {
    const outgoing = request({ hostname: '127.0.0.1', port, path, method: chunks.length ? 'POST' : 'GET', headers }, (incoming) => {
      const received: Buffer[] = [];
      incoming.on('data', (chunk: Buffer) => received.push(chunk));
      incoming.on('end', () => {
        resolve({ status: incoming.statusCode ?? 0, body: JSON.parse(Buffer.concat(received).toString()) as Record<string, unknown>, requestId: incoming.headers['x-request-id'] as string | undefined });
      });
      incoming.on('error', reject);
    });
    outgoing.on('error', reject);
    for (const chunk of chunks) outgoing.write(chunk);
    outgoing.end();
  });
}

test('HTTP awaits asynchronous applications and keeps their response', async () => {
  await withServer(async () => { await Promise.resolve(); return success(); }, async (port) => {
    assert.deepEqual((await send(port, '/health')).body, { ok: true });
  });
});

test('HTTP rejects an oversized streamed body before dispatch and remains usable', async () => {
  const sizes: number[] = [];
  await withServer(async (input) => { sizes.push(input.body.byteLength); return success(); }, async (port) => {
    const oversized = await send(port, '/v1/projects', [...Array<Buffer>(8).fill(Buffer.alloc(65536)), Buffer.from('x')]);
    assert.equal(oversized.status, 413);
    assert.equal(oversized.body.code, 'payload_too_large');
    assert.equal(oversized.body.request_id, oversized.requestId);
    assert.deepEqual(oversized.body.details, []);
    assert.deepEqual(sizes, []);
    assert.equal((await send(port, '/health')).status, 200);
    assert.deepEqual(sizes, [0]);
  });
});

test('HTTP permits a body exactly at the documented byte limit', async () => {
  let observed = 0;
  await withServer(async (input) => { observed = input.body.byteLength; return success(); }, async (port) => {
    assert.equal((await send(port, '/v1/projects', [Buffer.alloc(262144)])).status, 200);
    assert.equal(observed, 262144);
  });
});

test('HTTP rejects malformed targets and duplicate credentials without crashing', async () => {
  let calls = 0;
  await withServer(async () => { calls++; return success(); }, async (port) => {
    for (const target of ['//[/', 'http://[broken', '/health%', '/health%00', '/health#fragment', '/\\health']) {
      const result = await send(port, target);
      assert.equal(result.status, 400, target);
      assert.equal(result.body.code, 'invalid_request');
      assert.deepEqual(result.body.details, []);
    }
    assert.equal((await send(port, '/health', [], { authorization: ['Bearer first', 'Bearer second'] })).status, 400);
    assert.equal(calls, 0);
    assert.equal((await send(port, '/health')).status, 200);
    assert.equal(calls, 1);
  });
});

test('HTTP converts asynchronous failures to a sanitized error response', async () => {
  await withServer(async () => { throw new Error('Bearer private-secret /private/file'); }, async (port) => {
    const result = await send(port, '/health');
    assert.equal(result.status, 500);
    assert.equal(result.body.code, 'internal_error');
    assert.equal(result.body.request_id, result.requestId);
    assert.deepEqual(result.body.details, []);
    assert.doesNotMatch(JSON.stringify(result.body), /private-secret|private\/file/);
  });
});

test('Lambda awaits its application and normalizes header names', async () => {
  const seen: ApiRequest[] = [];
  const lambda = createLambdaHandler(async (input) => { seen.push(input); return success(); });
  const result = await lambda(event({ headers: { Authorization: 'Bearer example' }, body: Buffer.from('example').toString('base64'), isBase64Encoded: true }));
  assert.equal(result.statusCode, 200);
  assert.equal(seen[0]?.headers.authorization, 'Bearer example');
  assert.equal(Buffer.from(seen[0]?.body ?? []).toString(), 'example');
});

test('Lambda enforces the decoded byte limit without dispatching oversized bodies', async () => {
  const sizes: number[] = [];
  const lambda = createLambdaHandler(async (input) => { sizes.push(input.body.byteLength); return success(); });
  assert.equal((await lambda(event({ body: 'é'.repeat(131073) }))).statusCode, 413);
  assert.equal((await lambda(event({ body: Buffer.alloc(262145).toString('base64'), isBase64Encoded: true }))).statusCode, 413);
  assert.deepEqual(sizes, []);
  assert.equal((await lambda(event({ body: Buffer.alloc(262144).toString('base64'), isBase64Encoded: true }))).statusCode, 200);
  assert.deepEqual(sizes, [262144]);
});

test('Lambda rejects malformed base64, paths and duplicate normalized headers', async () => {
  let calls = 0;
  const lambda = createLambdaHandler(async () => { calls++; return success(); });
  for (const body of ['a', '!!!!', 'abcd====', 'ab=c', 'AB==', 'a b=']) {
    const result = await lambda(event({ body, isBase64Encoded: true }));
    assert.equal(result.statusCode, 400, body);
    assert.equal((JSON.parse(result.body) as { code: string }).code, 'invalid_request');
  }
  for (const rawPath of ['//[/', 'https://api.example/health', '/health?query', '/health#fragment', '/health%', '/health%00']) {
    assert.equal((await lambda(event({ rawPath }))).statusCode, 400);
  }
  assert.equal((await lambda(event({ headers: { Authorization: 'Bearer first', authorization: 'Bearer second' } }))).statusCode, 400);
  assert.equal(calls, 0);
});

test('Lambda returns safe structured errors if its application rejects', async () => {
  const lambda = createLambdaHandler(async () => { throw new Error('private-secret'); });
  const result = await lambda(event());
  const body = JSON.parse(result.body) as Record<string, unknown>;
  assert.equal(result.statusCode, 500);
  assert.equal(body.code, 'internal_error');
  assert.equal(body.request_id, result.headers['x-request-id']);
  assert.deepEqual(body.details, []);
  assert.doesNotMatch(result.body, /private-secret/);
});
