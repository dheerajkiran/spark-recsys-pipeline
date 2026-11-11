import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { createApplication } from '../src/application.js';
import { loadConfig } from '../src/config.js';
import { createHttpServer } from '../src/http.js';
import { createLambdaHandler } from '../src/lambda.js';
import { LocalStorage } from '../src/local-storage.js';
import { LocalWorkflowRunner } from '../src/local-workflow.js';

const config = { host: '127.0.0.1', port: 4100, dataRoot: '/tmp/loop-test' };
const app = createApplication(config);
const event = (path: string) => ({ version: '2.0' as const, rawPath: path, requestContext: { http: { method: 'GET' } } });

test('config requires an explicit local data root and valid port', () => {
  assert.throws(() => loadConfig({}), /LOOP_DATA_ROOT/);
  assert.throws(() => loadConfig({ LOOP_DATA_ROOT: 'relative' }), /absolute/);
  assert.throws(() => loadConfig({ LOOP_DATA_ROOT: '/tmp/x', LOOP_PORT: '99999' }), /LOOP_PORT/);
  assert.equal(loadConfig({ LOOP_DATA_ROOT: '/tmp/x' }).port, 4100);
});
test('Lambda adapter returns the same health and errors as domain application', async () => {
  const lambda = createLambdaHandler(app);
  for (const path of ['/health', '/v1/projects', '/missing']) {
    const direct = await app({ method: 'GET', path, query: '', headers: {}, body: Buffer.alloc(0) });
    const adapted = await lambda(event(path));
    assert.equal(adapted.statusCode, direct.status);
    const body = JSON.parse(adapted.body) as Record<string, unknown>;
    assert.deepEqual(Object.keys(body).sort(), Object.keys(JSON.parse(direct.body) as object).sort());
    assert.equal(body.request_id, adapted.headers['x-request-id']);
  }
  assert.equal((await lambda({ ...event('/health'), rawQueryString: 'unexpected=1' })).statusCode, 400);
  assert.equal((await lambda({ ...event('/health'), body: 'x'.repeat(262145) })).statusCode, 413);
});
test('local HTTP adapter serializes health and closes protected routes', async () => {
  const server = createHttpServer(app).listen(0, '127.0.0.1');
  await new Promise<void>((resolve) => server.once('listening', resolve));
  try {
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('No socket');
    const base = `http://127.0.0.1:${address.port}`;
    const health = await fetch(`${base}/health`);
    assert.equal(health.status, 200);
    assert.equal((await health.json() as { request_id: string }).request_id, health.headers.get('x-request-id'));
    const protectedResponse = await fetch(`${base}/v1/projects`);
    assert.equal(protectedResponse.status, 401);
  } finally { server.close(); }
});
test('local job transitions and content addressed artifacts persist', async () => {
  const root = await mkdtemp(join(tmpdir(), 'loop-storage-'));
  try {
    const storage = new LocalStorage(root);
    const job = await storage.createJob();
    assert.equal((await storage.getJob(job.id))?.state, 'queued');
    const runner = new LocalWorkflowRunner(storage);
    const finished = await runner.run(async () => { await storage.putArtifact(Buffer.from('sample')); });
    assert.equal((await storage.getJob(finished.id))?.state, 'succeeded');
    await assert.rejects(storage.transitionJob(finished.id, 'succeeded', 'running'), /Invalid/);
    const artifact = await storage.putArtifact(Buffer.from('sample'));
    assert.equal((await storage.getArtifact(artifact.sha256)).toString(), 'sample');
  } finally { await rm(root, { recursive: true, force: true }); }
});
