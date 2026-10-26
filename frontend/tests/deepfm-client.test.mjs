import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { request } from 'node:http';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createDeepFMClient } from '../deepfm-client.mjs';
import { createStaticServer } from '../server.mjs';

let fixture, workerPath;
before(async () => {
  fixture = await mkdtemp(path.join(tmpdir(), 'loop-model-bridge-'));
  workerPath = path.join(fixture, 'worker.mjs');
  await writeFile(workerPath, `
    import { createInterface } from 'node:readline';
    const lines = createInterface({ input: process.stdin });
    lines.on('line', line => {
      const { id, method, profile_id } = JSON.parse(line);
      const send = result => console.log(JSON.stringify({ id, result }));
      if (method === 'status') return send({ state: 'ready', model: 'DeepFM', pid: process.pid });
      if (method === 'profiles') return send([{ id: '1', name: 'Profile 1', example_count: 8 }]);
      if (profile_id === '3') return;
      if (profile_id === '4') return console.log('not-json');
      if (profile_id === '5') return process.exit(1);
      if (profile_id === '6') return console.log(JSON.stringify({ id, error: { code: 'unknown_profile', message: 'Choose an available profile.' } }));
      if (profile_id === '7') return console.log(JSON.stringify({ id, surprise: true }));
      setTimeout(() => send({ profile_id, pid: process.pid }), profile_id === '1' ? 35 : 5);
    });
  `);
});
after(async () => { if (fixture) await rm(fixture, { recursive: true, force: true }); });

function client(t, options = {}) {
  const value = createDeepFMClient({ pythonPath: process.execPath, workerPath, timeoutMs: 2000, ...options });
  t.after(() => value.close());
  return value;
}

test('lazily reuses one worker and matches concurrent out-of-order responses', async t => {
  const value = client(t);
  const [first, second, status] = await Promise.all([
    value.request('score', { profile_id: '1' }), value.request('score', { profile_id: '2' }), value.request('status'),
  ]);
  assert.equal(first.profile_id, '1');
  assert.equal(second.profile_id, '2');
  assert.equal(first.pid, second.pid);
  assert.equal(status.pid, first.pid);
  assert.deepEqual(await value.request('profiles'), [{ id: '1', name: 'Profile 1', example_count: 8 }]);
  assert.equal((await value.request('status')).pid, first.pid);
});

test('missing interpreter fails cleanly instead of hanging', async t => {
  const value = client(t, { pythonPath: path.join(fixture, 'missing-python') });
  await assert.rejects(value.request('status'), { code: 'model_unavailable' });
});

test('one timeout rejects queued requests and allows a fresh worker on retry', async t => {
  const value = client(t, { timeoutMs: 100 });
  const initial = await value.request('status');
  const requests = [value.request('score', { profile_id: '3' }), value.request('score', { profile_id: '3' })];
  const results = await Promise.allSettled(requests);
  assert.ok(results.every(result => result.status === 'rejected' && result.reason.code === 'model_unavailable'));
  assert.notEqual((await value.request('status')).pid, initial.pid);
});

for (const [profile, description] of [['4', 'malformed JSON'], ['5', 'early worker exit'], ['7', 'invalid protocol']]) {
  test(`${description} is an explicit transport failure`, async t => {
    const value = client(t);
    await assert.rejects(value.request('score', { profile_id: profile }), { code: 'model_unavailable' });
  });
}

test('backend validation errors preserve their code without destroying the worker', async t => {
  const value = client(t);
  await assert.rejects(value.request('score', { profile_id: '6' }), { code: 'unknown_profile' });
  assert.equal((await value.request('status')).state, 'ready');
});

test('closing rejects pending work and all later requests', async t => {
  const value = client(t);
  const pending = value.request('score', { profile_id: '3' });
  const rejected = assert.rejects(pending, { code: 'client_closed' });
  value.close(); value.close();
  await rejected;
  await assert.rejects(value.request('status'), { code: 'client_closed' });
});

test('unsupported commands and unsafe profile IDs never reach the worker', async t => {
  const value = client(t, { pythonPath: path.join(fixture, 'missing-python') });
  for (const [method, params] of [['evaluate_test', {}], ['score', {}], ['score', null], ['score', { profile_id: 12 }], ['score', { profile_id: '../gold' }], ['score', { profile_id: '1'.repeat(21) }]]) {
    await assert.rejects(value.request(method, params), { code: 'invalid_request' });
  }
});

async function withServer(t, deepfmClient) {
  const server = createStaticServer(fixture, { deepfmClient });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  t.after(() => new Promise(resolve => server.close(resolve)));
  const port = server.address().port;
  return (urlPath, method = 'GET') => new Promise((resolve, reject) => {
    const req = request({ hostname: '127.0.0.1', port, path: urlPath, method }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString();
        resolve({ status: res.statusCode, headers: res.headers, body: text ? JSON.parse(text) : undefined });
      });
    });
    req.on('error', reject); req.end();
  });
}

test('HTTP routes expose only the three read-only methods with the documented shape', async t => {
  const calls = [];
  const fetch = await withServer(t, { request: async (method, params) => {
    calls.push([method, params]);
    if (method === 'profiles') return [{ id: '12', name: 'Profile 1', example_count: 8 }];
    return method === 'status' ? { state: 'ready', model: 'DeepFM' } : { profile_id: params.profile_id, examples: [] };
  } });
  const status = await fetch('/api/deepfm/status');
  assert.equal(status.status, 200);
  assert.equal(status.headers['content-type'], 'application/json; charset=utf-8');
  assert.equal(status.headers['cache-control'], 'no-store');
  assert.deepEqual(status.body, { state: 'ready', model: 'DeepFM' });
  assert.deepEqual((await fetch('/api/deepfm/profiles')).body, { profiles: [{ id: '12', name: 'Profile 1', example_count: 8 }] });
  assert.deepEqual((await fetch('/api/deepfm/score?profile_id=12')).body, { profile_id: '12', examples: [] });
  assert.deepEqual(calls, [['status', {}], ['profiles', {}], ['score', { profile_id: '12' }]]);
});

test('invalid query fields, repeated profile IDs and write methods never invoke inference', async t => {
  let called = false;
  const fetch = await withServer(t, { request: async () => { called = true; return {}; } });
  for (const urlPath of ['/api/deepfm/score', '/api/deepfm/score?profile_id=../test', '/api/deepfm/score?profile_id=1&profile_id=2', '/api/deepfm/score?profile_id=1&features=label', '/api/deepfm/status?path=private', '/api/deepfm/profiles?limit=999']) {
    assert.equal((await fetch(urlPath)).status, 400, urlPath);
  }
  assert.equal((await fetch('/api/deepfm/checkpoints')).status, 404);
  for (const method of ['POST', 'DELETE', 'HEAD']) {
    const response = await fetch('/api/deepfm/score?profile_id=1', method);
    assert.equal(response.status, 405);
    assert.equal(response.headers.allow, 'GET');
  }
  assert.equal(called, false);
});

test('unavailable status remains readable; inference errors use safe, useful HTTP errors', async t => {
  const fetch = await withServer(t, { request: async method => {
    if (method === 'status') return { state: 'unavailable', model: 'DeepFM', message: 'Checkpoint not available.' };
    throw Object.assign(new Error('/private/local/path'), { code: method === 'score' ? 'unknown_profile' : 'model_unavailable' });
  } });
  assert.equal((await fetch('/api/deepfm/status')).body.state, 'unavailable');
  const unavailable = await fetch('/api/deepfm/profiles');
  assert.equal(unavailable.status, 503);
  assert.equal(unavailable.body.error.code, 'model_unavailable');
  assert.doesNotMatch(JSON.stringify(unavailable.body), /private\/local/);
  const unknown = await fetch('/api/deepfm/score?profile_id=99');
  assert.equal(unknown.status, 404);
  assert.equal(unknown.body.error.code, 'unknown_profile');
});
