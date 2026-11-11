import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { exportJWK, generateKeyPair, SignJWT } from 'jose';
import { createApplication, type ApiResponse } from '../src/application.js';
import { validateError } from '../src/api-contract.js';
import { DependencyUnavailable, type ErrorLog } from '../src/failure.js';
import { LocalIdentityStore, type IdentityStore } from '../src/identity-store.js';
import type { Config } from '../src/config.js';

const projectId = `prj_${'1'.repeat(32)}`;
const path = `/v1/projects/${projectId}`;
const config: Config = {
  host: '127.0.0.1', port: 4100, dataRoot: '/tmp/loop-failure-tests',
  auth: { mode: 'local', issuer: 'http://loop.local', clientId: 'loop-local', requiredScopes: ['loop/api'], jwksPath: '/tmp/unused-public-jwks' },
  cursorSecret: 'test-only-cursor-key-at-least-32-bytes',
};
const verifier = { async verify() { return { kind: 'dashboard' as const, subject: 'http://loop.local#tester' }; } };
const credential = 'Bearer private-test-credential';
const request = { method: 'GET', path, query: '', headers: { authorization: credential }, body: Buffer.alloc(0) };
function checkFailure(response: ApiResponse, status: number, logs: ErrorLog[]) {
  assert.equal(response.status, status);
  const body = JSON.parse(response.body) as { code: string; request_id: string };
  assert.equal(body.code, status === 500 ? 'internal_error' : 'unavailable');
  assert.equal(validateError(body), true);
  assert.equal(logs.length, 1);
  assert.equal(logs[0]?.request_id, response.headers['x-request-id']);
  assert.equal(body.request_id, logs[0]?.request_id);
  assert.ok(logs[0]?.stack.includes('application-failures.test'));
  assert.deepEqual(Object.keys(logs[0]!).sort(), ['error_name', 'level', 'request_id', 'stack']);
  assert.ok(!JSON.stringify(logs).includes(credential));
  assert.ok(!response.body.includes(credential));
}

test('unexpected application bugs return 500 with a correlated stack and no request data', async () => {
  const logs: ErrorLog[] = [];
  const failure = new TypeError(`Bad input from ${credential}\nprivate-body-data`);
  Object.assign(failure, { headers: { authorization: credential }, body: 'private-body-data' });
  const store: IdentityStore = {
    async get() { throw failure; }, async query() { return []; }, async transact() {},
  };
  const app = createApplication(config, { store, verifier, logger: (entry) => logs.push(entry) });
  checkFailure(await app(request), 500, logs);
  assert.equal(logs[0]?.error_name, 'TypeError');
  assert.ok(!JSON.stringify(logs).includes('private-body-data'));
});

test('an invalid successful response is reported as a code failure rather than an outage', async () => {
  const logs: ErrorLog[] = [];
  const store: IdentityStore = {
    async get(key) {
      return { ...key, revision: 1, data: key.sk.startsWith('MEMBER#')
        ? { role: 'owner' } : { name: 'Broken response', created_at: 'invalid timestamp' } };
    }, async query() { return []; }, async transact() {},
  };
  const app = createApplication(config, { store, verifier, logger: (entry) => logs.push(entry) });
  const result = await app(request);
  assert.equal(result.status, 500);
  assert.equal((JSON.parse(result.body) as { code: string }).code, 'internal_error');
  assert.equal(logs.length, 1);
  assert.equal(logs[0]?.request_id, result.headers['x-request-id']);
  assert.ok(logs[0]?.stack.includes('api-contract.ts'));
  assert.ok(!result.body.includes('Broken response'));
});

test('only explicitly classified dependency failures return 503 and retain an observable stack', async () => {
  const logs: ErrorLog[] = [];
  const failure = new DependencyUnavailable('Identity backend unavailable');
  const store: IdentityStore = {
    async get() { throw failure; }, async query() { return []; }, async transact() {},
  };
  const app = createApplication(config, { store, verifier, logger: (entry) => logs.push(entry) });
  checkFailure(await app(request), 503, logs);
  assert.equal(logs[0]?.error_name, 'DependencyUnavailable');
});

test('invalid credentials and expected validation failures do not produce internal-error logs', async () => {
  const logs: ErrorLog[] = [];
  const app = createApplication(config, { verifier, logger: (entry) => logs.push(entry) });
  assert.equal((await app({ ...request, headers: {} })).status, 401);
  assert.equal((await app({ ...request, path: '/v1/projects', query: 'unknown=true' })).status, 400);
  assert.deepEqual(logs, []);
});

test('failed lazy verifier setup can recover on the next request without restarting the application', async (t) => {
  const root = await mkdtemp(join(tmpdir(), 'loop-verifier-recovery-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const jwksPath = join(root, 'public-jwks.json');
  const localConfig: Config = { ...config, dataRoot: root, auth: { ...config.auth as Extract<NonNullable<Config['auth']>, { mode: 'local' }>, jwksPath } };
  const pair = await generateKeyPair('RS256');
  const token = await new SignJWT({ username: 'tester', token_use: 'access', client_id: 'loop-local', scope: 'loop/api' })
    .setProtectedHeader({ alg: 'RS256', kid: 'recovery' }).setIssuer('http://loop.local').setSubject('tester')
    .setIssuedAt().setExpirationTime('1h').sign(pair.privateKey);
  const logs: ErrorLog[] = [];
  const app = createApplication(localConfig, { store: new LocalIdentityStore(root), logger: (entry) => logs.push(entry) });
  const query = { ...request, path: '/v1/projects', headers: { authorization: `Bearer ${token}` } };
  assert.equal((await app(query)).status, 500);
  assert.equal(logs.length, 1);
  await writeFile(jwksPath, JSON.stringify({ keys: [{ ...await exportJWK(pair.publicKey), kid: 'recovery', alg: 'RS256' }] }));
  const recovered = await app(query);
  assert.equal(recovered.status, 200);
  assert.deepEqual((JSON.parse(recovered.body) as { items: unknown[] }).items, []);
  assert.equal(logs.length, 1);
});
