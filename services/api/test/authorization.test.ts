import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test, type TestContext } from 'node:test';
import { CreateTableCommand, DeleteTableCommand, DynamoDBClient, waitUntilTableExists } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { exportJWK, generateKeyPair, SignJWT } from 'jose';
import { createApplication, type ApiResponse, type Application } from '../src/application.js';
import { matchRoute, validateError, validateOutput } from '../src/api-contract.js';
import { DynamoIdentityStore } from '../src/dynamo-identity-store.js';
import { IdentityService, projectPk, type KeySummary, type Project, type Scope } from '../src/identity-service.js';
import { LocalIdentityStore, type IdentityStore } from '../src/identity-store.js';

const contract = JSON.parse(await readFile(new URL('../../../contracts/openapi.json', import.meta.url), 'utf8')) as {
  paths: Record<string, Record<string, { operationId: string; 'x-permissions': { actors: string[]; integration_scopes: Scope[] } }>>;
};
const examples = JSON.parse(await readFile(new URL('../../../contracts/examples/examples.json', import.meta.url), 'utf8')) as {
  kind: string; method: string; path: string; value: Record<string, unknown>;
}[];
const pair = await generateKeyPair('RS256');
const allScopes: Scope[] = ['catalog:write', 'events:write', 'recommendations:read'];
type Message = Record<string, unknown> & {
  project: Project; key: KeySummary; secret: string | null;
  items: (Project | KeySummary)[]; next_cursor: string | null;
};

function body(response: ApiResponse): Message { return JSON.parse(response.body) as Message; }
async function call(app: Application, method: string, path: string, headers: Record<string, string> = {}, value?: Record<string, unknown>, query = '') {
  return app({ method, path, query,
    headers: { ...(value ? { 'content-type': 'application/json' } : {}), ...headers },
    body: value ? Buffer.from(JSON.stringify(value)) : Buffer.alloc(0),
  });
}
function check(response: ApiResponse, status: number, secrets: string[] = []) {
  assert.equal(response.status, status, response.body);
  assert.equal(body(response).request_id, response.headers['x-request-id']);
  assert.equal(response.headers['cache-control'], 'no-store');
  if (status >= 400) {
    assert.equal(validateError(body(response)), true, response.body);
    assert.deepEqual(body(response).details, []);
  }
  for (const secret of secrets) assert.ok(!response.body.includes(secret), 'Credential must not appear in a response');
}
function successSchema(method: string, path: string, response: ApiResponse) {
  const route = matchRoute(method, path);
  assert.ok(route);
  validateOutput(route.operation, response.status, body(response));
}
async function signed(subject: string) {
  return new SignJWT({ username: subject, client_id: 'loop-local', token_use: 'access', scope: 'loop/api openid' })
    .setProtectedHeader({ alg: 'RS256', kid: 'authorization-test' }).setIssuer('http://loop.local')
    .setSubject(subject).setIssuedAt().setExpirationTime('1h').sign(pair.privateKey);
}
async function fixture(t: TestContext, customStore?: IdentityStore) {
  const root = await mkdtemp(join(tmpdir(), 'loop-authorization-'));
  t.after(async () => { await rm(root, { recursive: true, force: true }); });
  const jwksPath = join(root, 'jwks.json');
  await writeFile(jwksPath, JSON.stringify({ keys: [{ ...await exportJWK(pair.publicKey), kid: 'authorization-test', alg: 'RS256', use: 'sig' }] }));
  const store = customStore ?? new LocalIdentityStore(root);
  let clock = Date.now();
  const now = () => clock;
  const app = createApplication({
    host: '127.0.0.1', port: 4100, dataRoot: root,
    auth: { mode: 'local', issuer: 'http://loop.local', clientId: 'loop-local', requiredScopes: ['loop/api'], jwksPath },
    cursorSecret: 'test-cursor-secret-with-at-least-32-bytes',
  }, { store, now });
  const service = new IdentityService(store, now);
  const tokens = { alice: await signed('alice'), bob: await signed('bob'), viewer: await signed('viewer') };
  const alice = { authorization: `Bearer ${tokens.alice}` };
  const bob = { authorization: `Bearer ${tokens.bob}` };
  const viewer = { authorization: `Bearer ${tokens.viewer}` };
  const create = async (headers: Record<string, string>, name: string, idem = randomUUID()) => {
    const response = await call(app, 'POST', '/v1/projects', { ...headers, 'idempotency-key': idem }, { name });
    check(response, 201);
    successSchema('POST', '/v1/projects', response);
    return body(response).project;
  };
  const project = await create(alice, 'Alice project');
  const foreign = await create(bob, 'Bob project');
  await service.setMembership(project.project_id, 'http://loop.local#viewer', 'viewer');
  const issue = async (scopes = allScopes, idempotency = randomUUID(), expires = new Date(clock + 86400000).toISOString()) => {
    const path = `/v1/projects/${project.project_id}/api-keys`;
    const input = { name: 'Integration test', scopes, expires_at: expires };
    const response = await call(app, 'POST', path, { ...alice, 'idempotency-key': idempotency }, input);
    check(response, 201);
    successSchema('POST', path, response);
    return { response, input, path, idempotency, key: body(response).key, secret: body(response).secret as string };
  };
  return { root, store, service, app, tokens, alice, bob, viewer, project, foreign, create, issue, now, advance: (ms: number) => { clock += ms; } };
}

function requestValue(template: string, method: string, expiry: string): Record<string, unknown> | undefined {
  if (method === 'get') return undefined;
  if (template.endsWith('/api-keys')) return { name: 'Matrix key', scopes: ['recommendations:read'], expires_at: expiry };
  if (template.endsWith('/deployments/rollback')) return { model_version: `mdl_${'1'.repeat(32)}`, expected_generation: 0 };
  return examples.find((entry) => entry.kind === 'request' && entry.path === template && entry.method === method)?.value ?? {};
}

test('every project operation enforces anonymous, foreign-project, viewer, and integration boundaries from OpenAPI', async (t) => {
  const f = await fixture(t);
  const full = await f.issue();
  const revokeTarget = await f.issue(['recommendations:read']);
  const knownIds: Record<string, string> = {
    project_id: f.project.project_id, dataset_id: `dts_${'1'.repeat(32)}`, job_id: `job_${'2'.repeat(32)}`,
    model_version: `mdl_${'3'.repeat(32)}`, deployment_id: `dep_${'4'.repeat(32)}`, key_id: revokeTarget.key.key_id,
  };
  let checked = 0;
  for (const [template, methods] of Object.entries(contract.paths)) {
    if (!template.includes('{project_id}')) continue;
    const path = template.replace(/\{([^}]+)\}/g, (_match, name: string) => knownIds[name] ?? 'unknown');
    for (const [method, operation] of Object.entries(methods)) {
      const headers = { 'idempotency-key': randomUUID() };
      const value = requestValue(template, method, new Date(f.now() + 86400000).toISOString());
      const invoke = (credential: Record<string, string>, input = value) => call(f.app, method.toUpperCase(), path, { ...headers, ...credential }, input);
      check(await invoke({}), 401);
      check(await invoke(f.bob), 404, [f.tokens.bob]);
      const permissions = operation['x-permissions'];
      const viewerStatus = !permissions.actors.includes('viewer') ? 403 : operation.operationId === 'get_project' ? 200 : 503;
      check(await invoke(f.viewer), viewerStatus, [f.tokens.viewer]);
      const integrationStatus = permissions.actors.includes('integration') ? 503 : 403;
      check(await invoke({ 'x-api-key': full.secret }), integrationStatus, [full.secret]);
      const wrongProjectPath = path.replace(f.project.project_id, f.foreign.project_id);
      check(await call(f.app, method.toUpperCase(), wrongProjectPath, { ...headers, 'x-api-key': full.secret }, value), 404, [full.secret]);
      if (permissions.actors.includes('integration')) {
        const missing = await f.issue(allScopes.filter((scope) => !permissions.integration_scopes.includes(scope)));
        check(await invoke({ 'x-api-key': missing.secret }), 403, [missing.secret]);
      }
      const ownerExpected = operation.operationId === 'issue_key' ? 201
        : ['get_project', 'list_keys', 'revoke_key'].includes(operation.operationId) ? 200 : 503;
      const ownerResponse = await invoke(f.alice);
      check(ownerResponse, ownerExpected, [f.tokens.alice]);
      if (ownerExpected < 400) successSchema(method.toUpperCase(), path, ownerResponse);
      checked++;
    }
  }
  assert.equal(checked, 23, 'All project-bound contract operations must be included');
});

test('project creation and developer credential lifecycle preserve idempotency, one-time secrets, and immediate revocation', async (t) => {
  const f = await fixture(t);
  const idem = randomUUID();
  const created = await f.create(f.alice, 'Repeated project', idem);
  assert.equal((await f.create(f.alice, 'Repeated project', idem)).project_id, created.project_id);
  check(await call(f.app, 'POST', '/v1/projects', { ...f.alice, 'idempotency-key': idem }, { name: 'Different payload' }), 409);
  const issued = await f.issue(['recommendations:read']);
  assert.match(issued.secret, /^loop_key_[0-9a-f]{32}\.[A-Za-z0-9_-]{43}$/);
  const replay = await call(f.app, 'POST', issued.path, { ...f.alice, 'idempotency-key': issued.idempotency }, issued.input);
  check(replay, 201, [issued.secret]);
  assert.equal(body(replay).secret, null);
  assert.equal(body(replay).key.key_id, issued.key.key_id);
  const conflicting = await call(f.app, 'POST', issued.path, { ...f.alice, 'idempotency-key': issued.idempotency }, { ...issued.input, name: 'Changed' });
  check(conflicting, 409, [issued.secret]);
  const recommendation = `/v1/projects/${f.project.project_id}/recommendations`;
  check(await call(f.app, 'POST', recommendation, { 'x-api-key': issued.secret }, { user_id: 'same-user', limit: 5, context: { surface: 'home' } }), 503, [issued.secret]);
  const listed = await call(f.app, 'GET', issued.path, f.alice);
  check(listed, 200, [issued.secret, f.tokens.alice]);
  assert.deepEqual(body(listed).items, [issued.key]);
  successSchema('GET', issued.path, listed);
  const revokePath = `${issued.path}/${issued.key.key_id}/revoke`;
  const revokeHeaders = { ...f.alice, 'idempotency-key': randomUUID() };
  const revoked = await call(f.app, 'POST', revokePath, revokeHeaders, {});
  check(revoked, 200, [issued.secret]);
  assert.equal(body(revoked).key.revoked, true);
  successSchema('POST', revokePath, revoked);
  check(await call(f.app, 'POST', revokePath, revokeHeaders, {}), 200);
  check(await call(f.app, 'POST', recommendation, { 'x-api-key': issued.secret }, { user_id: 'same-user' }), 401, [issued.secret]);
  const replayRevoked = await call(f.app, 'POST', issued.path, { ...f.alice, 'idempotency-key': issued.idempotency }, issued.input);
  check(replayRevoked, 201, [issued.secret]);
  assert.equal(body(replayRevoked).secret, null);
  assert.equal(body(replayRevoked).key.revoked, true);
  const snapshot = await readFile(join(f.root, 'identity', 'records.json'), 'utf8');
  for (const credential of [issued.secret, ...Object.values(f.tokens)]) assert.ok(!snapshot.includes(credential));
  const audit = await f.store.query(projectPk(f.project.project_id), 'AUDIT#');
  assert.deepEqual(audit.map((record) => record.data.action).sort(), ['key.issued', 'key.revoked', 'membership.updated', 'project.created'].sort());
  assert.ok(audit.every((record) => typeof record.data.actor === 'string' && /^[0-9a-f]{64}$/.test(record.data.actor)));
});

test('expired or invalid keys, mixed credentials, and demo credentials fail with safe contract errors', async (t) => {
  const f = await fixture(t);
  const key = await f.issue();
  const path = `/v1/projects/${f.project.project_id}/recommendations`;
  const data = { user_id: 'same-user' };
  for (const headers of [
    { 'x-api-key': 'invalid-key-secret' },
    { 'x-api-key': `${key.secret.slice(0, -1)}${key.secret.endsWith('A') ? 'B' : 'A'}` },
    { ...f.alice, 'x-api-key': key.secret },
    { ...f.alice, 'x-demo-session': 'opaque-demo-secret' },
    { 'x-api-key': key.secret, 'x-demo-session': 'opaque-demo-secret' },
    { 'x-demo-session': 'opaque-demo-secret' },
    { authorization: 'Bearer unsigned-or-malformed-token' },
  ]) check(await call(f.app, 'POST', path, headers, data), 401, [key.secret, f.tokens.alice, 'opaque-demo-secret']);
  f.advance(86400001);
  check(await call(f.app, 'POST', path, { 'x-api-key': key.secret }, data), 401, [key.secret]);
  const demo = await call(f.app, 'POST', '/v1/demo/sessions', {}, {});
  check(demo, 503);
  for (const demoPath of ['/v1/demo/recommendations', '/v1/demo/events']) {
    check(await call(f.app, 'POST', demoPath, { 'x-api-key': key.secret }, {}), 401);
  }
});

test('strict input validation rejects caller-selected project, storage, workflow and artifact fields before future work', async (t) => {
  const f = await fixture(t);
  const base = `/v1/projects/${f.project.project_id}`;
  for (const template of ['/v1/projects/{project_id}/datasets', '/v1/projects/{project_id}/training-jobs', '/v1/projects/{project_id}/deployments', '/v1/projects/{project_id}/recommendations']) {
    const good = requestValue(template, 'post', '') ?? {};
    for (const field of ['project_id', 's3_key', 'workflow_arn', 'artifact_path']) {
      const path = template.replace('/v1/projects/{project_id}', base);
      check(await call(f.app, 'POST', path, { ...f.alice, 'idempotency-key': randomUUID() }, { ...good, [field]: field === 'project_id' ? f.foreign.project_id : '/private/arbitrary/secret' }), 400, [f.tokens.alice]);
    }
  }
  for (const expires of [new Date(f.now() - 1).toISOString(), new Date(f.now() + 31 * 86400000).toISOString()]) {
    check(await call(f.app, 'POST', `${base}/api-keys`, { ...f.alice, 'idempotency-key': randomUUID() }, { name: 'Bad expiry', scopes: ['recommendations:read'], expires_at: expires }), 400);
  }
  check(await call(f.app, 'POST', `${base}/api-keys`, { ...f.alice, 'idempotency-key': randomUUID() }, { name: 'Bad scope', scopes: ['training:write'], expires_at: new Date(f.now() + 100000).toISOString() }), 400);
  check(await call(f.app, 'POST', '/v1/projects', f.alice, { name: 'No idempotency key' }), 400);
  check(await call(f.app, 'GET', '/v1/projects', f.alice, undefined, 'unknown=true'), 400);
  check(await call(f.app, 'GET', '/v1/projects', f.alice, undefined, 'limit=1&limit=2'), 400);
  check(await call(f.app, 'GET', '/v1/projects/prj_invalid', f.alice), 400);
});

test('project and key pagination bind cursors to principal, route and snapshot and reject tampering or expiry', async (t) => {
  const f = await fixture(t);
  const extra = await f.create(f.alice, 'Second Alice project');
  const first = await call(f.app, 'GET', '/v1/projects', f.alice, undefined, 'limit=1');
  check(first, 200);
  const cursor = body(first).next_cursor;
  assert.ok(cursor);
  const late = await f.create(f.alice, 'Late insert');
  const page = await call(f.app, 'GET', '/v1/projects', f.alice, undefined, new URLSearchParams({ limit: '10', cursor }).toString());
  check(page, 200);
  assert.deepEqual(body(page).items.map((item) => (item as Project).project_id), [extra.project_id]);
  assert.ok(!body(page).items.some((item) => (item as Project).project_id === late.project_id));
  assert.equal(body(page).next_cursor, null);
  check(await call(f.app, 'GET', '/v1/projects', f.bob, undefined, new URLSearchParams({ cursor }).toString()), 400);
  check(await call(f.app, 'GET', `/v1/projects/${f.project.project_id}/api-keys`, f.alice, undefined, new URLSearchParams({ cursor }).toString()), 400);
  check(await call(f.app, 'GET', '/v1/projects', f.alice, undefined, new URLSearchParams({ cursor: `${cursor.slice(0, -2)}AA` }).toString()), 400);
  await f.issue();
  await f.issue(['events:write']);
  const keysPage = await call(f.app, 'GET', `/v1/projects/${f.project.project_id}/api-keys`, f.alice, undefined, 'limit=1');
  check(keysPage, 200);
  const keysCursor = body(keysPage).next_cursor;
  assert.ok(keysCursor);
  check(await call(f.app, 'GET', `/v1/projects/${extra.project_id}/api-keys`, f.alice, undefined, new URLSearchParams({ cursor: keysCursor }).toString()), 400);
  f.advance(15 * 60000 + 1);
  check(await call(f.app, 'GET', '/v1/projects', f.alice, undefined, new URLSearchParams({ cursor }).toString()), 400);
});

const endpoint = process.env.LOOP_DYNAMODB_TEST_ENDPOINT;
test('DynamoDB Local persists actual project/key transactions, idempotent replay and revocation through the application', { skip: !endpoint }, async (t) => {
  const url = new URL(endpoint!);
  assert.ok(url.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname), 'DynamoDB test endpoint must be local');
  const low = new DynamoDBClient({ endpoint: url.href, region: 'us-east-1', credentials: { accessKeyId: 'local', secretAccessKey: 'local' } });
  const table = `loop-authorization-test-${randomUUID()}`;
  t.after(async () => {
    await low.send(new DeleteTableCommand({ TableName: table })).catch(() => undefined);
    low.destroy();
  });
  await low.send(new CreateTableCommand({
    TableName: table, BillingMode: 'PAY_PER_REQUEST',
    AttributeDefinitions: [{ AttributeName: 'PK', AttributeType: 'S' }, { AttributeName: 'SK', AttributeType: 'S' }],
    KeySchema: [{ AttributeName: 'PK', KeyType: 'HASH' }, { AttributeName: 'SK', KeyType: 'RANGE' }],
  }));
  await waitUntilTableExists({ client: low, maxWaitTime: 20, minDelay: 1 }, { TableName: table });
  const f = await fixture(t, new DynamoIdentityStore(DynamoDBDocumentClient.from(low), table));
  const idem = randomUUID();
  const project = await f.create(f.alice, 'Dynamo replay', idem);
  assert.equal((await f.create(f.alice, 'Dynamo replay', idem)).project_id, project.project_id);
  const key = await f.issue(['recommendations:read']);
  const replay = await call(f.app, 'POST', key.path, { ...f.alice, 'idempotency-key': key.idempotency }, key.input);
  check(replay, 201);
  assert.equal(body(replay).secret, null);
  assert.equal(body(replay).key.key_id, key.key.key_id);
  const path = `/v1/projects/${f.project.project_id}/recommendations`;
  check(await call(f.app, 'POST', path, { 'x-api-key': key.secret }, { user_id: 'same-user', limit: 5 }), 503);
  check(await call(f.app, 'POST', path.replace(f.project.project_id, f.foreign.project_id), { 'x-api-key': key.secret }, { user_id: 'same-user' }), 404);
  const revoked = await call(f.app, 'POST', `${key.path}/${key.key.key_id}/revoke`, { ...f.alice, 'idempotency-key': randomUUID() }, {});
  check(revoked, 200);
  assert.equal(body(revoked).key.revoked, true);
  check(await call(f.app, 'POST', path, { 'x-api-key': key.secret }, { user_id: 'same-user' }), 401);
});
