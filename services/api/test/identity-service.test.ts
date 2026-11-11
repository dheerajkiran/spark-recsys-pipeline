import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { ApiError } from '../src/api-error.js';
import { IdentityService, hash, projectPk, type IdentityContext, type Scope } from '../src/identity-service.js';
import { LocalIdentityStore } from '../src/identity-store.js';
import { paginate, readCursor } from '../src/pagination.js';

const DAY = 86400000;
const owner = 'http://loop.local#sensitive-owner';
const viewer = 'http://loop.local#sensitive-viewer';
const foreign = 'http://loop.local#sensitive-foreign';
const request = (subject = owner): IdentityContext => ({ subject, requestId: 'req_12345678901234567890123456789012' });
const status = (expected: number) => (error: unknown) => error instanceof ApiError && error.status === expected;

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'loop-identity-domain-'));
  let now = Date.parse('2026-11-11T12:00:00Z');
  const store = new LocalIdentityStore(root);
  const service = new IdentityService(store, () => now);
  return {
    root, store, service, now: () => now, advance: (milliseconds: number) => { now += milliseconds; },
    cleanup: () => rm(root, { recursive: true, force: true }),
    keyInput: (scopes: Scope[] = ['recommendations:read']) => ({ name: 'integration', scopes, expires_at: new Date(now + DAY).toISOString() }),
  };
}

test('project creation is idempotent under concurrent retries and binds payload and actor', async () => {
  const f = await fixture();
  try {
    const results = await Promise.all(Array.from({ length: 6 }, () => f.service.createProject(request(), 'Owner project', 'same-project-request')));
    assert.equal(new Set(results.map((project) => project.project_id)).size, 1);
    assert.equal((await f.service.listProjects(owner)).items.length, 1);
    const [project] = results;
    assert.ok(project);
    assert.equal(project.role, 'owner');
    await assert.rejects(f.service.createProject(request(), 'Changed', 'same-project-request'), status(409));
    const other = await f.service.createProject(request(foreign), 'Owner project', 'same-project-request');
    assert.notEqual(other.project_id, project.project_id);
    assert.equal((await f.service.listProjects(foreign)).items.length, 1);
    await assert.rejects(f.service.getProject(project.project_id, foreign), status(404));
    f.advance(7 * DAY);
    const expiredReplay = await f.service.createProject(request(), 'New request after retention', 'same-project-request');
    assert.notEqual(expiredReplay.project_id, project.project_id);
  } finally { await f.cleanup(); }
});

test('membership establishes viewer visibility without granting owner operations or foreign access', async () => {
  const f = await fixture();
  try {
    const project = await f.service.createProject(request(), 'Protected', 'create-project');
    await f.service.setMembership(project.project_id, viewer, 'viewer');
    assert.equal((await f.service.getProject(project.project_id, viewer)).role, 'viewer');
    assert.deepEqual((await f.service.listProjects(viewer)).items.map((item) => item.project_id), [project.project_id]);
    assert.equal(await f.service.authorize(project.project_id, { kind: 'dashboard', subject: viewer }, ['owner', 'viewer'], []), 'viewer');
    await assert.rejects(f.service.authorize(project.project_id, { kind: 'dashboard', subject: viewer }, ['owner'], []), status(403));
    await assert.rejects(f.service.authorize(project.project_id, { kind: 'dashboard', subject: foreign }, ['owner', 'viewer'], []), status(404));
    await assert.rejects(f.service.issueKey(request(viewer), project.project_id, f.keyInput(), 'viewer-key'), status(403));
    await assert.rejects(f.service.setMembership(project.project_id, owner, 'viewer'), status(409));
    const ownProject = await f.service.createProject(request(viewer), 'Viewer creates own project', 'viewer-create');
    assert.equal(ownProject.role, 'owner');
    assert.equal((await f.service.getProject(project.project_id, viewer)).role, 'viewer');
  } finally { await f.cleanup(); }
});

test('concurrent API key issuance reveals exactly one secret and replay never persists or repeats it', async () => {
  const f = await fixture();
  try {
    const project = await f.service.createProject(request(), 'Keys', 'project');
    const input = f.keyInput(['events:write', 'recommendations:read']);
    const results = await Promise.all(Array.from({ length: 6 }, () => f.service.issueKey(request(), project.project_id, input, 'key-issuance')));
    assert.equal(new Set(results.map((result) => result.key.key_id)).size, 1);
    const revealed = results.filter((result) => result.secret !== null);
    assert.equal(revealed.length, 1);
    const issued = revealed[0];
    assert.ok(issued?.secret);
    const verified = await f.service.verifyKey(issued.secret);
    assert.equal(verified.projectId, project.project_id);
    assert.deepEqual(verified.scopes, ['events:write', 'recommendations:read']);
    const replay = await f.service.issueKey(request(), project.project_id, { ...input }, 'key-issuance');
    assert.equal(replay.secret, null);
    assert.deepEqual(replay.key, issued.key);
    assert.equal((await f.service.listKeys(project.project_id)).items.length, 1);
    await assert.rejects(f.service.issueKey(request(), project.project_id, { ...input, name: 'different' }, 'key-issuance'), status(409));
    const lookup = await f.store.get({ pk: `KEY#${issued.key.key_id}`, sk: 'LOOKUP' });
    assert.equal(lookup?.data.digest, hash(issued.secret));
    const persisted = await readFile(join(f.root, 'identity', 'records.json'), 'utf8');
    assert.equal(persisted.includes(issued.secret), false);
    assert.equal(persisted.includes(owner), false);
    assert.equal(persisted.includes('key-issuance'), false);
    assert.equal(persisted.includes('Bearer '), false);
    const audit = await f.store.query(projectPk(project.project_id), 'AUDIT#');
    assert.deepEqual(audit.map((event) => event.data.action).sort(), ['key.issued', 'project.created']);
    assert.ok(audit.every((event) => event.data.actor === hash(owner)));
    assert.ok(audit.every((event) => event.data.request_id === request().requestId));
  } finally { await f.cleanup(); }
});

test('key verification enforces digest, project, scopes, expiry and immediate revocation on a new request', async () => {
  const f = await fixture();
  try {
    const project = await f.service.createProject(request(), 'Project A', 'project-a');
    const other = await f.service.createProject(request(foreign), 'Project B', 'project-b');
    const issued = await f.service.issueKey(request(), project.project_id, f.keyInput(), 'key-a');
    assert.ok(issued.secret);
    const identity = await f.service.verifyKey(issued.secret);
    assert.equal(await f.service.authorize(project.project_id, identity, ['integration'], ['recommendations:read']), 'integration');
    await assert.rejects(f.service.authorize(project.project_id, identity, ['integration'], ['events:write']), status(403));
    await assert.rejects(f.service.authorize(project.project_id, identity, ['owner'], []), status(403));
    await assert.rejects(f.service.authorize(other.project_id, identity, ['integration'], ['recommendations:read']), status(404));
    await assert.rejects(f.service.revokeKey(request(foreign), other.project_id, issued.key.key_id, 'foreign-revoke'), status(404));
    const changed = `${issued.secret.slice(0, -1)}${issued.secret.endsWith('A') ? 'B' : 'A'}`;
    await assert.rejects(f.service.verifyKey(changed), status(401));
    await assert.rejects(f.service.verifyKey('loop_key_00000000000000000000000000000000.' + 'A'.repeat(43)), status(401));
    const revoked = await f.service.revokeKey(request(), project.project_id, issued.key.key_id, 'revoke-a');
    assert.equal(revoked.revoked, true);
    assert.deepEqual(await f.service.revokeKey(request(), project.project_id, issued.key.key_id, 'revoke-a'), revoked);
    await assert.rejects(f.service.verifyKey(issued.secret), status(401));
    assert.equal((await f.service.issueKey(request(), project.project_id, f.keyInput(), 'key-a')).secret, null);
    assert.equal((await f.service.listKeys(project.project_id)).items[0]?.revoked, true);
    const expires = await f.service.issueKey(request(), project.project_id, f.keyInput(), 'expires');
    assert.ok(expires.secret);
    f.advance(DAY);
    await assert.rejects(f.service.verifyKey(expires.secret), status(401));
  } finally { await f.cleanup(); }
});

test('key expiry boundaries are checked before any credential is persisted', async () => {
  const f = await fixture();
  try {
    const project = await f.service.createProject(request(), 'Expiry', 'project');
    for (const expires_at of ['invalid', new Date(f.now()).toISOString(), new Date(f.now() - 1).toISOString(), new Date(f.now() + 30 * DAY + 1).toISOString()]) {
      await assert.rejects(f.service.issueKey(request(), project.project_id, { ...f.keyInput(), expires_at }, 'expiry-check'), status(400));
    }
    assert.equal((await f.service.listKeys(project.project_id)).items.length, 0);
    const max = await f.service.issueKey(request(), project.project_id, { ...f.keyInput(), expires_at: new Date(f.now() + 30 * DAY).toISOString() }, 'expiry-check');
    assert.ok(max.secret);
    assert.equal((await f.service.verifyKey(max.secret)).projectId, project.project_id);
  } finally { await f.cleanup(); }
});

test('project and key cursors preserve insertion snapshots even when wall-clock timestamps collide', async () => {
  const f = await fixture();
  try {
    const a = await f.service.createProject(request(), 'A', 'a');
    const b = await f.service.createProject(request(), 'B', 'b');
    const c = await f.service.createProject(request(), 'C', 'c');
    assert.equal(a.created_at, c.created_at);
    const context = { secret: 'a'.repeat(32), route: '/v1/projects', actor: owner, now: f.now() };
    const initial = await f.service.listProjects(owner);
    const first = paginate(initial.items, 1, initial.snapshot, 0, context);
    assert.equal(first.items[0]?.project_id, a.project_id);
    assert.ok(first.next_cursor);
    const inserted = await f.service.createProject(request(), 'Inserted later, same instant', 'later');
    const cursor = readCursor(first.next_cursor, context.secret, context.route, context.actor, f.now());
    const continuation = await f.service.listProjects(owner, cursor.snapshot);
    const rest = paginate(continuation.items, 100, continuation.snapshot, cursor.after, context);
    assert.deepEqual(rest.items.map((project) => project.project_id), [b.project_id, c.project_id]);
    assert.equal(rest.items.some((project) => project.project_id === inserted.project_id), false);
    assert.equal('sequence' in rest.items[0]!, false);
    const keys = await Promise.all(['k1', 'k2', 'k3'].map((key) => f.service.issueKey(request(), a.project_id, f.keyInput(), key)));
    const keySnapshot = await f.service.listKeys(a.project_id);
    const laterKey = await f.service.issueKey(request(), a.project_id, f.keyInput(), 'k4');
    const continuedKeys = await f.service.listKeys(a.project_id, keySnapshot.snapshot);
    assert.deepEqual(new Set(continuedKeys.items.map((key) => key.key_id)), new Set(keys.map((key) => key.key.key_id)));
    assert.equal(continuedKeys.items.some((key) => key.key_id === laterKey.key.key_id), false);
  } finally { await f.cleanup(); }
});

test('opaque cursors reject tampering, wrong actor/project/route, expiry and secret rotation', () => {
  const context = { secret: 'cursor-secret-'.repeat(3), route: '/v1/projects/prj_a/api-keys', actor: owner, now: 1000000 };
  const first = paginate([{ sequence: 1, value: 'first' }, { sequence: 2, value: 'second' }], 1, 2, 0, context);
  assert.ok(first.next_cursor);
  assert.deepEqual(readCursor(first.next_cursor, context.secret, context.route, owner, context.now), { snapshot: 2, after: 1 });
  const bytes = Buffer.from(first.next_cursor, 'base64url');
  bytes[bytes.length - 1] = bytes[bytes.length - 1]! ^ 1;
  assert.throws(() => readCursor(bytes.toString('base64url'), context.secret, context.route, owner, context.now), status(400));
  for (const [secret, route, actor, now] of [
    [context.secret, '/v1/projects/prj_b/api-keys', owner, context.now],
    [context.secret, '/v1/projects', owner, context.now],
    [context.secret, context.route, foreign, context.now],
    [context.secret, context.route, owner, context.now + 15 * 60000],
    ['rotated'.repeat(6), context.route, owner, context.now],
  ] as const) assert.throws(() => readCursor(first.next_cursor, secret, route, actor, now), status(400));
  assert.equal(Buffer.from(first.next_cursor, 'base64url').includes(Buffer.from(owner)), false);
});
