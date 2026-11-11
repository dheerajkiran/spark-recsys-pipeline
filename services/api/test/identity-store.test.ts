import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import { promisify } from 'node:util';
import { LocalIdentityStore, StoreConflict, type StoreRecord, type StoreWrite } from '../src/identity-store.js';
import { DependencyUnavailable } from '../src/failure.js';

const execute = promisify(execFile);
const item = (pk: string, sk: string, revision = 1, name = 'sample'): StoreRecord => ({ pk, sk, revision, data: { name } });
const put = (record: StoreRecord, expectedRevision: number | null = null): StoreWrite => ({ key: record, expectedRevision, record });

test('identity snapshots preserve project-local collisions and survive new instances', async () => {
  const root = await mkdtemp(join(tmpdir(), 'loop-identity-'));
  try {
    const store = new LocalIdentityStore(root);
    assert.equal(await store.get({ pk: 'PROJECT#a', sk: 'ITEM#same' }), null);
    await store.transact([
      put(item('PROJECT#a', 'ITEM#same', 1, 'A')),
      put(item('PROJECT#b', 'ITEM#same', 1, 'B')),
      put(item('PROJECT#a', 'KEY#one')),
    ]);
    const reopened = new LocalIdentityStore(root);
    assert.equal((await reopened.get({ pk: 'PROJECT#b', sk: 'ITEM#same' }))?.data.name, 'B');
    assert.deepEqual((await reopened.query('PROJECT#a', 'ITEM#')).map((record) => record.data.name), ['A']);
    assert.equal((await stat(join(root, 'identity', 'records.json'))).mode & 0o777, 0o600);
    assert.equal((await stat(join(root, 'identity'))).mode & 0o777, 0o700);
    const result = await reopened.get({ pk: 'PROJECT#a', sk: 'KEY#one' });
    if (!result) throw new Error('Missing test record');
    result.data.name = 'unpersisted';
    assert.equal((await store.get(result))?.data.name, 'sample');
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('a failed conditional identity transaction changes no records', async () => {
  const root = await mkdtemp(join(tmpdir(), 'loop-identity-'));
  try {
    const store = new LocalIdentityStore(root);
    const a = item('PROJECT#a', 'META');
    await store.transact([put(a)]);
    await assert.rejects(store.transact([
      put(item('PROJECT#a', 'MEMBER#alice')),
      put(item(a.pk, a.sk, 10), 9),
    ]), StoreConflict);
    assert.equal(await store.get({ pk: a.pk, sk: 'MEMBER#alice' }), null);
    assert.equal((await store.get(a))?.revision, 1);
    await store.transact([put(item(a.pk, a.sk, 2, 'updated'), 1)]);
    assert.equal((await store.get(a))?.data.name, 'updated');
    await assert.rejects(store.transact([{ key: a, expectedRevision: 1, record: null }]), StoreConflict);
    await store.transact([{ key: a, expectedRevision: 2, record: null }]);
    assert.equal(await store.get(a), null);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('concurrent identity transactions across instances have a single conditional winner', async () => {
  const root = await mkdtemp(join(tmpdir(), 'loop-identity-'));
  try {
    const record = item('PROJECT#a', 'META');
    const outcomes = await Promise.allSettled(Array.from({ length: 8 }, () => new LocalIdentityStore(root).transact([put(record)])));
    assert.equal(outcomes.filter((outcome) => outcome.status === 'fulfilled').length, 1);
    assert.equal(outcomes.filter((outcome) => outcome.status === 'rejected'
      && outcome.reason instanceof StoreConflict).length, 7);
    assert.equal((await new LocalIdentityStore(root).query('PROJECT#a', 'META')).length, 1);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('separate processes cannot overwrite one another during snapshot publication', async () => {
  const root = await mkdtemp(join(tmpdir(), 'loop-identity-'));
  try {
    const modulePath = new URL('../src/identity-store.ts', import.meta.url).href;
    const script = `
      const { LocalIdentityStore } = await import(process.argv[1]);
      const store = new LocalIdentityStore(process.argv[2]);
      for (let index = 0; index < 6; index++) {
        const record = { pk: 'PROJECT#' + process.argv[3], sk: 'KEY#' + index, revision: 1, data: { index } };
        await store.transact([{ key: record, expectedRevision: null, record }]);
      }
    `;
    await Promise.all(['a', 'b'].map((identity) => execute(process.execPath,
      ['--import', 'tsx', '--input-type=module', '-e', script, modulePath, root, identity])));
    const store = new LocalIdentityStore(root);
    assert.equal((await store.query('PROJECT#a', 'KEY#')).length, 6);
    assert.equal((await store.query('PROJECT#b', 'KEY#')).length, 6);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('live or stale identity locks fail closed and are never automatically removed', async () => {
  const root = await mkdtemp(join(tmpdir(), 'loop-identity-'));
  try {
    const store = new LocalIdentityStore(root, 30);
    const record = item('PROJECT#a', 'META');
    await store.transact([put(record)]);
    const lock = join(root, 'identity', 'write.lock');
    await writeFile(lock, 'operator-owned-lock', { flag: 'wx' });
    await assert.rejects(store.transact([put(item('PROJECT#a', 'KEY#new'))]), (error: unknown) =>
      error instanceof DependencyUnavailable && error.message.includes('locked')
      && (error.cause as NodeJS.ErrnoException).code === 'EEXIST');
    assert.equal(await readFile(lock, 'utf8'), 'operator-owned-lock');
    assert.equal(await store.get({ pk: 'PROJECT#a', sk: 'KEY#new' }), null);
    assert.equal((await store.get(record))?.revision, 1);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('filesystem I/O failures are typed dependencies with their original errno cause', async () => {
  const root = await mkdtemp(join(tmpdir(), 'loop-identity-'));
  try {
    const blocked = join(root, 'file-instead-of-directory');
    await writeFile(blocked, 'not a directory');
    const store = new LocalIdentityStore(blocked);
    const dependency = (error: unknown) => error instanceof DependencyUnavailable
      && error.cause instanceof Error && (error.cause as NodeJS.ErrnoException).code === 'ENOTDIR';
    await assert.rejects(store.get({ pk: 'PROJECT#a', sk: 'META' }), dependency);
    await assert.rejects(store.query('PROJECT#a', 'KEY#'), dependency);
    await assert.rejects(store.transact([put(item('PROJECT#a', 'META'))]), dependency);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('corrupt snapshots and programmer errors retain their failure type instead of becoming dependencies', async () => {
  const root = await mkdtemp(join(tmpdir(), 'loop-identity-'));
  try {
    const store = new LocalIdentityStore(root);
    const record = item('PROJECT#a', 'META');
    await store.transact([put(record)]);
    const snapshot = join(root, 'identity', 'records.json');
    await writeFile(snapshot, '{');
    await assert.rejects(store.get(record), SyntaxError);
    await assert.rejects(store.transact([put(item('PROJECT#b', 'META'))]), SyntaxError);
    await writeFile(snapshot, JSON.stringify({ format: 1, records: [{ ...record, revision: -1 }] }));
    await assert.rejects(store.query('PROJECT#a', 'META'), (error: unknown) =>
      error instanceof Error && !(error instanceof DependencyUnavailable) && /Invalid identity record/.test(error.message));
    await assert.rejects(store.transact([put({ ...record, data: { unsupported: 1n } })]), TypeError);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test('identity adapters reject duplicate keys and revision or key mismatches before writes', async () => {
  const root = await mkdtemp(join(tmpdir(), 'loop-identity-'));
  try {
    const store = new LocalIdentityStore(root);
    const record = item('PROJECT#a', 'META');
    await assert.rejects(store.transact([put(record), put(record)]), /Duplicate/);
    await assert.rejects(store.transact([put({ ...record, sk: 'wrong' }, 1)]), /revision/);
    await assert.rejects(store.transact([{ ...put(record), key: { ...record, pk: 'PROJECT#b' } }]), /key/);
    assert.equal(await store.get(record), null);
  } finally { await rm(root, { recursive: true, force: true }); }
});
