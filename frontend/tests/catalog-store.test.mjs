import assert from 'node:assert/strict';
import { test } from 'node:test';
import { existsSync } from 'node:fs';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { request } from 'node:http';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { createCatalogStore } from '../catalog-store.mjs';
import { createStaticServer } from '../server.mjs';

const TOPICS = ['Nature', 'Travel', 'Food', 'Design', 'Science'];
function post(index, overrides = {}) {
  return { id: `commons:${index}`, creator: `Creator ${index % 3}`, handle: `commons-author-${index % 3}`,
    initials: `C${index % 3}`, avatarColor: '#aabbcc', title: `Clip ${index}`, description: `A scene from collection ${index % 4}.`,
    tags: [`topic${index % 4}`], category: TOPICS[(index - 1) % TOPICS.length],
    video: `https://upload.wikimedia.org/wikipedia/commons/a/ab/Clip_${index}.webm`,
    poster: `https://upload.wikimedia.org/wikipedia/commons/thumb/a/ab/Clip_${index}.webm/480px--Clip_${index}.webm.jpg`,
    likes: 0, comments: [], duration: 20, reason: 'A licensed catalog item.', location: '',
    source: { provider: 'wikimedia', id: String(index), url: `https://commons.wikimedia.org/wiki/File:Clip_${index}.webm`,
      license: { name: 'CC0', url: 'https://creativecommons.org/publicdomain/zero/1.0/' },
      attribution: `Creator ${index % 3}`, demo: false, kuairandId: null }, ...overrides };
}
function insert(dbPath, entries) {
  const db = new DatabaseSync(dbPath);
  try {
    db.exec('PRAGMA busy_timeout=5000; BEGIN IMMEDIATE;');
    const statement = db.prepare('INSERT INTO videos(id,source,source_id,category,creator_key,search_text,payload_json,imported_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING');
    for (const item of entries) statement.run(item.id, item.source.provider, item.source.id, item.category, item.handle,
      [item.creator, item.handle, item.title, item.description, item.category, ...item.tags].join(' ').toLowerCase(), JSON.stringify(item), '2026-10-27T00:00:00Z');
    db.exec('COMMIT');
  } finally { db.close(); }
}
async function fixture(t, { count = 0, seedPosts = [] } = {}) {
  const directory = await mkdtemp(path.join(tmpdir(), 'loop-catalog-'));
  const dbPath = path.join(directory, 'catalog.sqlite');
  const store = createCatalogStore({ dbPath, seedPosts });
  t.after(async () => { store.close(); await rm(directory, { recursive: true, force: true }); });
  if (count) {
    store.stats();
    insert(dbPath, Array.from({ length: count }, (_, index) => post(index + 1)));
  }
  return { store, dbPath, directory };
}

test('initialization is lazy and original demo seeding is idempotent with attribution', async t => {
  const directory = await mkdtemp(path.join(tmpdir(), 'loop-seed-'));
  const dbPath = path.join(directory, 'nested/catalog.sqlite');
  const first = createCatalogStore({ dbPath });
  assert.equal(existsSync(dbPath), false);
  assert.equal(first.stats().total, 8);
  const example = first.get('coast-sunset');
  assert.equal(example.source.provider, 'demo');
  assert.equal(example.source.demo, true);
  assert.equal(example.source.kuairandId, null);
  assert.match(example.source.url, /drone-tour-of-a-coast-at-sunset-44405/);
  assert.equal(example.source.license.url, 'https://mixkit.co/license/#videoFree');
  first.close();
  const second = createCatalogStore({ dbPath });
  t.after(async () => { second.close(); await rm(directory, { recursive: true, force: true }); });
  assert.equal(second.stats().total, 8);
  assert.equal(second.stats().bySource.demo, 8);
});

test('pages beyond eight items without duplicates and preserves stable interest ordering', async t => {
  const { store } = await fixture(t, { count: 53 });
  const ids = [], categories = [];
  let cursor = null, total;
  do {
    const page = store.query({ limit: 7, preferences: ['Nature'], cursor });
    total ??= page.total;
    assert.equal(page.total, total);
    assert.ok(page.items.length <= 7);
    ids.push(...page.items.map(item => item.id));
    categories.push(...page.items.map(item => item.category));
    cursor = page.nextCursor;
  } while (cursor);
  assert.equal(total, 53);
  assert.equal(new Set(ids).size, 53);
  assert.deepEqual(categories.slice(0, 11), Array(11).fill('Nature'));
  assert.ok(categories.slice(11).every(category => category !== 'Nature'));
  assert.equal(store.query({ limit: 40, view: 'discover' }).items[0].id, 'commons:1');
});

test('category and search filters intersect and search treats SQL wildcard characters literally', async t => {
  const { store, dbPath } = await fixture(t, { count: 20 });
  insert(dbPath, [post(100, { title: '100% complete', description: 'A single_percent and back\\slash literal.', category: 'Food' })]);
  const filtered = store.query({ category: 'Food', query: ' collection 3 ', view: 'discover' });
  assert.ok(filtered.total > 0);
  assert.ok(filtered.items.every(item => item.category === 'Food' && item.description.includes('collection 3')));
  assert.deepEqual(store.query({ query: '%' }).items.map(item => item.id), ['commons:100']);
  assert.deepEqual(store.query({ query: 'single_' }).items.map(item => item.id), ['commons:100']);
  assert.deepEqual(store.query({ query: 'back\\slash' }).items.map(item => item.id), ['commons:100']);
  assert.equal(store.query({ query: "%' OR 1=1 --" }).total, 0);
  assert.equal(store.stats().total, 21);
});

test('following and library views use exact persisted creator/item identities', async t => {
  const { store } = await fixture(t, { count: 15 });
  for (const view of ['following', 'saved', 'liked']) assert.equal(store.query({ view }).total, 0);
  const following = store.query({ view: 'following', following: ['commons-author-1'], limit: 40 });
  assert.equal(following.total, 5);
  assert.ok(following.items.every(item => item.handle === 'commons-author-1'));
  for (const view of ['saved', 'liked']) {
    assert.deepEqual(store.query({ view, ids: ['commons:2', 'commons:12', 'commons:2'], limit: 40 }).items.map(item => item.id), ['commons:2', 'commons:12']);
  }
  assert.equal(store.query({ view: 'saved', ids: ['commons:2'], category: 'Nature' }).total, 0);
});

test('a cursor binds query filters but permits a different page size', async t => {
  const { store } = await fixture(t, { count: 20 });
  const first = store.query({ limit: 3, preferences: ['Nature', 'Food'] });
  const second = store.query({ limit: 7, preferences: ['Food', 'Nature'], cursor: first.nextCursor });
  assert.equal(second.items.length, 7);
  assert.ok(second.items.every(item => !first.items.some(before => before.id === item.id)));
  for (const change of [{ category: 'Food' }, { query: 'Clip' }, { view: 'discover' }, { ids: ['commons:1'] }]) {
    assert.throws(() => store.query({ preferences: ['Nature', 'Food'], cursor: first.nextCursor, ...change }), { status: 400 });
  }
  assert.throws(() => store.query({ cursor: 'not-json' }), { status: 400 });
});

test('append-only imports do not change an active page snapshot', async t => {
  const { store, dbPath } = await fixture(t, { count: 17 });
  const first = store.query({ limit: 5, preferences: ['Nature'] });
  insert(dbPath, Array.from({ length: 12 }, (_, index) => post(100 + index, { category: 'Nature' })));
  const seen = first.items.map(item => item.id);
  let cursor = first.nextCursor;
  do {
    const page = store.query({ limit: 5, preferences: ['Nature'], cursor });
    assert.equal(page.total, 17);
    assert.equal(page.snapshot, first.snapshot);
    seen.push(...page.items.map(item => item.id));
    cursor = page.nextCursor;
  } while (cursor);
  assert.equal(new Set(seen).size, 17);
  assert.ok(seen.every(id => Number(id.split(':')[1]) < 100));
  assert.equal(store.query().total, 29);
});

for (const mutation of ['update', 'delete']) {
  test(`changing existing metadata by ${mutation} invalidates a cursor explicitly`, async t => {
    const { store, dbPath } = await fixture(t, { count: 12 });
    const cursor = store.query({ limit: 4 }).nextCursor;
    const db = new DatabaseSync(dbPath);
    if (mutation === 'update') db.prepare('UPDATE videos SET category=? WHERE id=?').run('Food', 'commons:1');
    else db.prepare('DELETE FROM videos WHERE id=?').run('commons:1');
    db.close();
    assert.throws(() => store.query({ cursor }), { code: 'catalog_changed', status: 409 });
    assert.equal(store.query().total, mutation === 'delete' ? 11 : 12);
  });
}

test('query validation is bounded and closes without reopening the database', async t => {
  const { store, dbPath } = await fixture(t);
  for (const value of [[], null, { unknown: 1 }, { limit: 0 }, { limit: 41 }, { limit: '12' }, { query: 'x'.repeat(101) },
    { category: 'Secret' }, { view: 'all-data' }, { cursor: '' }, { following: null }, { ids: Array(501).fill('a') },
    { ids: [''] }, { preferences: ['Unknown'] }, JSON.parse('{"__proto__":{"bad":1}}')]) {
    assert.throws(() => store.query(value), { status: 400 });
  }
  assert.equal(existsSync(dbPath), false, 'invalid queries should not initialize storage');
  store.close();
  assert.throws(() => store.stats(), { status: 503 });
});

test('single-item lookup and counts contain metadata only and keep identities parameterized', async t => {
  const { store } = await fixture(t, { count: 13 });
  assert.equal(store.get('commons:2').source.provider, 'wikimedia');
  assert.equal(store.get("' OR 1=1 --"), null);
  assert.equal(store.get('not-found'), null);
  assert.throws(() => store.get(''), { status: 400 });
  assert.equal(store.stats().bySource.wikimedia, 13);
  assert.equal(Object.values(store.stats().byCategory).reduce((sum, value) => sum + value, 0), 13);
  assert.equal(store.stats().mode, 'metadata-and-streaming');
});

test('snapshot-consistent stats keep source, category and total counts aligned during a concurrent import', async t => {
  const { store, dbPath } = await fixture(t, { count: 3 });
  const prepare = DatabaseSync.prototype.prepare;
  let imported = false;
  // A second real SQLite connection commits after the first grouped SELECT,
  // reproducing an importer writing while the HTTP stats response is built.
  t.mock.method(DatabaseSync.prototype, 'prepare', function (sql) {
    const statement = prepare.call(this, sql);
    if (sql !== 'SELECT source,COUNT(*) AS count FROM videos GROUP BY source') return statement;
    return { all(...args) {
      const rows = statement.all(...args);
      if (!imported) { imported = true; insert(dbPath, [post(100, { category: 'Science' })]); }
      return rows;
    } };
  });
  const snapshot = store.stats();
  assert.equal(imported, true);
  assert.equal(snapshot.total, 3);
  assert.equal(Object.values(snapshot.bySource).reduce((sum, count) => sum + count, 0), snapshot.total);
  assert.equal(Object.values(snapshot.byCategory).reduce((sum, count) => sum + count, 0), snapshot.total);
  const refreshed = store.stats();
  assert.equal(refreshed.total, 4);
  assert.equal(Object.values(refreshed.bySource).reduce((sum, count) => sum + count, 0), refreshed.total);
  assert.equal(Object.values(refreshed.byCategory).reduce((sum, count) => sum + count, 0), refreshed.total);
});

async function httpFixture(t, { count = 23 } = {}) {
  const data = await fixture(t, { count });
  await writeFile(path.join(data.directory, 'index.html'), '<!doctype html><title>Catalog fixture</title>');
  let modelCalls = 0;
  const server = createStaticServer(data.directory, { catalogStore: data.store, deepfmClient: { request: () => { modelCalls += 1; throw new Error('Model must stay lazy'); } } });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  t.after(() => new Promise(resolve => server.close(resolve)));
  const port = server.address().port;
  const fetch = (urlPath, { method = 'GET', body, raw, headers = {} } = {}) => new Promise((resolve, reject) => {
    const payload = raw !== undefined ? raw : body === undefined ? undefined : JSON.stringify(body);
    const req = request({ hostname: '127.0.0.1', port, path: urlPath, method,
      headers: { ...(body === undefined ? {} : { 'Content-Type': 'application/json' }), ...headers } }, res => {
      const chunks = [];
      res.on('data', chunk => chunks.push(chunk));
      res.on('end', () => { const text = Buffer.concat(chunks).toString(); resolve({ status: res.statusCode, headers: res.headers, text, body: text && res.headers['content-type']?.includes('application/json') ? JSON.parse(text) : undefined }); });
      res.on('error', reject);
    });
    req.on('error', reject); req.end(payload);
  });
  return { ...data, fetch, modelCalls: () => modelCalls };
}

test('catalog HTTP routes page metadata, serve identities and preserve static/model separation', async t => {
  const { fetch, modelCalls } = await httpFixture(t);
  const first = await fetch('/api/catalog/query', { method: 'POST', body: { limit: 6 } });
  assert.equal(first.status, 200);
  assert.equal(first.headers['cache-control'], 'no-store');
  assert.equal(first.body.items.length, 6);
  const second = await fetch('/api/catalog/query', { method: 'POST', body: { limit: 6, cursor: first.body.nextCursor } });
  assert.equal(second.status, 200);
  assert.equal(second.body.total, 23);
  assert.ok(second.body.items.every(item => !first.body.items.some(before => before.id === item.id)));
  const item = await fetch('/api/catalog/items/commons%3A2');
  assert.equal(item.status, 200);
  assert.equal(item.body.item.id, 'commons:2');
  assert.equal((await fetch('/api/catalog/stats')).body.total, 23);
  assert.equal((await fetch('/api/catalog/items/commons%3A999')).status, 404);
  assert.equal((await fetch('/')).status, 200);
  assert.equal(modelCalls(), 0);
});

test('catalog HTTP validation rejects malformed or oversized bodies and unsupported requests', async t => {
  const { fetch, store } = await httpFixture(t);
  assert.equal((await fetch('/api/catalog/query', { method: 'POST', raw: '{', headers: { 'Content-Type': 'application/json' } })).status, 400);
  assert.equal((await fetch('/api/catalog/query', { method: 'POST', raw: '{}' })).status, 400);
  assert.equal((await fetch('/api/catalog/query', { method: 'POST', body: { limit: 1000 } })).status, 400);
  assert.equal((await fetch('/api/catalog/query', { method: 'POST', raw: ' '.repeat(128 * 1024 + 1), headers: { 'Content-Type': 'application/json' } })).status, 413);
  assert.equal((await fetch('/api/catalog/query?limit=1', { method: 'POST', body: {} })).status, 400);
  assert.equal((await fetch('/api/catalog/items/commons%3A2%2Fsecret')).status, 400);
  assert.equal((await fetch('/api/catalog/unknown')).status, 404);
  const wrongMethod = await fetch('/api/catalog/query');
  assert.equal(wrongMethod.status, 405);
  assert.equal(wrongMethod.headers.allow, 'POST');
  assert.equal((await fetch('/api/catalog/stats', { method: 'POST', body: {} })).status, 405);
  assert.equal(store.stats().total, 23);
});

test('stale cursors and storage failures have explicit safe HTTP errors', async t => {
  const { fetch, dbPath, store } = await httpFixture(t);
  const first = await fetch('/api/catalog/query', { method: 'POST', body: { limit: 3 } });
  const db = new DatabaseSync(dbPath);
  db.prepare('DELETE FROM videos WHERE id=?').run('commons:1'); db.close();
  const stale = await fetch('/api/catalog/query', { method: 'POST', body: { cursor: first.body.nextCursor } });
  assert.equal(stale.status, 409);
  assert.equal(stale.body.error.code, 'catalog_changed');
  store.close();
  const unavailable = await fetch('/api/catalog/stats');
  assert.equal(unavailable.status, 503);
  assert.equal(unavailable.body.error.code, 'catalog_unavailable');
  assert.doesNotMatch(unavailable.text, /loop-catalog-|catalog\.sqlite/);
});
