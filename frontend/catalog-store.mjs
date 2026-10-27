/** Indexed, local metadata catalog. Media bytes stay at their credited source. */
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { posts as demoPosts } from './assets/catalog.js';

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const TOPICS = ['Nature', 'Travel', 'Food', 'Design', 'Science'];
const VIEWS = ['for-you', 'discover', 'following', 'saved', 'liked'];
const QUERY_FIELDS = new Set(['limit', 'cursor', 'view', 'category', 'query', 'preferences', 'following', 'ids']);
const DEMO_SOURCE_URLS = {
  'coast-sunset': 'https://mixkit.co/free-stock-video/drone-tour-of-a-coast-at-sunset-44405/',
  'mountain-air': 'https://mixkit.co/free-stock-video/flying-over-a-mountainous-area-43130/',
  'coffee-ritual': 'https://mixkit.co/free-stock-video/latte-art-810/',
  'tokyo-after-dark': 'https://mixkit.co/free-stock-video/pedestrian-walk-in-tokyo-4231/',
  'ink-in-motion': 'https://mixkit.co/free-stock-video/abstract-video-of-a-liquid-with-dark-ink-flowing-44818/',
  'northern-lights': 'https://mixkit.co/free-stock-video/aurora-borealis-at-dawn-4040/',
  'forest-pause': 'https://mixkit.co/free-stock-video/gloomy-forest-full-of-trees-during-a-rainy-afternoon-22729/',
  'a-million-little-lights': 'https://mixkit.co/free-stock-video/view-of-the-night-sky-filled-with-stars-39770/',
};
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const catalogError = (message, code = 'invalid_query', status = 400) => Object.assign(new Error(message), { code, status });

function stringList(value, name, { categories = false } = {}) {
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.length > 500) throw catalogError(`${name} must be an array of at most 500 strings.`);
  const normalized = [];
  for (const item of value) {
    if (typeof item !== 'string' || !item.trim() || item.length > 160) throw catalogError(`${name} contains an invalid value.`);
    if (categories && !TOPICS.includes(item)) throw catalogError('Choose a supported preference category.');
    normalized.push(item.trim());
  }
  return [...new Set(normalized)].sort();
}

export function normalizeCatalogQuery(value = {}) {
  if (!object(value) || Object.keys(value).some(key => !QUERY_FIELDS.has(key))) throw catalogError('Unsupported catalog query fields.');
  const limit = value.limit === undefined ? 12 : value.limit;
  if (!Number.isInteger(limit) || limit < 1 || limit > 40) throw catalogError('limit must be an integer from 1 to 40.');
  const view = value.view === undefined ? 'for-you' : value.view;
  if (!VIEWS.includes(view)) throw catalogError('Choose a supported catalog view.');
  const category = value.category === undefined ? 'All' : value.category;
  if (category !== 'All' && !TOPICS.includes(category)) throw catalogError('Choose a supported category.');
  const query = value.query === undefined ? '' : value.query;
  if (typeof query !== 'string' || query.length > 100) throw catalogError('query must be a string of at most 100 characters.');
  const cursor = value.cursor === undefined ? null : value.cursor;
  if (cursor !== null && (typeof cursor !== 'string' || !cursor || cursor.length > 2048)) throw catalogError('The catalog cursor is invalid.');
  return { limit, cursor, view, category, query: query.trim().toLowerCase(),
    preferences: stringList(value.preferences, 'preferences', { categories: true }),
    following: stringList(value.following, 'following'), ids: stringList(value.ids, 'ids') };
}

function fingerprint(query) {
  const { limit, cursor, ...filters } = query;
  return createHash('sha256').update(JSON.stringify(filters)).digest('hex');
}

function decodeCursor(value, expectedFingerprint) {
  let parsed;
  try {
    if (!/^[A-Za-z0-9_-]+$/.test(value)) throw new Error();
    const bytes = Buffer.from(value, 'base64url');
    if (bytes.toString('base64url') !== value) throw new Error();
    parsed = JSON.parse(bytes.toString('utf8'));
  } catch { throw catalogError('The catalog cursor is invalid.'); }
  if (!object(parsed) || Object.keys(parsed).sort().join(',') !== 'fingerprint,maxSeq,offset,revision,v'
      || parsed.v !== 1 || parsed.fingerprint !== expectedFingerprint
      || !Number.isSafeInteger(parsed.maxSeq) || parsed.maxSeq < 0
      || !Number.isSafeInteger(parsed.revision) || parsed.revision < 0
      || !Number.isSafeInteger(parsed.offset) || parsed.offset < 0) {
    throw catalogError('The cursor does not match this catalog query.');
  }
  return parsed;
}

function demoPayload(post) {
  return { ...post, source: { provider: 'demo', id: post.id,
    url: DEMO_SOURCE_URLS[post.id] || 'https://mixkit.co/', license: { name: 'Mixkit Stock Video Free License', url: 'https://mixkit.co/license/#videoFree' },
    attribution: 'Demo creators and engagement; credited Mixkit footage. See assets/MEDIA.md.',
    demo: true, kuairandId: null } };
}

function searchText(post) {
  return [post.creator, post.handle, post.title, post.description, post.category, ...(post.tags || [])].join(' ').toLowerCase();
}

export function createCatalogStore({ dbPath = process.env.CATALOG_DB || path.join(ROOT, 'data/catalog/catalog.sqlite'), seedPosts = demoPosts } = {}) {
  let db = null, closed = false;
  function database() {
    if (closed) throw catalogError('The catalog connection is closed.', 'catalog_unavailable', 503);
    if (db) return db;
    if (dbPath !== ':memory:') mkdirSync(path.dirname(path.resolve(dbPath)), { recursive: true });
    const opened = new DatabaseSync(dbPath);
    try {
      opened.exec('PRAGMA busy_timeout = 5000; PRAGMA journal_mode = WAL;');
      opened.exec(readFileSync(path.join(ROOT, 'catalog/schema.sql'), 'utf8'));
      const version = opened.prepare("SELECT value FROM catalog_meta WHERE key='schema_version'").get()?.value;
      if (version !== '1') throw new Error('Unsupported catalog schema version');
      const insert = opened.prepare('INSERT INTO videos(id,source,source_id,category,creator_key,search_text,payload_json,imported_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING');
      opened.exec('BEGIN IMMEDIATE');
      try {
        for (const post of seedPosts) {
          const payload = demoPayload(post);
          insert.run(post.id, 'demo', post.id, post.category, post.handle, searchText(post), JSON.stringify(payload), new Date().toISOString());
        }
        opened.exec('COMMIT');
      } catch (error) { opened.exec('ROLLBACK'); throw error; }
      db = opened;
      return db;
    } catch (error) { opened.close(); throw error; }
  }
  function revision(connection) {
    return Number(connection.prepare("SELECT value FROM catalog_meta WHERE key='content_revision'").get().value);
  }
  return {
    query(value = {}) {
      const query = normalizeCatalogQuery(value);
      const connection = database();
      connection.exec('BEGIN');
      try {
        const key = fingerprint(query);
        const currentMax = connection.prepare('SELECT COALESCE(MAX(seq),0) AS max FROM videos').get().max;
        const currentRevision = revision(connection);
        const snapshot = query.cursor ? decodeCursor(query.cursor, key)
          : { v: 1, fingerprint: key, maxSeq: currentMax, revision: currentRevision, offset: 0 };
        if (snapshot.revision !== currentRevision || snapshot.maxSeq > currentMax) {
          throw catalogError('The catalog changed. Start a fresh catalog query.', 'catalog_changed', 409);
        }
        const clauses = ['seq <= ?'], values = [snapshot.maxSeq];
        if (query.category !== 'All') { clauses.push('category = ?'); values.push(query.category); }
        if (query.query) {
          clauses.push("search_text LIKE ? ESCAPE '\\'");
          values.push(`%${query.query.replace(/[\\%_]/g, character => `\\${character}`)}%`);
        }
        if (query.view === 'following') {
          if (!query.following.length) clauses.push('0');
          else { clauses.push(`creator_key IN (${query.following.map(() => '?').join(',')})`); values.push(...query.following); }
        }
        if (query.view === 'saved' || query.view === 'liked') {
          if (!query.ids.length) clauses.push('0');
          else { clauses.push(`id IN (${query.ids.map(() => '?').join(',')})`); values.push(...query.ids); }
        }
        const where = clauses.join(' AND ');
        const total = connection.prepare(`SELECT COUNT(*) AS count FROM videos WHERE ${where}`).get(...values).count;
        if (snapshot.offset > total) throw catalogError('The catalog cursor offset is invalid.');
        const orderValues = [];
        let order = 'seq ASC';
        if (query.view === 'for-you' && query.preferences.length) {
          order = `CASE WHEN category IN (${query.preferences.map(() => '?').join(',')}) THEN 0 ELSE 1 END, seq ASC`;
          orderValues.push(...query.preferences);
        }
        const rows = connection.prepare(`SELECT payload_json FROM videos WHERE ${where} ORDER BY ${order} LIMIT ? OFFSET ?`)
          .all(...values, ...orderValues, query.limit, snapshot.offset);
        const offset = snapshot.offset + rows.length;
        const nextCursor = offset < total ? Buffer.from(JSON.stringify({ ...snapshot, offset })).toString('base64url') : null;
        return { items: rows.map(row => JSON.parse(row.payload_json)), nextCursor, total,
          snapshot: `${snapshot.maxSeq}:${snapshot.revision}` };
      } finally { connection.exec('ROLLBACK'); }
    },
    stats() {
      const connection = database();
      connection.exec('BEGIN');
      try {
        const bySource = Object.fromEntries([['demo', 0], ['wikimedia', 0],
          ...connection.prepare('SELECT source,COUNT(*) AS count FROM videos GROUP BY source').all().map(row => [row.source, row.count])]);
        const byCategory = Object.fromEntries(TOPICS.map(topic => [topic, 0]));
        for (const row of connection.prepare('SELECT category,COUNT(*) AS count FROM videos GROUP BY category').all()) byCategory[row.category] = row.count;
        return { total: connection.prepare('SELECT COUNT(*) AS count FROM videos').get().count,
          bySource, byCategory, mode: 'metadata-and-streaming' };
      } finally { connection.exec('ROLLBACK'); }
    },
    get(id) {
      if (typeof id !== 'string' || !id.trim() || id.length > 160 || /[\u0000-\u001f]/.test(id)) throw catalogError('Choose a valid catalog item ID.');
      const row = database().prepare('SELECT payload_json FROM videos WHERE id = ?').get(id);
      return row ? JSON.parse(row.payload_json) : null;
    },
    close() { if (closed) return; closed = true; db?.close(); db = null; },
  };
}
