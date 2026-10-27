import test from 'node:test';
import assert from 'node:assert/strict';
import {
  STORAGE_KEY, createInitialState, normalizeState, toggleItem,
  getVisiblePosts, formatCount, loadState, saveState,
} from '../feed-state.js';

const posts = [
  { id: 'food', creator: 'Leo Chen', handle: '@leo', category: 'Food',
    title: 'Sunday ramen', description: 'A bowl worth slowing down for', tags: ['cooking'] },
  { id: 'mountain', creator: 'Maya Reed', handle: '@maya', category: 'Nature',
    title: 'Above the clouds', description: 'Morning on the ridge', tags: ['hiking'] },
  { id: 'city', creator: 'Maya Reed', handle: '@maya', category: 'Travel',
    title: 'Night in Tokyo', description: 'A different kind of city walk', tags: ['japan'] },
  { id: 'ocean', creator: 'Ari Lane', handle: '@ari', category: 'Nature',
    title: 'Blue hour', description: 'Waves and a little quiet', tags: ['ocean'] },
  { id: 'shape', creator: 'Nora Vale', handle: '@nora', category: 'Design',
    title: 'Shape studies', description: 'A playful poster', tags: ['color'] },
];

function memoryStorage() {
  const values = new Map();
  return {
    getItem: (key) => values.has(key) ? values.get(key) : null,
    setItem: (key, value) => values.set(key, String(value)),
  };
}

test('each initial state has independent lists', () => {
  const first = createInitialState();
  first.saved.push('ocean');
  first.preferences.push('Food');
  const next = createInitialState();
  assert.deepEqual(next.saved, []);
  assert.deepEqual(next.preferences, ['Nature', 'Travel']);
  assert.equal(next.reducedMotion, false);
});

test('likes, saves, follows and settings survive a normalized storage round trip', () => {
  const storage = memoryStorage();
  const state = createInitialState();
  state.liked = toggleItem(state.liked, 'city');
  state.saved = toggleItem(state.saved, 'ocean');
  state.following = toggleItem(state.following, '@maya');
  state.preferences = ['Nature', 'Food'];
  state.reducedMotion = true;
  assert.equal(saveState(storage, state), true);
  const restored = loadState(storage);
  assert.deepEqual(restored, state);
  assert.deepEqual(getVisiblePosts(posts, restored, { view: 'following' }).map((post) => post.id),
    ['mountain', 'city']);
  assert.deepEqual(getVisiblePosts(posts, restored, { view: 'saved' }).map((post) => post.id), ['ocean']);
  assert.deepEqual(getVisiblePosts(posts, restored, { view: 'liked' }).map((post) => post.id), ['city']);
});

test('following, saved and liked views start empty and react to removing an item', () => {
  const state = createInitialState();
  for (const view of ['following', 'saved', 'liked']) {
    assert.deepEqual(getVisiblePosts(posts, state, { view }), []);
  }
  state.following = ['@maya'];
  state.following = toggleItem(state.following, '@maya');
  assert.deepEqual(getVisiblePosts(posts, state, { view: 'following' }), []);
  state.saved = ['ocean'];
  state.saved = toggleItem(state.saved, 'ocean');
  assert.deepEqual(getVisiblePosts(posts, state, { view: 'saved' }), []);
});

test('personalized ordering is stable and returns original posts without mutation', () => {
  const snapshot = structuredClone(posts);
  const state = createInitialState();
  const stateSnapshot = structuredClone(state);
  const result = getVisiblePosts(posts, state);
  assert.deepEqual(result.map((post) => post.id), ['mountain', 'city', 'ocean', 'food', 'shape']);
  assert.equal(result[0], posts[1]);
  assert.deepEqual(posts, snapshot);
  assert.deepEqual(state, stateSnapshot);
  assert.deepEqual(getVisiblePosts(posts, state, { view: 'discover' }), posts);
  assert.deepEqual(getVisiblePosts(posts, { ...state, preferences: [] }), posts);
});

test('likes do not reorder a playing feed while changing interests does', () => {
  const state = createInitialState();
  const first = getVisiblePosts(posts, state);
  assert.deepEqual(getVisiblePosts(posts, { ...state, liked: ['food'] }), first);
  assert.equal(getVisiblePosts(posts, { ...state, preferences: ['Food'] })[0], posts[0]);
});

test('query and category intersect with each view and search all useful metadata', () => {
  const state = { ...createInitialState(), following: ['@maya'], saved: ['city', 'mountain'] };
  for (const query of [' TOKYO ', 'japan', '@MAYA', 'city walk']) {
    assert.deepEqual(getVisiblePosts(posts, state, { view: 'saved', category: 'Travel', query }), [posts[2]]);
  }
  assert.deepEqual(getVisiblePosts(posts, state, { view: 'following', category: 'Nature', query: 'hiking' }), [posts[1]]);
  assert.deepEqual(getVisiblePosts(posts, state, { category: 'Nature', query: 'ramen' }), []);
  assert.deepEqual(getVisiblePosts(posts, state, { query: 'no matches' }), []);
});

test('malformed and blocked storage falls back to a usable default state', () => {
  const storage = memoryStorage();
  assert.deepEqual(loadState(storage), createInitialState());
  for (const value of ['broken JSON', 'null', 'false', '15', '[]', '"text"']) {
    storage.setItem(STORAGE_KEY, value);
    assert.deepEqual(loadState(storage), createInitialState());
  }
  const blocked = {
    getItem() { throw new Error('Storage is blocked'); },
    setItem() { throw new Error('Storage quota exceeded'); },
  };
  assert.deepEqual(loadState(blocked), createInitialState());
  assert.equal(saveState(blocked, createInitialState()), false);
  assert.deepEqual(loadState(null), createInitialState());
});

test('normalization rejects invalid entries, limits lists and preserves explicit settings', () => {
  const dirty = {
    liked: [' ocean ', 'ocean', 4, null, '', 'x'.repeat(161), 'city'],
    saved: 'ocean', following: ['@maya', '@maya', false],
    preferences: ['Nature', 'Nature', 'Unknown', 'Food', null],
    reducedMotion: 'false', unexpected: 'discard',
  };
  const snapshot = structuredClone(dirty);
  assert.deepEqual(normalizeState(dirty), {
    liked: ['ocean', 'city'], saved: [], following: ['@maya'],
    preferences: ['Nature', 'Food'], reducedMotion: false,
  });
  assert.deepEqual(dirty, snapshot);
  assert.equal(normalizeState({ liked: Array.from({ length: 700 }, (_, i) => `id-${i}`) }).liked.length, 500);
  assert.deepEqual(normalizeState({ preferences: [] }).preferences, []);
  assert.deepEqual(normalizeState({ preferences: null }).preferences, ['Nature', 'Travel']);
  assert.equal(normalizeState({ reducedMotion: true }).reducedMotion, true);
});

test('toggle is immutable, deduplicates entries and keeps the most recent addition at capacity', () => {
  const original = ['ocean', 'ocean', 'city'];
  assert.deepEqual(toggleItem(original, 'city'), ['ocean']);
  assert.deepEqual(toggleItem(original, 'mountain'), ['ocean', 'city', 'mountain']);
  assert.deepEqual(original, ['ocean', 'ocean', 'city']);
  assert.deepEqual(toggleItem(original, ''), ['ocean', 'city']);
  const full = Array.from({ length: 500 }, (_, i) => `id-${i}`);
  const result = toggleItem(full, 'new');
  assert.equal(result.length, 500);
  assert.equal(result[0], 'id-1');
  assert.equal(result.at(-1), 'new');
});

test('compact counts handle common sizes and invalid values', () => {
  for (const [input, expected] of [[0, '0'], [999, '999'], [1000, '1K'], [12800, '12.8K'],
    [1000000, '1M'], [999999, '1M'], [-5, '0'], [NaN, '0'], [Infinity, '0'], ['20', '0']]) {
    assert.equal(formatCount(input), expected);
  }
});
