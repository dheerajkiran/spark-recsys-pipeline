/** Browser-local demo feed state. This module never reads model/test data. */
export const STORAGE_KEY = 'loop-feed-v1';

const MAX_ITEMS = 500;
const MAX_ID_LENGTH = 160;
const CATEGORIES = new Set(['All', 'Nature', 'Travel', 'Food', 'Design', 'Science']);

export function createInitialState() {
  return {
    liked: [],
    saved: [],
    following: [],
    preferences: ['Nature', 'Travel'],
    reducedMotion: false,
  };
}

function uniqueStrings(value, fallback = []) {
  if (!Array.isArray(value)) return [...fallback];
  const unique = new Set();
  for (const item of value) {
    if (typeof item !== 'string') continue;
    const id = item.trim();
    if (id && id.length <= MAX_ID_LENGTH) unique.add(id);
    if (unique.size === MAX_ITEMS) break;
  }
  return [...unique];
}

export function normalizeState(value) {
  const defaults = createInitialState();
  if (!value || typeof value !== 'object' || Array.isArray(value)) return defaults;
  return {
    liked: uniqueStrings(value.liked),
    saved: uniqueStrings(value.saved),
    following: uniqueStrings(value.following),
    preferences: uniqueStrings(value.preferences, defaults.preferences)
      .filter((category) => CATEGORIES.has(category)),
    reducedMotion: typeof value.reducedMotion === 'boolean'
      ? value.reducedMotion : defaults.reducedMotion,
  };
}

export function toggleItem(list, id) {
  const items = uniqueStrings(list);
  const validId = uniqueStrings([id])[0];
  if (!validId) return items;
  if (items.includes(validId)) return items.filter((item) => item !== validId);
  return [...items.slice(-(MAX_ITEMS - 1)), validId];
}

export function getVisiblePosts(posts, state, {
  view = 'for-you', category = 'All', query = '',
} = {}) {
  const current = normalizeState(state);
  const search = typeof query === 'string' ? query.trim().toLowerCase() : '';
  const following = new Set(current.following);
  const saved = new Set(current.saved);
  const liked = new Set(current.liked);
  const preferred = new Set(current.preferences);
  const visible = (Array.isArray(posts) ? posts : []).filter((post) => {
    if (!post || typeof post !== 'object') return false;
    if (view === 'following' && !following.has(post.handle)) return false;
    if (view === 'saved' && !saved.has(post.id)) return false;
    if (view === 'liked' && !liked.has(post.id)) return false;
    if (category !== 'All' && post.category !== category) return false;
    if (!search) return true;
    const text = [post.creator, post.handle, post.title, post.description,
      post.category, ...(Array.isArray(post.tags) ? post.tags : [])]
      .filter((value) => typeof value === 'string').join(' ').toLowerCase();
    return text.includes(search);
  });
  if (view !== 'for-you') return visible;
  // Rank by demo interests only. Likes do not reorder a playing feed.
  return visible.map((post, index) => ({ post, index }))
    .sort((left, right) => Number(preferred.has(right.post.category))
      - Number(preferred.has(left.post.category)) || left.index - right.index)
    .map(({ post }) => post);
}

export function formatCount(value) {
  let number = typeof value === 'number' && Number.isFinite(value)
    ? Math.max(0, Math.floor(value)) : 0;
  const units = ['', 'K', 'M', 'B'];
  let unit = 0;
  while (number >= 1000 && unit < units.length - 1) {
    number /= 1000;
    unit += 1;
  }
  if (!unit) return String(number);
  if (Math.round(number * 10) / 10 >= 1000 && unit < units.length - 1) {
    number /= 1000;
    unit += 1;
  }
  return `${number.toFixed(1).replace(/\.0$/, '')}${units[unit]}`;
}

export function loadState(storage) {
  try {
    const value = storage.getItem(STORAGE_KEY);
    return value === null ? createInitialState() : normalizeState(JSON.parse(value));
  } catch {
    return createInitialState();
  }
}

export function saveState(storage, state) {
  try {
    storage.setItem(STORAGE_KEY, JSON.stringify(normalizeState(state)));
    return true;
  } catch {
    return false;
  }
}
