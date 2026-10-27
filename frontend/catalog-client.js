/** Paginated, attribution-aware catalog transport. Never invents extra clips. */
const TOPICS = new Set(['All', 'Nature', 'Travel', 'Food', 'Design', 'Science']);
const VIEWS = new Set(['for-you', 'discover', 'following', 'saved', 'liked']);
const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9:_.-]{0,159}$/;
const MEDIA_TYPE = /^video\/(?:webm|mp4|quicktime)(?:; ?codecs="[a-z0-9., ]{1,40}")?$/;
const TIMEOUT = 30_000;
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const text = (value, limit, nonempty = true) => typeof value === 'string'
  && value.length <= limit && (!nonempty || value.trim().length > 0);
const invalid = () => new Error('The catalog returned an invalid response. Refresh your mix and try again.');

function safeMedia(value, video, { quicktime = false } = {}) {
  if (!text(value, 4096)) return false;
  const extension = video ? (quicktime ? /\.(mp4|webm|mov)$/i : /\.(mp4|webm)$/i) : /\.(jpg|jpeg|png|webp)$/i;
  if (/^\.\/assets\/media\/[A-Za-z0-9_.-]+$/.test(value)) return extension.test(value);
  try {
    const url = new URL(value);
    const trustedHost = url.hostname === 'upload.wikimedia.org'
      || (!video && url.hostname === 'thumb.wikimedia.org');
    return url.protocol === 'https:' && trustedHost
      && !url.port && !url.username && !url.password
      && url.pathname.startsWith('/wikipedia/commons/') && extension.test(url.pathname);
  } catch { return false; }
}

function safeLink(value) {
  try {
    const url = new URL(value);
    return text(value, 4096) && url.protocol === 'https:' && !url.username && !url.password;
  } catch { return false; }
}

function validateItem(item) {
  if (!object(item) || typeof item.id !== 'string' || !IDENTIFIER.test(item.id)
      || !text(item.title, 1000) || !text(item.description, 8000, false)
      || !text(item.creator, 1000) || typeof item.handle !== 'string' || !IDENTIFIER.test(item.handle)
      || !text(item.initials, 12) || typeof item.avatarColor !== 'string' || !/^#[0-9a-f]{6}$/i.test(item.avatarColor)
      || !TOPICS.has(item.category) || item.category === 'All'
      || !Array.isArray(item.tags) || item.tags.length > 40 || !item.tags.every(tag => text(tag, 160))
      || !safeMedia(item.video, true) || !safeMedia(item.poster, false)
      || (item.sources !== undefined && (!Array.isArray(item.sources) || !item.sources.length || item.sources.length > 4
        || !item.sources.every(source => object(source) && MEDIA_TYPE.test(source.type)
          && safeMedia(source.src, true, { quicktime: source.type.startsWith('video/quicktime') }))))
      || !Number.isFinite(item.duration) || item.duration <= 0
      || !Number.isSafeInteger(item.likes) || item.likes < 0
      || !Array.isArray(item.comments) || item.comments.length > 100
      || !item.comments.every(comment => object(comment) && text(comment.author, 1000) && text(comment.text, 4000))
      || !text(item.reason, 4000, false) || !text(item.location, 1000, false)) throw invalid();
  const source = item.source;
  if (!object(source) || !['demo', 'wikimedia'].includes(source.provider)
      || !text(source.id, 160) || !safeLink(source.url)
      || !object(source.license) || !text(source.license.name, 240) || !safeLink(source.license.url)
      || !text(source.attribution, 16000) || source.demo !== (source.provider === 'demo')
      || source.kuairandId !== null) throw invalid();
  return item;
}

/**
 * The first listed source the browser reports as playable, else post.video.
 * Imported clips list QuickTime first: Safari renders it, while Chrome and
 * Firefox report it unplayable and fall through to the WebM.
 */
export function playableSource(post, canPlayType) {
  return post.sources?.find(source => canPlayType(source.type) !== '')?.src || post.video;
}

function filtersFor(value) {
  if (!object(value)) throw new Error('Choose valid catalog filters.');
  const allowed = new Set(['view', 'category', 'query', 'preferences', 'following', 'ids']);
  if (Object.keys(value).some(key => !allowed.has(key))) throw new Error('Choose valid catalog filters.');
  const result = { view: 'for-you', category: 'All', query: '', preferences: [], following: [], ids: [], ...value };
  if (!VIEWS.has(result.view) || !TOPICS.has(result.category) || !text(result.query, 100, false)) throw new Error('Choose valid catalog filters.');
  for (const key of ['preferences', 'following', 'ids']) {
    if (!Array.isArray(result[key]) || result[key].length > 500
        || !result[key].every(entry => text(entry, 160))) throw new Error('Choose valid catalog filters.');
    result[key] = [...new Set(result[key])];
  }
  if (!result.preferences.every(topic => TOPICS.has(topic))) throw new Error('Choose valid catalog topics.');
  result.preferences = result.preferences.filter(topic => topic !== 'All');
  return result;
}

function withAbort(value, signal) {
  return new Promise((resolve, reject) => {
    const abort = () => reject(Object.assign(new Error('Request cancelled.'), { name: 'AbortError' }));
    if (signal.aborted) return abort();
    signal.addEventListener('abort', abort, { once: true });
    Promise.resolve(value).then(result => { signal.removeEventListener('abort', abort); resolve(result); }, error => { signal.removeEventListener('abort', abort); reject(error); });
  });
}

async function requestJSON(fetcher, url, options, controller) {
  let timedOut = false;
  const timer = setTimeout(() => { timedOut = true; controller.abort(); }, TIMEOUT);
  try {
    const response = await withAbort(fetcher(url, { ...options, cache: 'no-store', signal: controller.signal }), controller.signal);
    let body;
    try { body = await withAbort(response.json(), controller.signal); }
    catch (error) { if (controller.signal.aborted) throw error; throw invalid(); }
    if (!response.ok) {
      const message = object(body) && object(body.error) && text(body.error.message, 1000)
        ? body.error.message : 'The catalog is unavailable. Please try again.';
      throw Object.assign(new Error(message), { status: response.status, code: body?.error?.code });
    }
    return body;
  } catch (error) {
    if (timedOut) throw new Error('The catalog took too long to respond. Please try again.');
    if (controller.signal.aborted || error.status || error.message === invalid().message) throw error;
    throw new Error('Cannot load the catalog. Check that the local preview server is running.');
  } finally { clearTimeout(timer); }
}

export function createCatalogPager({ fetcher = globalThis.fetch, pageSize = 12 } = {}) {
  if (typeof fetcher !== 'function') throw new TypeError('A fetch function is required.');
  if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 40) throw new TypeError('pageSize must be an integer from 1 to 40.');
  let items = [], total = 0, cursor = null, snapshot = null, generation = 0;
  let initialized = false, hasMore = false, loading = false, error = null, active = null;
  let filters = filtersFor({});
  const getState = () => ({ items: [...items], total, hasMore, loading, error, generation });

  function loadMore() {
    if (active) return active.promise;
    if (initialized && !hasMore) return Promise.resolve(getState());
    const ticket = generation, requestedCursor = cursor, controller = new AbortController();
    const request = { controller, promise: null };
    loading = true; error = null;
    request.promise = (async () => {
      try {
        const body = await requestJSON(fetcher, '/api/catalog/query', {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ ...filters, limit: pageSize, cursor: requestedCursor }),
        }, controller);
        if (ticket !== generation) return getState();
        if (!object(body) || !Array.isArray(body.items) || body.items.length > pageSize
            || !Number.isSafeInteger(body.total) || body.total < 0
            || !(body.nextCursor === null || text(body.nextCursor, 8192))) throw invalid();
        if (body.snapshot !== undefined && !text(body.snapshot, 8192)) throw invalid();
        if (initialized && (body.total !== total || (snapshot !== null && body.snapshot !== snapshot))) throw invalid();
        const seen = new Set(items.map(item => item.id));
        for (const item of body.items) { validateItem(item); if (seen.has(item.id)) throw invalid(); seen.add(item.id); }
        const size = items.length + body.items.length;
        if (size > body.total || (body.nextCursor === null && size !== body.total)
            || (body.nextCursor !== null && (!body.items.length || body.nextCursor === requestedCursor || size >= body.total))) throw invalid();
        items = [...items, ...body.items]; total = body.total; cursor = body.nextCursor;
        snapshot = body.snapshot ?? snapshot; initialized = true; hasMore = cursor !== null;
      } catch (problem) {
        if (ticket !== generation) return getState();
        error = problem.message || 'The catalog is unavailable. Please try again.';
        throw problem;
      } finally {
        if (active === request) { loading = false; active = null; }
      }
      return getState();
    })();
    active = request;
    return request.promise;
  }

  return {
    reset(value = {}) {
      let next;
      try { next = filtersFor(value); } catch (problem) { return Promise.reject(problem); }
      generation += 1;
      active?.controller.abort(); active = null;
      filters = next; items = []; total = 0; cursor = null; snapshot = null;
      initialized = false; hasMore = true; loading = false; error = null;
      return loadMore();
    },
    loadMore,
    getState,
    async getItem(id) {
      if (typeof id !== 'string' || !IDENTIFIER.test(id)) throw new Error('Choose a valid catalog item.');
      const result = await requestJSON(fetcher, `/api/catalog/items/${encodeURIComponent(id)}`, { method: 'GET' }, new AbortController());
      if (!object(result) || result.item?.id !== id) throw invalid();
      return validateItem(result.item);
    },
    async stats() {
      const result = await requestJSON(fetcher, '/api/catalog/stats', { method: 'GET' }, new AbortController());
      if (!object(result) || !Number.isSafeInteger(result.total) || result.total < 0
          || !object(result.bySource) || !object(result.byCategory)) throw invalid();
      for (const value of [...Object.values(result.bySource), ...Object.values(result.byCategory)]) if (!Number.isSafeInteger(value) || value < 0) throw invalid();
      return result;
    },
  };
}
