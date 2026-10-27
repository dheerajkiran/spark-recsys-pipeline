import test from 'node:test';
import assert from 'node:assert/strict';
import { createCatalogPager, playableSource } from '../catalog-client.js';

const item = id => ({
  id: `commons:${id}`, creator: 'A real source author', handle: 'commons-author-123',
  initials: 'AS', avatarColor: '#cbd6bd', title: `A new view ${id}`,
  description: 'A credited catalog clip.', tags: ['nature'], category: 'Nature',
  video: `https://upload.wikimedia.org/wikipedia/commons/transcoded/a/ab/View_${id}.webm/View_${id}.webm.360p.vp9.webm`,
  poster: `https://upload.wikimedia.org/wikipedia/commons/thumb/a/ab/View_${id}.webm/480px--View_${id}.webm.jpg`,
  likes: 0, comments: [], duration: 12, reason: 'From the catalog.', location: '',
  source: { provider: 'wikimedia', id: String(id), url: `https://commons.wikimedia.org/wiki/File:View_${id}.webm`,
    license: { name: 'CC BY 4.0', url: 'https://creativecommons.org/licenses/by/4.0/' },
    attribution: 'A real source author', demo: false, kuairandId: null },
});
const reply = (value, status = 200) => ({ ok: status >= 200 && status < 300, status, json: async () => value });
const page = (ids, total, nextCursor = null, snapshot = 'snapshot-1') => ({ items: ids.map(item), total, nextCursor, snapshot });
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };

test('loads distinct pages beyond eight and stops requesting at the real catalog end', async () => {
  const requests = [];
  const replies = [page([1, 2, 3, 4], 12, 'page-2'), page([5, 6, 7, 8], 12, 'page-3'), page([9, 10, 11, 12], 12)];
  const pager = createCatalogPager({ pageSize: 4, fetcher: async (url, options) => {
    requests.push({ url, options, body: JSON.parse(options.body) }); return reply(replies.shift());
  } });
  assert.equal((await pager.reset({ preferences: ['Nature', 'All'] })).items.length, 4);
  assert.equal((await pager.loadMore()).items.length, 8);
  const result = await pager.loadMore();
  assert.equal(result.items.length, 12); assert.equal(result.total, 12); assert.equal(result.hasMore, false); assert.equal(result.loading, false);
  assert.equal(new Set(result.items.map(post => post.id)).size, 12);
  await pager.loadMore(); assert.equal(requests.length, 3);
  assert.deepEqual(requests.map(request => request.body.cursor), [null, 'page-2', 'page-3']);
  assert.deepEqual(requests[0].body.preferences, ['Nature']);
  assert.equal(requests[0].options.method, 'POST'); assert.equal(requests[0].options.cache, 'no-store');
  result.items.pop(); assert.equal(pager.getState().items.length, 12);
});

test('coalesces concurrent requests rather than loading the same page twice', async () => {
  const pending = deferred(); let calls = 0;
  const pager = createCatalogPager({ fetcher: () => { calls++; return pending.promise; } });
  const first = pager.reset(); const second = pager.loadMore();
  assert.equal(first, second); assert.equal(calls, 1); assert.equal(pager.getState().loading, true);
  pending.resolve(reply(page([1], 1)));
  assert.equal((await first).loading, false); await second;
});

test('view changes abort old requests and ignore late results even if transport ignores abort', async () => {
  const pending = deferred(); const signals = []; let calls = 0;
  const pager = createCatalogPager({ fetcher: (url, options) => {
    signals.push(options.signal); calls++;
    return calls === 1 ? pending.promise : reply(page([20], 1));
  } });
  const stale = pager.reset({ category: 'Nature' });
  const current = await pager.reset({ category: 'Travel' });
  assert.equal(signals[0].aborted, true); assert.equal(current.generation, 2);
  pending.resolve(reply(page([1], 1)));
  await stale;
  assert.deepEqual(pager.getState().items.map(post => post.id), ['commons:20']);
  assert.equal(pager.getState().error, null);
});

test('failed next-page requests retain good results and retry the same cursor', async () => {
  let calls = 0; const cursors = [];
  const pager = createCatalogPager({ fetcher: async (url, options) => {
    cursors.push(JSON.parse(options.body).cursor); calls++;
    return calls === 1 ? reply(page([1], 2, 'next')) : calls === 2
      ? reply({ error: { message: 'Temporary catalog outage.' } }, 503) : reply(page([2], 2));
  } });
  await pager.reset();
  await assert.rejects(pager.loadMore(), /Temporary catalog outage/);
  assert.equal(pager.getState().items.length, 1); assert.equal(pager.getState().loading, false); assert.equal(pager.getState().hasMore, true);
  assert.match(pager.getState().error, /outage/);
  const recovered = await pager.loadMore();
  assert.equal(recovered.items.length, 2); assert.equal(recovered.error, null);
  assert.deepEqual(cursors, [null, 'next', 'next']);
});

test('empty results are exhausted without adding invented cards', async () => {
  let calls = 0; const pager = createCatalogPager({ fetcher: async () => { calls++; return reply(page([], 0)); } });
  const result = await pager.reset({ query: 'nothing matches' });
  assert.deepEqual(result.items, []); assert.equal(result.hasMore, false);
  await pager.loadMore(); assert.equal(calls, 1);
});

for (const corruption of ['duplicate-page', 'duplicate-existing', 'cursor', 'empty-continuation', 'wrong-total', 'snapshot']) {
  test(`rejects pagination corruption: ${corruption}`, async () => {
    let calls = 0;
    const second = corruption === 'duplicate-page' ? page([2, 2], 3)
      : corruption === 'duplicate-existing' ? page([1, 2], 3)
      : corruption === 'cursor' ? page([2], 3, 'next')
      : corruption === 'empty-continuation' ? page([], 3, 'more')
      : corruption === 'wrong-total' ? page([2], 4, 'more') : page([2, 3], 3, null, 'different-snapshot');
    const pager = createCatalogPager({ fetcher: async () => reply(++calls === 1 ? page([1], 3, 'next') : second) });
    await pager.reset(); await assert.rejects(pager.loadMore(), /invalid response/);
    assert.equal(pager.getState().items.length, 1);
  });
}

for (const [field, value] of [
  ['video', 'javascript:alert(1)'], ['video', 'https://evil.example/video.webm'],
  ['video', 'https://upload.wikimedia.org.evil.example/wikipedia/commons/test.webm'],
  ['video', 'https://thumb.wikimedia.org/wikipedia/commons/test.webm'],
  ['poster', 'https://thumb.wikimedia.org.evil.example/wikipedia/commons/test.jpg'],
  ['poster', './assets/media/../../secret.jpg'], ['poster', 'data:image/svg+xml,evil'],
  ['duration', NaN], ['id', 'unsafe" data-id="oops'], ['avatarColor', 'red;background:url(evil)'],
  ['handle', 'author" onclick="alert(1)'],
]) {
  test(`rejects unsafe or invalid metadata in ${field}: ${String(value)}`, async () => {
    const post = item(1); post[field] = value;
    const pager = createCatalogPager({ fetcher: async () => reply({ ...page([1], 1), items: [post] }) });
    await assert.rejects(pager.reset(), /invalid response/);
    assert.deepEqual(pager.getState().items, []);
  });
}

const MOV = 'video/quicktime; codecs="mp4v"', WEBM = 'video/webm; codecs="vp9, opus"';
const withSources = id => {
  const post = item(id);
  post.sources = [{ src: post.video.replace('.360p.vp9.webm', '.360p.mpeg4.mov'), type: MOV }, { src: post.video, type: WEBM }];
  return post;
};

test('Safari takes the QuickTime source; Chrome and Firefox fall through to WebM', () => {
  const post = withSources(1);
  const safari = type => (type.startsWith('video/') ? 'probably' : '');
  const chrome = type => (type.startsWith('video/quicktime') ? '' : 'probably');
  assert.match(playableSource(post, safari), /\.360p\.mpeg4\.mov$/);
  assert.equal(playableSource(post, chrome), post.video);
  assert.equal(playableSource(post, () => ''), post.video);
  assert.equal(playableSource(item(2), safari), item(2).video);
});

test('ordered sources pass validation and reach the feed', async () => {
  const pager = createCatalogPager({ fetcher: async () => reply({ ...page([1], 1), items: [withSources(1)] }) });
  assert.deepEqual((await pager.reset()).items[0].sources.map(source => source.type), [MOV, WEBM]);
});

for (const [label, change] of [
  ['empty list', post => { post.sources = []; }],
  ['not a list', post => { post.sources = { src: post.video }; }],
  ['foreign host', post => { post.sources[0].src = post.sources[0].src.replace('upload.wikimedia.org', 'evil.example'); }],
  ['unknown type', post => { post.sources[0].type = 'text/html'; }],
  ['injected codecs', post => { post.sources[1].type = 'video/webm; codecs="vp9"><script>'; }],
  ['mov declared as WebM', post => { post.sources[0].type = WEBM; }],
  ['too many', post => { post.sources = Array(5).fill(post.sources[1]); }],
]) {
  test(`rejects invalid sources: ${label}`, async () => {
    const post = withSources(1); change(post);
    const pager = createCatalogPager({ fetcher: async () => reply({ ...page([1], 1), items: [post] }) });
    await assert.rejects(pager.reset(), /invalid response/);
  });
}

test('local credited demo files remain valid catalog records', async () => {
  const post = item(1);
  Object.assign(post, { id: 'coast-sunset', video: './assets/media/coast-sunset.mp4', poster: './assets/media/coast-sunset.jpg',
    source: { provider: 'demo', id: 'coast-sunset', url: 'https://mixkit.co/free-stock-video/example/', license: { name: 'Mixkit Stock Video Free License', url: 'https://mixkit.co/license/#videoFree' }, attribution: 'Credited stock footage', demo: true, kuairandId: null } });
  const pager = createCatalogPager({ fetcher: async () => reply({ ...page([1], 1), items: [post] }) });
  assert.equal((await pager.reset()).items[0].video, './assets/media/coast-sunset.mp4');
});

test('trusted Commons thumbnail host is accepted only for image media', async () => {
  const post = item(1);
  post.poster = post.poster.replace('upload.wikimedia.org', 'thumb.wikimedia.org');
  const pager = createCatalogPager({ fetcher: async () => reply({ ...page([1], 1), items: [post] }) });
  assert.equal((await pager.reset()).items[0].poster, post.poster);
});

test('item lookup uses encoded stable IDs and validates the returned identity and source', async () => {
  const calls = [];
  const pager = createCatalogPager({ fetcher: async (url, options) => { calls.push([url, options]); return reply({ item: item(1) }); } });
  assert.equal((await pager.getItem('commons:1')).id, 'commons:1');
  assert.equal(calls[0][0], '/api/catalog/items/commons%3A1'); assert.equal(calls[0][1].method, 'GET');
  await assert.rejects(pager.getItem('../secret'), /valid catalog item/); assert.equal(calls.length, 1);
  await assert.rejects(pager.getItem('commons:2'), /invalid response/);
});

test('stats uses actual source/category counts and rejects invalid counts', async () => {
  let valid = true;
  const pager = createCatalogPager({ fetcher: async () => reply({ total: 1008, bySource: { demo: 8, wikimedia: valid ? 1000 : -1 }, byCategory: { Nature: 1008 } }) });
  assert.equal((await pager.stats()).total, 1008);
  valid = false; await assert.rejects(pager.stats(), /invalid response/);
});

test('bad filters never reach the server; network errors remain explicit', async () => {
  let calls = 0; const pager = createCatalogPager({ fetcher: async () => { calls++; throw new TypeError('private transport failure'); } });
  await assert.rejects(pager.reset({ category: 'unknown' }), /valid catalog filters/);
  await assert.rejects(pager.reset({ ignored: true }), /valid catalog filters/);
  assert.equal(calls, 0);
  await assert.rejects(pager.reset(), /local preview server/);
  assert.equal(pager.getState().loading, false); assert.equal(pager.getState().items.length, 0);
});
