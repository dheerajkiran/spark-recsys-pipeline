import { categories, posts as demoPosts } from './assets/catalog.js';
import { loadState, saveState, toggleItem, formatCount } from './feed-state.js';
import { fetchModelPreview, fetchModelScores } from './model-preview.js';
import { createCatalogPager, playableSource } from './catalog-client.js';

const paths = {
  home: '<path d="m3 10 9-7 9 7v10a1 1 0 0 1-1 1h-5v-7H9v7H4a1 1 0 0 1-1-1Z"/>',
  users: '<path d="M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2m20 0v-2a4 4 0 0 0-3-3.87M15 3.13a4 4 0 0 1 0 7.75"/><circle cx="9" cy="7" r="4"/>',
  compass: '<circle cx="12" cy="12" r="9"/><path d="m16 8-2.5 5.5L8 16l2.5-5.5Z"/>',
  bookmark: '<path d="M6 4a1 1 0 0 1 1-1h10a1 1 0 0 1 1 1v17l-6-4-6 4Z"/>',
  heart: '<path d="M20.8 4.6a5.5 5.5 0 0 0-7.8 0L12 5.7l-1.1-1.1a5.5 5.5 0 0 0-7.8 7.8L12 21l8.8-8.6a5.5 5.5 0 0 0 0-7.8Z"/>',
  search: '<circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 5 5"/>',
  sliders: '<path d="M4 7h8m4 0h4M4 17h3m4 0h9"/><circle cx="14" cy="7" r="2"/><circle cx="9" cy="17" r="2"/>',
  sparkles: '<path d="m12 3 2.6 6.4L21 12l-6.4 2.6L12 21l-2.6-6.4L3 12l6.4-2.6ZM20 2v4m-2-2h4"/>',
  refresh: '<path d="M20 7v5h-5M4 17v-5h5"/><path d="M5.4 8a7 7 0 0 1 11.6-3L20 8M4 16l3 3a7 7 0 0 0 11.6-3"/>',
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6m0-10h.01"/>',
  chevrons: '<path d="m9 8 3-3 3 3m-6 8 3 3 3-3"/>',
  sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5"/>',
  'arrow-up-right': '<path d="M6 18 18 6M6 6h12v12"/>',
  keyboard: '<rect x="2" y="5" width="20" height="14" rx="2"/><path d="M6 9h.01M10 9h.01M14 9h.01M18 9h.01M6 12h.01M10 12h.01M14 12h.01M18 12h.01M7 15h10"/>',
  close: '<path d="m6 6 12 12M6 18 18 6"/>',
  play: '<path d="m8 5 11 7-11 7Z"/>',
  pause: '<path d="M8 5v14M16 5v14"/>',
  mute: '<path d="m11 5-6 4H2v6h3l6 4Zm5 4 6 6m0-6-6 6"/>',
  volume: '<path d="m11 5-6 4H2v6h3l6 4Zm4 3a5 5 0 0 1 0 8m3-11a9 9 0 0 1 0 14"/>',
  plus: '<path d="M12 5v14M5 12h14"/>',
  check: '<path d="m5 12 4 4L19 6"/>',
  verified: '<circle cx="12" cy="12" r="9" fill="currentColor" stroke="none"/><path d="m8 12 3 3 5-6" stroke="#fff" stroke-width="2"/>',
  message: '<path d="M21 11.5a8.4 8.4 0 0 1-.9 3.8 8.5 8.5 0 0 1-7.6 4.7 8.4 8.4 0 0 1-3.8-.9L3 21l1.9-5.7a8.4 8.4 0 0 1-.9-3.8 8.5 8.5 0 0 1 4.7-7.6 8.4 8.4 0 0 1 3.8-.9h.5a8.5 8.5 0 0 1 8 8Z"/>',
  share: '<path d="m14 3 7 7-7 7v-5c-5 0-8 2-11 7 0-8 4-12 11-12Z"/>',
  leaf: '<path d="M20 3C8 1 2 9 5 16s16 4 15-13ZM4 21l11-11"/>',
  expand: '<path d="M8 3H3v5m13-5h5v5M3 16v5h5m13-5v5h-5"/>',
};
const icon = (name) => `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${paths[name] || paths.sparkles}</svg>`;
const escape = value => String(value).replace(/[&<>"']/g, character => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[character]));
const $ = selector => document.querySelector(selector);
const feed = $('#feed');
const modal = $('#modal');
let storage;
try { storage = window.localStorage; } catch { /* Browser may disable storage. */ }
let state = loadState(storage);
let view = 'for-you', category = 'All', query = '', activeId = null;
let visible = [], observer, toastTimer, muted = true;
let modalGeneration = 0, modelPreview = null, selectedProfileId = '';
const posts = [...demoPosts];
const postById = new Map(posts.map(post => [post.id, post]));
const pager = createCatalogPager({ pageSize: 12 });
const WINDOW_SIZE = 36;
const WINDOW_STEP = 12;
let catalogGeneration = 0, catalogTotal = 0, windowStart = 0, sharedView = false;
let loadingPage = null, pageError = null, catalogLoading = false, searchTimer;
const userPaused = new Set();
const sessionComments = new Map();
const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
const mediaProbe = document.createElement('video');
const canPlayType = type => mediaProbe.canPlayType(type);

function hydrateIcons(root = document) {
  root.querySelectorAll('[data-icon]').forEach(element => { element.innerHTML = icon(element.dataset.icon); });
}
function persist() {
  if (!saveState(storage, state)) toast('Your changes will last for this visit. Browser storage is unavailable.');
}
function toast(message) {
  clearTimeout(toastTimer);
  $('#toast').textContent = message;
  $('#toast').classList.add('visible');
  toastTimer = setTimeout(() => $('#toast').classList.remove('visible'), 3200);
}
function avatar(post, extra = '') {
  return `<span class="avatar ${extra}" style="background:${escape(post.avatarColor)}">${escape(post.initials)}</span>`;
}
function clock(seconds) {
  const rounded = Math.max(0, Math.floor(Number.isFinite(seconds) ? seconds : 0));
  return `${Math.floor(rounded / 60)}:${String(rounded % 60).padStart(2, '0')}`;
}
function postCard(post) {
  const liked = state.liked.includes(post.id), saved = state.saved.includes(post.id), followed = state.following.includes(post.handle);
  const imported = post.source?.provider === 'wikimedia';
  return `<article class="feed-item" data-post="${escape(post.id)}" aria-label="${escape(post.title)}">
    <div class="video-card">
      <video poster="${escape(post.poster)}" data-src="${escape(playableSource(post, canPlayType))}" preload="none" muted playsinline loop aria-label="${escape(post.title)} video"></video>
      <div class="video-shade"></div>
      <button class="play-surface paused" data-action="play" data-id="${escape(post.id)}" aria-label="Play ${escape(post.title)}"><span class="play-glyph">${icon('play')}</span></button>
      <div class="video-top"><span class="category-tag">${icon('leaf')} ${escape(post.category.toUpperCase())}</span><button data-action="why" data-id="${escape(post.id)}" aria-label="About this video">${icon('info')}</button></div>
      <div class="video-copy"><div class="creator-line">${avatar(post)}<div><span class="creator-name">${escape(post.creator)} ${imported ? '' : icon('verified')}</span><div class="creator-handle">${imported ? 'Wikimedia Commons' : `@${escape(post.handle)}`}</div></div><button class="follow-inline ${followed ? 'followed' : ''}" data-follow="${escape(post.handle)}" aria-pressed="${followed}">${followed ? 'Following' : 'Follow'}</button></div>
        <h2>${escape(post.title)}</h2><p class="caption">${escape(post.description)}</p><div class="video-tags">${post.tags.map(tag => `<span>#${escape(tag)}</span>`).join('')}</div>
        ${imported ? `<button class="catalog-credit" data-action="why" data-id="${escape(post.id)}">${escape(post.source.license.name)} · Credits & source ↗</button>` : ''}
      </div>
      <div class="video-controls"><button class="control-button playback-button" data-action="play" data-id="${post.id}" aria-label="Play video">${icon('play')}</button><span class="video-time">0:00 / ${clock(post.duration)}</span><input class="video-progress" type="range" min="0" max="100" step="0.1" value="0" aria-label="Seek ${escape(post.title)}"><button class="control-button sound-button" data-action="sound" aria-label="${muted ? 'Unmute video' : 'Mute video'}">${icon(muted ? 'mute' : 'volume')}</button><button class="control-button" data-action="fullscreen" data-id="${post.id}" aria-label="Expand video">${icon('expand')}</button></div>
    </div>
    <div class="action-rail"><button class="rail-avatar" data-follow="${post.handle}" aria-label="${followed ? 'Unfollow' : 'Follow'} ${escape(post.creator)}" aria-pressed="${followed}">${avatar(post)}<span class="plus">${followed ? '✓' : '+'}</span></button>
      <button class="rail-action ${liked ? 'selected' : ''}" data-action="like" data-id="${post.id}" aria-label="${liked ? 'Unlike' : 'Like'} ${escape(post.title)}" aria-pressed="${liked}"><span class="action-circle">${icon('heart')}</span><small>${formatCount(post.likes + Number(liked))}</small></button>
      <button class="rail-action" data-action="comments" data-id="${post.id}" aria-label="Comments on ${escape(post.title)}"><span class="action-circle">${icon('message')}</span><small>${post.comments.length}</small></button>
      <button class="rail-action ${saved ? 'selected' : ''}" data-action="save" data-id="${post.id}" aria-label="${saved ? 'Unsave' : 'Save'} ${escape(post.title)}" aria-pressed="${saved}"><span class="action-circle">${icon('bookmark')}</span><small>${saved ? 'Saved' : 'Save'}</small></button>
      <button class="rail-action" data-action="share" data-id="${post.id}" aria-label="Share ${escape(post.title)}"><span class="action-circle">${icon('share')}</span><small>Share</small></button>
      <button class="rail-action rail-why" data-action="why" data-id="${post.id}" aria-label="Why this video?">${icon('sparkles')}</button>
    </div>
  </article>`;
}
function setPlaybackUI(article) {
  const video = article.querySelector('video');
  const paused = video.paused;
  article.querySelector('.play-surface').classList.toggle('paused', paused);
  article.querySelector('.play-surface').setAttribute('aria-label', `${paused ? 'Play' : 'Pause'} ${postById.get(article.dataset.post).title}`);
  const button = article.querySelector('.playback-button');
  button.innerHTML = icon(paused ? 'play' : 'pause');
  button.setAttribute('aria-label', paused ? 'Play video' : 'Pause video');
}
function playActive(force = false) {
  if (!activeId) return;
  const article = feed.querySelector(`[data-post="${activeId}"]`);
  if (!article) return;
  const video = article.querySelector('video');
  if (document.hidden || modal.open || (!force && (userPaused.has(activeId) || state.reducedMotion || prefersReducedMotion))) return;
  if (!video.hasAttribute('src')) video.src = video.dataset.src;
  video.muted = muted;
  video.play().catch(error => { if (error.name !== 'AbortError') setPlaybackUI(article); });
}
function rememberPosts(items) {
  for (const post of items) {
    const old = postById.get(post.id);
    if (old === post) continue;
    if (old) posts[posts.indexOf(old)] = post;
    else posts.push(post);
    postById.set(post.id, post);
  }
}
function catalogFilters() {
  return { view, category, query, preferences: state.preferences.filter(name => name !== 'All'),
    following: state.following, ids: view === 'saved' ? state.saved : view === 'liked' ? state.liked : [] };
}
function disposeCard(article) {
  observer?.unobserve(article);
  const video = article.querySelector('video');
  video.pause();
  video.removeAttribute('src');
  video.load();
  article.remove();
}
function bindCards(articles) {
  for (const article of articles) {
    const video = article.querySelector('video');
    video.addEventListener('play', () => setPlaybackUI(article));
    video.addEventListener('pause', () => setPlaybackUI(article));
    video.addEventListener('timeupdate', () => {
      const duration = Number.isFinite(video.duration) ? video.duration : postById.get(article.dataset.post).duration;
      article.querySelector('.video-time').textContent = `${clock(video.currentTime)} / ${clock(duration)}`;
      article.querySelector('.video-progress').value = duration ? video.currentTime / duration * 100 : 0;
    });
    video.addEventListener('error', () => {
      if (!video.getAttribute('src') || article.querySelector('.media-error')) return;
      const message = document.createElement('div');
      message.className = 'media-error';
      message.innerHTML = '<span>This moment couldn’t load.</span><button data-action="retry">Try again</button><button data-action="skip">Skip this clip →</button>';
      article.querySelector('.video-card').append(message);
    });
    observer?.observe(article);
  }
}
function renderWindow(center, reset = false) {
  const anchor = activeId ? feed.querySelector(`[data-post="${CSS.escape(activeId)}"]`) : null;
  const anchorTop = anchor?.getBoundingClientRect().top;
  let start = windowStart;
  if (reset) start = Math.max(0, center - WINDOW_STEP);
  else if (center < start + 3 && start > 0) start = Math.max(0, start - WINDOW_STEP);
  else if (center >= start + WINDOW_SIZE - 3 && start + WINDOW_SIZE < visible.length) start += WINDOW_STEP;
  const desired = visible.slice(start, start + WINDOW_SIZE);
  const wanted = new Set(desired.map(post => post.id));
  for (const article of feed.querySelectorAll('.feed-item')) if (!wanted.has(article.dataset.post)) disposeCard(article);
  const existing = new Map([...feed.querySelectorAll('.feed-item')].map(article => [article.dataset.post, article]));
  const added = [];
  let next = feed.firstElementChild;
  for (const post of desired) {
    if (existing.has(post.id)) { next = existing.get(post.id).nextElementSibling; continue; }
    const template = document.createElement('template');
    template.innerHTML = postCard(post);
    const article = template.content.firstElementChild;
    feed.insertBefore(article, next);
    added.push(article);
  }
  windowStart = start;
  if (anchor?.isConnected && anchorTop !== undefined) feed.scrollTop += anchor.getBoundingClientRect().top - anchorTop;
  bindCards(added);
}
function updateCatalogStatus() {
  const position = visible.findIndex(post => post.id === activeId);
  $('#position-label').textContent = `${String(position + 1).padStart(2, '0')} / ${catalogTotal.toLocaleString()}`;
  const info = $('#catalog-status');
  info.classList.toggle('catalog-error', Boolean(pageError));
  info.textContent = pageError ? 'Couldn’t load the next clips. Tap to retry.' : catalogLoading ? 'Loading more moments…' : sharedView ? 'Shared moment · open Explore for more' : pager.getState().hasMore ? `${catalogTotal.toLocaleString()} clips · more load as you scroll` : `${catalogTotal.toLocaleString()} clips · all loaded`;
  info.setAttribute('aria-label', pageError ? 'Retry loading catalog' : 'Catalog loading status');
  info.disabled = !pageError;
}
function setActive(id) {
  if (id === activeId || !feed.querySelector(`[data-post="${CSS.escape(id)}"]`)) return;
  feed.querySelectorAll('video').forEach(video => video.pause());
  activeId = id;
  const position = visible.findIndex(post => post.id === id);
  renderWindow(position);
  // Release old media buffers as well as pruning DOM nodes. Only the current
  // clip and immediate neighbours may retain a video source.
  for (const article of feed.querySelectorAll('.feed-item')) {
    const index = visible.findIndex(post => post.id === article.dataset.post);
    const video = article.querySelector('video');
    if (Math.abs(index - position) > 1 && video.hasAttribute('src')) { video.removeAttribute('src'); video.load(); }
  }
  updateCatalogStatus();
  playActive();
  if (!pageError && position >= visible.length - 4) void loadMoreClips();
}
async function loadMoreClips() {
  if (sharedView || catalogLoading || !pager.getState().hasMore) return loadingPage;
  const generation = catalogGeneration;
  catalogLoading = true; pageError = null; updateCatalogStatus();
  const work = (async () => {
    try {
      const result = await pager.loadMore();
      if (generation !== catalogGeneration) return;
      visible = result.items; catalogTotal = result.total; rememberPosts(visible);
      const position = Math.max(0, visible.findIndex(post => post.id === activeId));
      renderWindow(position);
    } catch (error) {
      if (generation !== catalogGeneration) return;
      pageError = error;
    } finally {
      if (generation === catalogGeneration) { catalogLoading = false; updateCatalogStatus(); }
    }
  })();
  loadingPage = work;
  await work;
  if (loadingPage === work) loadingPage = null;
}
async function renderFeed({ sharedItem = null } = {}) {
  const generation = ++catalogGeneration;
  observer?.disconnect();
  for (const article of feed.querySelectorAll('.feed-item')) disposeCard(article);
  visible = []; activeId = null; windowStart = 0; catalogTotal = 0; pageError = null;
  sharedView = Boolean(sharedItem); catalogLoading = true; loadingPage = null;
  const labels = { 'for-you': 'A few things we think you’ll love', following: 'From the people you follow', discover: 'Explore the licensed video catalog', saved: 'Good moments, kept for later', liked: 'Things that made you pause' };
  $('#feed-label').textContent = sharedItem ? 'A shared moment' : query ? `Results for “${query}”` : labels[view];
  feed.innerHTML = `<div class="empty-state catalog-loading" role="status">${icon('sparkles')}<p>Finding your next moment…</p></div>`;
  updateCatalogStatus();
  try {
    const result = sharedItem ? { items: [sharedItem], total: 1 } : await pager.reset(catalogFilters());
    if (generation !== catalogGeneration) return;
    visible = result.items; catalogTotal = result.total; rememberPosts(visible);
    catalogLoading = false;
    if (!visible.length) {
      const empty = query || category !== 'All' ? ['No moments found just yet.', 'Try another topic or a different search.'] : view === 'following' ? ['Find your people.', 'Follow a creator from your feed. Their moments will show up right here.'] : view === 'saved' ? ['Keep the good ones.', 'Tap the bookmark on a video to save a little inspiration for later.'] : ['Your favorites start here.', 'Like a video and you’ll find it in this space.'];
      feed.innerHTML = `<div class="empty-state">${icon(view === 'saved' ? 'bookmark' : 'compass')}<h2>${empty[0]}</h2><p>${empty[1]}</p><button class="primary-button" data-action="reset">Explore your feed ${icon('arrow-up-right')}</button></div>`;
      updateCatalogStatus(); return;
    }
    feed.innerHTML = ''; feed.scrollTop = 0;
    observer = new IntersectionObserver(entries => {
      if (generation !== catalogGeneration) return;
      const next = entries.filter(entry => feed.contains(entry.target) && entry.isIntersecting && entry.intersectionRatio >= .65).sort((a, b) => b.intersectionRatio - a.intersectionRatio)[0];
      if (next) setActive(next.target.dataset.post);
    }, { root: feed, threshold: [.65, .85] });
    renderWindow(0, true); setActive(visible[0].id);
  } catch (error) {
    if (generation !== catalogGeneration) return;
    catalogLoading = false; pageError = error;
    feed.innerHTML = `<div class="empty-state">${icon('info')}<h2>The catalog couldn’t load.</h2><p>${escape(error.message)}</p><button class="primary-button" data-action="catalog-retry">Try again</button></div>`;
    updateCatalogStatus();
  }
}
function updateNav() {
  document.querySelectorAll('[data-view]').forEach(button => {
    const active = button.dataset.view === view;
    button.classList.toggle('active', active);
    if (active) button.setAttribute('aria-current', 'page'); else button.removeAttribute('aria-current');
  });
  $('#saved-count').textContent = String(state.saved.length);
  $('#categories').innerHTML = categories.map(name => `<button class="category-pill ${name === category ? 'active' : ''}" data-category="${name}" aria-pressed="${name === category}">${name === 'All' ? '✦ &nbsp; All' : name}</button>`).join('');
}
function renderDiscovery() {
  $('#feature-image').src = posts.find(post => post.id === 'mountain-air')?.poster || posts[0].poster;
  $('#interest-chips').innerHTML = (state.preferences.length ? state.preferences : ['All']).map(name => `<button class="interest-chip" data-category="${escape(name)}">${escape(name)}</button>`).join('');
  const creators = posts.filter((post, index) => posts.findIndex(other => other.handle === post.handle) === index).slice(0, 3);
  $('#creator-list').innerHTML = creators.map(post => `<div class="creator-suggestion">${avatar(post)}<div class="suggestion-copy"><strong>${escape(post.creator)}</strong><small>${escape(post.category)} & little discoveries</small></div><button class="creator-follow ${state.following.includes(post.handle) ? 'followed' : ''}" data-follow="${post.handle}" aria-label="${state.following.includes(post.handle) ? 'Unfollow' : 'Follow'} ${escape(post.creator)}" aria-pressed="${state.following.includes(post.handle)}">${icon(state.following.includes(post.handle) ? 'check' : 'plus')}</button></div>`).join('');
  const collections = [{ post: posts.find(p => p.id === 'forest-pause'), title: 'Quiet corners', tag: '#slowmoments', category: 'Nature' }, { post: posts.find(p => p.id === 'tokyo-after-dark'), title: 'A different view', tag: '#wanderfar', category: 'Travel' }];
  $('#collection-grid').innerHTML = collections.filter(item => item.post).map(({ post, title, tag, category: topic }) => `<button class="collection-card" data-category="${topic}" aria-label="Explore ${topic}"><img src="${post.poster}" alt="" loading="lazy"><span>${title}<small>${tag}</small></span></button>`).join('');
}
function changeView(next) {
  view = next; category = 'All'; query = '';
  $('#search').value = '';
  clearTimeout(searchTimer);
  updateNav(); return renderFeed();
}
function showModal(title, content, eyebrow = 'A LITTLE MORE YOU') {
  modalGeneration += 1;
  modal.classList.remove('model-modal');
  $('#modal-title').textContent = title;
  $('#modal-eyebrow').textContent = eyebrow;
  $('#modal-content').innerHTML = `<div class="modal-body">${content}</div>`;
  feed.querySelectorAll('video').forEach(video => video.pause());
  if (!modal.open) modal.showModal();
  return modalGeneration;
}
function updateModelIndicator(ready) {
  const launch = $('.model-launch');
  launch.classList.toggle('connected', ready);
  $('.model-launch-label').textContent = ready ? 'DeepFM ready' : 'Try DeepFM';
}
function previewIsCurrent(generation) {
  return modal.open && modalGeneration === generation;
}
function showModelUnavailable(message, generation) {
  if (!previewIsCurrent(generation)) return;
  updateModelIndicator(false);
  $('#modal-content').innerHTML = `<div class="modal-body"><div class="model-notice"><strong>The model isn’t available yet.</strong><p>${escape(message)}</p></div><p>This preview needs the project’s Python training environment, saved DeepFM checkpoint and local gold training data. See the frontend setup notes to restore any missing files, then restart the preview server before trying again.</p><p>The scrolling demo remains available while the connection is offline.</p><button class="primary-button" data-action="deepfm">Try the connection again ${icon('refresh')}</button></div>`;
}
function renderModelPanel(generation) {
  if (!previewIsCurrent(generation)) return;
  const { status, profiles } = modelPreview;
  if (!profiles.some(profile => profile.id === selectedProfileId)) selectedProfileId = profiles[0]?.id || '';
  $('#modal-content').innerHTML = `<div class="modal-body"><div class="model-summary"><span class="model-ready-badge"><span></span>Saved model loaded</span><span class="model-split-badge">RECORDED TRAINING EXAMPLES</span></div><p class="model-intro">DeepFM estimates the chance of a click for a user–video interaction. Choose a recorded profile to run the saved model on its historical examples.</p><div class="model-profile-controls"><label for="model-profile">Recorded profile<select id="model-profile">${profiles.map(profile => `<option value="${escape(profile.id)}" ${profile.id === selectedProfileId ? 'selected' : ''}>${escape(profile.name)} · user ${escape(profile.id)} · ${profile.example_count} examples</option>`).join('')}</select></label><button id="model-score-button" class="primary-button" data-action="deepfm-score" ${profiles.length ? '' : 'disabled'}>Run prediction ${icon('sparkles')}</button></div><div id="model-score-results" aria-live="polite"><div class="model-result-placeholder">${icon('sparkles')}<p>Run a prediction to see real model outputs here.</p></div></div><details class="model-provenance"><summary>Model and data details</summary><dl><div><dt>Model</dt><dd>DeepFM · selected epoch ${status.selected_epoch}</dd></div><div><dt>Checkpoint SHA256</dt><dd class="checkpoint-hash">${escape(status.checkpoint_sha256)}</dd></div><div><dt>Source</dt><dd>Local gold training split</dd></div></dl><p>Each row keeps its original recorded features and timestamp. These examples can have different contexts, so this is an inference preview rather than a live candidate-ranking evaluation.</p></details><p class="model-boundary-note">These scores belong to KuaiRand item IDs, not to the stock clips in your scrolling feed. This panel does not evaluate held-out data or retrain the model.</p></div>`;
}
async function openModelPreview() {
  const generation = showModal('Meet the model.', '<div class="modal-body"><div class="model-loading"><span class="model-loading-dot"></span><p>Connecting to the saved DeepFM model…</p></div><p class="model-boundary-note">The video feed stays available independently of this connection.</p></div>', 'DEEPFM · PREDICTION PREVIEW');
  modal.classList.add('model-modal');
  try {
    const preview = await fetchModelPreview();
    if (!previewIsCurrent(generation)) return;
    if (preview.status.state !== 'ready') return showModelUnavailable(preview.status.message || 'The inference worker could not load the local model.', generation);
    modelPreview = preview;
    updateModelIndicator(true);
    renderModelPanel(generation);
  } catch (error) {
    showModelUnavailable(error.message || 'Unable to reach the local inference service.', generation);
  }
}
async function scoreModelProfile() {
  const generation = modalGeneration;
  const profileId = $('#model-profile')?.value;
  if (!profileId || !previewIsCurrent(generation)) return;
  selectedProfileId = profileId;
  const button = $('#model-score-button');
  const select = $('#model-profile');
  button.disabled = true; select.disabled = true;
  $('#model-score-results').innerHTML = '<div class="model-loading"><span class="model-loading-dot"></span><p>Running DeepFM on the recorded examples…</p></div>';
  try {
    const result = await fetchModelScores(profileId);
    if (!previewIsCurrent(generation)) return;
    if (result.checkpoint_sha256 !== modelPreview.status.checkpoint_sha256) throw new Error('The loaded checkpoint changed. Reopen the model preview before scoring again.');
    if (result.examples.length !== modelPreview.profiles.find(profile => profile.id === profileId)?.example_count) throw new Error('The model returned an incomplete set of examples. Reopen the preview before scoring again.');
    const dateFormat = new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', year: 'numeric', hour: '2-digit', minute: '2-digit', timeZone: 'UTC', hour12: false });
    $('#model-score-results').innerHTML = `<div class="prediction-summary"><strong>${result.examples.length} model predictions</strong><span>Sorted by click estimate</span></div><div class="prediction-table-wrap"><table class="prediction-table"><caption class="visually-hidden">DeepFM scores for recorded training examples, ordered by predicted click likelihood</caption><thead><tr><th scope="col">Item</th><th scope="col">Recorded (UTC)</th><th scope="col">Click estimate</th></tr></thead><tbody>${result.examples.map(example => `<tr><th scope="row"><span class="prediction-rank">${example.rank}</span> Video ${escape(example.video_id)}</th><td>${escape(dateFormat.format(new Date(example.recorded_at_ms)))}</td><td><span class="prediction-value">${(example.probability * 100).toFixed(1)}%</span><span class="prediction-bar" aria-hidden="true"><span style="width:${(example.probability * 100).toFixed(1)}%"></span></span></td></tr>`).join('')}</tbody></table></div><p class="prediction-caveat">These are in-sample historical predictions, not an accuracy measurement or calibrated guarantee of a click.</p>`;
  } catch (error) {
    if (!previewIsCurrent(generation)) return;
    updateModelIndicator(false);
    $('#model-score-results').innerHTML = `<div class="model-notice model-error" role="alert"><strong>Prediction couldn’t finish.</strong><p>${escape(error.message || 'The inference service is unavailable. Try again.')}</p></div>`;
  } finally {
    if (previewIsCurrent(generation)) { button.disabled = false; select.disabled = false; }
  }
}
function showInterests() {
  showModal('What catches your eye?', `<p>Choose a few things you love. Your demo mix starts with these interests.</p><form id="interests-form"><div class="modal-interest-grid">${categories.filter(name => name !== 'All').map(name => `<label class="interest-option"><input type="checkbox" name="interest" value="${name}" ${state.preferences.includes(name) ? 'checked' : ''}> ${name}</label>`).join('')}</div><label class="interest-option"><input type="checkbox" name="reduced" ${state.reducedMotion ? 'checked' : ''}> Pause automatic video playback</label><button class="primary-button" type="submit">Make it my mix ${icon('sparkles')}</button></form>`);
}
function showComments(post) {
  const comments = [...post.comments, ...(sessionComments.get(post.id) || [])];
  showModal('A little conversation.', `${comments.map(comment => `<div class="comment"><span class="avatar">${escape(comment.author.slice(0, 2).toUpperCase())}</span><div><strong>${escape(comment.author)}</strong><p>${escape(comment.text)}</p></div></div>`).join('')}<p class="modal-note">Comments you add stay in this browser session. Existing comments on the eight original demo clips are sample content.</p><form class="comment-form" id="comment-form" data-id="${escape(post.id)}"><input name="comment" required maxlength="400" placeholder="Leave a little kindness…" aria-label="Your comment"><button class="primary-button" type="submit">Post</button></form>`, 'THE CONVERSATION');
}
async function nextVideo(direction) {
  const generation = catalogGeneration;
  const startingId = activeId;
  const index = visible.findIndex(post => post.id === activeId);
  if (direction > 0 && index + 1 >= visible.length && !sharedView && pager.getState().hasMore) await loadMoreClips();
  if (generation !== catalogGeneration || activeId !== startingId) return;
  const target = visible[index + direction];
  if (!target) { toast(pageError ? 'The next page couldn’t load. Tap the catalog status to retry.' : direction > 0 ? 'You’ve reached the current catalog’s end. Refresh the catalog for newly imported clips.' : 'You’re at the start of your mix.'); return; }
  renderWindow(index + direction);
  feed.querySelector(`[data-post="${CSS.escape(target.id)}"]`)?.scrollIntoView({ behavior: prefersReducedMotion ? 'instant' : 'smooth', block: 'start' });
}

document.addEventListener('click', async event => {
  const nav = event.target.closest('[data-view]');
  if (nav) return changeView(nav.dataset.view);
  const topic = event.target.closest('[data-category]');
  if (topic) { category = topic.dataset.category; clearTimeout(searchTimer); updateNav(); void renderFeed(); return; }
  const follow = event.target.closest('[data-follow]');
  if (follow) {
    const handle = follow.dataset.follow;
    state.following = toggleItem(state.following, handle); persist();
    const followed = state.following.includes(handle);
    document.querySelectorAll('[data-follow]').forEach(button => {
      if (button.dataset.follow !== handle) return;
      button.setAttribute('aria-pressed', followed);
      button.classList.toggle('followed', followed);
      if (button.classList.contains('follow-inline')) button.textContent = followed ? 'Following' : 'Follow';
      else if (button.classList.contains('creator-follow')) button.innerHTML = icon(followed ? 'check' : 'plus');
      else button.querySelector('.plus').textContent = followed ? '✓' : '+';
      const creator = posts.find(post => post.handle === handle).creator;
      if (!button.classList.contains('follow-inline')) button.setAttribute('aria-label', `${followed ? 'Unfollow' : 'Follow'} ${creator}`);
    });
    toast(followed ? 'Following. A little more of their world.' : 'Creator unfollowed.');
    if (view === 'following' && !followed) renderFeed();
    return;
  }
  const button = event.target.closest('[data-action]');
  if (!button) return;
  const action = button.dataset.action, post = postById.get(button.dataset.id);
  if (action === 'deepfm') return openModelPreview();
  if (action === 'deepfm-score') return scoreModelProfile();
  if (action === 'interests' || action === 'profile') return showInterests();
  if (action === 'about') return showModal('Welcome to Loop.', '<p>A space for your curiosity. Scroll through short films, follow a fresh perspective, and keep the moments you love.</p><p>The catalog now loads licensed videos in pages. Each imported video keeps its real source, author and license. The eight original local clips still use sample creators, comments and counts. Likes, saves and follows stay in this browser.</p><p><strong>Try DeepFM</strong> opens the saved model and scores separate historical KuaiRand examples. Catalog items have their own IDs; their order here is based on selected topics, not DeepFM scores.</p><p class="modal-note">Checkpoint 3 · A growing playable catalog with source credits and continuous loading. Review it before we connect interaction tracking and model-backed feed ranking.</p>');
  if (action === 'shortcuts') return showModal('A shortcut to good things.', '<div class="shortcut-line">Next / previous video <span><kbd>↓</kbd> <kbd>↑</kbd></span></div><div class="shortcut-line">Play / pause <kbd>Space</kbd></div><div class="shortcut-line">Like this video <kbd>L</kbd></div><div class="shortcut-line">Save this video <kbd>S</kbd></div><div class="shortcut-line">Mute / unmute <kbd>M</kbd></div><div class="shortcut-line">Search <kbd>/</kbd></div><div class="shortcut-line">Close dialog <kbd>Esc</kbd></div>');
  if (action === 'reset') return changeView('for-you');
  if (action === 'scenic') { view = 'discover'; category = 'Travel'; query = ''; $('#search').value = ''; updateNav(); renderFeed(); return; }
  if (action === 'refresh') { void renderFeed(); toast('Refreshing the catalog. New imports can appear in this mix.'); return; }
  if (action === 'catalog-retry') { if (!visible.length || pageError?.status === 409) void renderFeed(); else void loadMoreClips(); return; }
  if (action === 'skip') { void nextVideo(1); return; }
  if (action === 'retry') { const article = button.closest('.feed-item'); button.closest('.media-error').remove(); article.querySelector('video').load(); playActive(true); return; }
  if (action === 'sound') {
    muted = !muted;
    feed.querySelectorAll('video').forEach(video => { video.muted = muted; });
    document.querySelectorAll('.sound-button').forEach(control => { control.innerHTML = icon(muted ? 'mute' : 'volume'); control.setAttribute('aria-label', muted ? 'Unmute video' : 'Mute video'); });
    return;
  }
  if (!post) return;
  const article = feed.querySelector(`[data-post="${post.id}"]`);
  if (action === 'play') {
    const video = article.querySelector('video');
    if (video.paused) { userPaused.delete(post.id); playActive(true); } else { userPaused.add(post.id); video.pause(); }
  } else if (action === 'like' || action === 'save') {
    const key = action === 'like' ? 'liked' : 'saved';
    state[key] = toggleItem(state[key], post.id); persist();
    const selected = state[key].includes(post.id);
    button.classList.toggle('selected', selected);
    button.setAttribute('aria-pressed', selected);
    button.setAttribute('aria-label', `${action === 'like' ? (selected ? 'Unlike' : 'Like') : (selected ? 'Unsave' : 'Save')} ${post.title}`);
    button.querySelector('small').textContent = action === 'like' ? formatCount(post.likes + Number(selected)) : selected ? 'Saved' : 'Save';
    $('#saved-count').textContent = String(state.saved.length);
    if (action === 'save') toast(selected ? 'A good moment, saved for later.' : 'Removed from saved moments.');
    if ((view === 'saved' || view === 'liked') && !selected) renderFeed();
  } else if (action === 'comments') showComments(post);
  else if (action === 'why') showModal('A little behind the moment.', `<p>${escape(post.reason)}</p><p><strong>${escape(post.creator)}</strong> · ${escape(post.location)}</p>${post.source ? `<div class="catalog-attribution"><p>${escape(post.source.attribution)}</p><a href="${escape(post.source.url)}" target="_blank" rel="noopener noreferrer">Original source ↗</a> · <a href="${escape(post.source.license.url)}" target="_blank" rel="noopener noreferrer">${escape(post.source.license.name)} ↗</a></div>` : ''}<p>Your selected topics shape this catalog order. These item IDs are not mapped to KuaiRand, and this feed does not use live DeepFM predictions.</p><button class="primary-button" data-action="interests">Tune your interests ${icon('sliders')}</button>`, 'CREDITS & DISCOVERY');
  else if (action === 'share') {
    const link = new URL(window.location.href); link.hash = `video=${post.id}`;
    try { await navigator.clipboard.writeText(link.href); toast('Link copied. Share a little discovery.'); }
    catch { showModal('Pass a good thing along.', `<p>Copy this link to open this moment in the preview:</p><input class="share-link" aria-label="Video link" readonly value="${escape(link.href)}" style="width:100%;padding:12px;border:1px solid #ddd;border-radius:6px;font-size:11px">`); }
  } else if (action === 'fullscreen') {
    const card = article.querySelector('.video-card');
    try { if (document.fullscreenElement) await document.exitFullscreen(); else if (card.requestFullscreen) await card.requestFullscreen(); else if (article.querySelector('video').webkitEnterFullscreen) article.querySelector('video').webkitEnterFullscreen(); else toast('Full-screen playback isn’t available in this browser.'); }
    catch { toast('Full-screen playback isn’t available in this browser.'); }
  }
});
feed.addEventListener('input', event => {
  if (!event.target.matches('.video-progress')) return;
  const video = event.target.closest('.feed-item').querySelector('video');
  if (Number.isFinite(video.duration)) video.currentTime = Number(event.target.value) / 100 * video.duration;
});
$('#search').addEventListener('input', event => { query = event.target.value.slice(0, 100); clearTimeout(searchTimer); searchTimer = setTimeout(() => void renderFeed(), 180); });
$('#close-modal').addEventListener('click', () => modal.close());
modal.addEventListener('change', event => {
  if (event.target.id !== 'model-profile') return;
  selectedProfileId = event.target.value;
  $('#model-score-results').innerHTML = `<div class="model-result-placeholder">${icon('sparkles')}<p>Run a prediction to score this recorded profile.</p></div>`;
});
modal.addEventListener('click', event => { if (event.target === modal) { const bounds = modal.getBoundingClientRect(); if (event.clientX < bounds.left || event.clientX > bounds.right || event.clientY < bounds.top || event.clientY > bounds.bottom) modal.close(); } });
modal.addEventListener('close', () => playActive());
document.addEventListener('submit', event => {
  if (event.target.id === 'interests-form') {
    event.preventDefault();
    const data = new FormData(event.target);
    state.preferences = data.getAll('interest'); state.reducedMotion = data.has('reduced'); persist();
    modal.close(); renderDiscovery(); changeView('for-you'); toast('Your mix, a little more you.');
  } else if (event.target.id === 'comment-form') {
    event.preventDefault();
    const text = new FormData(event.target).get('comment').trim().slice(0, 400);
    if (!text) return;
    const id = event.target.dataset.id;
    sessionComments.set(id, [...(sessionComments.get(id) || []), { author: 'You', text }].slice(-30));
    showComments(posts.find(post => post.id === id));
    const counter = feed.querySelector(`[data-action="comments"][data-id="${id}"] small`);
    if (counter) counter.textContent = posts.find(post => post.id === id).comments.length + sessionComments.get(id).length;
  }
});
document.addEventListener('keydown', event => {
  if (event.ctrlKey || event.metaKey || event.altKey || event.target.closest('input, textarea, select, [contenteditable="true"]') || modal.open) return;
  const shortcuts = { l: 'like', s: 'save', m: 'sound' };
  if (event.key === '/') { event.preventDefault(); $('#search').focus(); }
  else if (event.key === 'ArrowDown' || event.key === 'ArrowUp') { event.preventDefault(); nextVideo(event.key === 'ArrowDown' ? 1 : -1); }
  else if (event.key === ' ' && !event.target.closest('button, a')) { event.preventDefault(); feed.querySelector(`[data-post="${activeId}"] .playback-button`)?.click(); }
  else if (shortcuts[event.key.toLowerCase()]) { event.preventDefault(); feed.querySelector(`[data-post="${activeId}"] [data-action="${shortcuts[event.key.toLowerCase()]}"]`)?.click(); }
});
document.addEventListener('visibilitychange', () => { if (document.hidden) feed.querySelectorAll('video').forEach(video => video.pause()); else playActive(); });
async function openSharedVideo() {
  const hash = window.location.hash;
  const id = new URLSearchParams(hash.slice(1)).get('video');
  if (!id) return;
  const generation = catalogGeneration;
  const initialView = view, initialCategory = category, initialQuery = query;
  const stillCurrent = () => generation === catalogGeneration && hash === window.location.hash && view === initialView && category === initialCategory && query === initialQuery;
  try {
    const item = await pager.getItem(id);
    if (!stillCurrent()) return;
    clearTimeout(searchTimer);
    view = 'discover'; category = 'All'; query = ''; $('#search').value = ''; updateNav();
    await renderFeed({ sharedItem: item });
  } catch { if (stillCurrent()) toast('That shared clip isn’t available in this catalog.'); }
}
window.addEventListener('hashchange', openSharedVideo);
async function initialize() {
  hydrateIcons(); updateNav(); renderDiscovery();
  const generation = catalogGeneration + 1;
  await renderFeed();
  if (generation === catalogGeneration) await openSharedVideo();
}
void initialize();
