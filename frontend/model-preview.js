/** Read-only browser contract for historical DeepFM examples, never demo scores. */
const API = '/api/deepfm';
const PROFILE_ID = /^\d{1,20}$/;
const SCOPE = 'historical_training_examples';
const TIMEOUT_MS = 30_000;

const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const positiveInteger = value => Number.isSafeInteger(value) && value > 0;
const text = value => typeof value === 'string' && value.trim().length > 0;
const identifier = value => typeof value === 'string' && PROFILE_ID.test(value);

function invalidResponse() {
  return new Error('The DeepFM preview returned an invalid response. Please retry after restarting the preview server.');
}

async function requestJSON(path, fetcher) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    let response;
    try {
      response = await fetcher(path, { method: 'GET', cache: 'no-store', signal: controller.signal });
    } catch {
      if (controller.signal.aborted) throw new Error('The DeepFM preview took too long to respond. Please try again.');
      throw new Error('Cannot connect to the DeepFM preview. Check that the local preview server is running.');
    }
    let body;
    try { body = await response.json(); }
    catch {
      if (controller.signal.aborted) throw new Error('The DeepFM preview took too long to respond. Please try again.');
      throw new Error('The DeepFM preview returned an unreadable response. Please restart the local preview server.');
    }
    if (!response.ok) {
      const message = object(body) && object(body.error) && text(body.error.message)
        ? body.error.message.trim().slice(0, 240)
        : 'The DeepFM preview is unavailable. Check the model setup and try again.';
      throw new Error(message);
    }
    return body;
  } finally {
    clearTimeout(timer);
  }
}

function validateScope(value) {
  if (!object(value) || value.model !== 'DeepFM' || value.source_split !== 'train'
      || value.scope !== SCOPE || !text(value.checkpoint_sha256)) throw invalidResponse();
}

export async function fetchModelPreview(fetcher = globalThis.fetch) {
  const status = await requestJSON(`${API}/status`, fetcher);
  if (!object(status) || !['ready', 'unavailable'].includes(status.state)) throw invalidResponse();
  // Missing model artifacts are an explicit state, not an invitation to invent scores.
  if (status.state === 'unavailable') return { status, profiles: [] };
  validateScope(status);
  if (!positiveInteger(status.selected_epoch) || !text(status.feature_spec_sha256)
      || !positiveInteger(status.profile_count) || !positiveInteger(status.examples_per_profile)
      || status.media_mapping !== false) throw invalidResponse();
  const response = await requestJSON(`${API}/profiles`, fetcher);
  if (!object(response) || !Array.isArray(response.profiles)
      || response.profiles.length !== status.profile_count) throw invalidResponse();
  const ids = new Set();
  for (const profile of response.profiles) {
    if (!object(profile) || !identifier(profile.id) || !text(profile.name)
        || !positiveInteger(profile.example_count) || profile.example_count !== status.examples_per_profile
        || ids.has(profile.id)) throw invalidResponse();
    ids.add(profile.id);
  }
  return { status, profiles: response.profiles };
}

export async function fetchModelScores(profileId, fetcher = globalThis.fetch) {
  if (!identifier(profileId)) throw new Error('Choose a valid recorded profile before running DeepFM.');
  const score = await requestJSON(`${API}/score?profile_id=${encodeURIComponent(profileId)}`, fetcher);
  validateScope(score);
  if (score.profile_id !== profileId || !Array.isArray(score.examples) || !score.examples.length
      || !text(score.explanation)) throw invalidResponse();
  const videos = new Set();
  let previous = Infinity;
  for (const [index, example] of score.examples.entries()) {
    if (!object(example) || !identifier(example.video_id) || videos.has(example.video_id)
        || !Number.isSafeInteger(example.recorded_at_ms) || example.recorded_at_ms < 0
        || typeof example.probability !== 'number' || !Number.isFinite(example.probability)
        || example.probability < 0 || example.probability > 1 || example.probability > previous
        || typeof example.logit !== 'number' || !Number.isFinite(example.logit)
        || example.rank !== index + 1) throw invalidResponse();
    previous = example.probability;
    videos.add(example.video_id);
  }
  return score;
}
