import test from 'node:test';
import assert from 'node:assert/strict';
import { fetchModelPreview, fetchModelScores } from '../model-preview.js';

const status = {
  state: 'ready', model: 'DeepFM', checkpoint_sha256: 'checkpoint-digest',
  selected_epoch: 1, source_split: 'train', feature_spec_sha256: 'spec-digest',
  profile_count: 2, examples_per_profile: 2,
  scope: 'historical_training_examples', media_mapping: false,
};
const profiles = [
  { id: '123', name: 'Profile 1', example_count: 2 },
  { id: '456', name: 'Profile 2', example_count: 2 },
];
const score = {
  profile_id: '123', source_split: 'train', scope: 'historical_training_examples',
  model: 'DeepFM', checkpoint_sha256: 'checkpoint-digest',
  examples: [
    { video_id: '789', recorded_at_ms: 1_700_000_000_000, probability: .8, logit: 1.38, rank: 1 },
    { video_id: '987', recorded_at_ms: 1_700_000_001_000, probability: .3, logit: -.84, rank: 2 },
  ],
  explanation: 'Recorded examples from training data, not live recommendations.',
};

const response = (body, ok = true) => ({ ok, json: async () => structuredClone(body) });
function fixtures(values) {
  const calls = [];
  const fetcher = async (path, options) => {
    calls.push({ path, options });
    assert.ok(calls.length <= values.length, 'No additional or fallback request expected');
    return response(values[calls.length - 1]);
  };
  return { calls, fetcher };
}

test('loads readiness before profiles and preserves real metadata without caching', async () => {
  const { calls, fetcher } = fixtures([status, { profiles }]);
  assert.deepEqual(await fetchModelPreview(fetcher), { status, profiles });
  assert.deepEqual(calls.map(call => call.path), ['/api/deepfm/status', '/api/deepfm/profiles']);
  for (const { options } of calls) {
    assert.equal(options.method, 'GET');
    assert.equal(options.cache, 'no-store');
    assert.ok(options.signal instanceof AbortSignal);
    assert.equal(options.signal.aborted, false);
  }
});

test('an unavailable model stays explicit and never fetches profiles or fabricates predictions', async () => {
  const unavailable = { state: 'unavailable', message: 'Selected checkpoint is missing.' };
  const { calls, fetcher } = fixtures([unavailable]);
  assert.deepEqual(await fetchModelPreview(fetcher), { status: unavailable, profiles: [] });
  assert.equal(calls.length, 1);
});

test('scores the selected recorded profile and keeps sorted values unchanged', async () => {
  const { calls, fetcher } = fixtures([score]);
  assert.deepEqual(await fetchModelScores('123', fetcher), score);
  assert.equal(calls[0].path, '/api/deepfm/score?profile_id=123');
  assert.equal(calls[0].options.cache, 'no-store');
});

for (const id of ['', 'abc', '123&other=456', '../status', 123, null, '9'.repeat(21)]) {
  test(`invalid profile ${JSON.stringify(id)} is rejected before making a request`, async () => {
    let called = false;
    await assert.rejects(fetchModelScores(id, () => { called = true; }), /Choose a valid recorded profile/);
    assert.equal(called, false);
  });
}

test('backend HTTP errors are surfaced instead of silently switching to demo scores', async () => {
  await assert.rejects(fetchModelScores('123', async () => response({
    error: { code: 'model_unavailable', message: 'The saved DeepFM checkpoint is unavailable.' },
  }, false)), /saved DeepFM checkpoint is unavailable/);
  await assert.rejects(fetchModelPreview(async () => response({}, false)), /preview is unavailable/);
});

test('connection and unreadable-response failures are readable and do not expose transport details', async () => {
  await assert.rejects(fetchModelPreview(async () => { throw new Error('private transport detail'); }), /Cannot connect to the DeepFM preview/);
  await assert.rejects(fetchModelPreview(async () => ({ ok: true, json: async () => { throw new SyntaxError('private JSON detail'); } })), /unreadable response/);
});

const badStatuses = [
  null, {}, { ...status, state: 'loading' }, { ...status, model: 'LR' },
  { ...status, source_split: 'test' }, { ...status, scope: 'live_recommendations' },
  { ...status, media_mapping: true }, { ...status, profile_count: 0 },
  { ...status, examples_per_profile: NaN }, { ...status, selected_epoch: 0 },
];
test('rejects malformed readiness or claims about held-out/live/media-backed inference', async () => {
  for (const value of badStatuses) {
    const { calls, fetcher } = fixtures([value]);
    await assert.rejects(fetchModelPreview(fetcher), /invalid response/);
    assert.equal(calls.length, 1);
  }
});

test('validates profile identity, uniqueness and counts before presenting selectable users', async () => {
  const badProfiles = [
    {}, { profiles: [] }, { profiles: [profiles[0], profiles[0]] },
    { profiles: [profiles[0], { ...profiles[1], id: 'not-a-user' }] },
    { profiles: [profiles[0], { ...profiles[1], example_count: 1 }] },
    { profiles: [profiles[0], { ...profiles[1], name: '' }] },
  ];
  for (const value of badProfiles) {
    const { fetcher } = fixtures([status, value]);
    await assert.rejects(fetchModelPreview(fetcher), /invalid response/);
  }
});

test('rejects nonfinite/out-of-range probabilities and malformed candidate records', async () => {
  const mutations = [
    value => { value.examples[0].probability = NaN; },
    value => { value.examples[0].probability = Infinity; },
    value => { value.examples[0].probability = -0.1; },
    value => { value.examples[0].probability = 1.1; },
    value => { value.examples[0].probability = '0.8'; },
    value => { value.examples[0].logit = NaN; },
    value => { value.examples[0].recorded_at_ms = -1; },
    value => { value.examples[1].video_id = value.examples[0].video_id; },
    value => { value.examples[1].rank = 1; },
    value => { value.examples.reverse(); },
    value => { value.profile_id = '456'; },
    value => { value.source_split = 'test_random'; },
    value => { value.scope = 'live_recommendations'; },
    value => { value.examples = []; },
  ];
  for (const mutate of mutations) {
    const value = structuredClone(score);
    mutate(value);
    await assert.rejects(fetchModelScores('123', async () => response(value)), /invalid response/);
  }
});
