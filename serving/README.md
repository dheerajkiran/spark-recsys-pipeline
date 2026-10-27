# DeepFM inference preview

This checkpoint connects the local UI to the frozen DeepFM model. DeepFM is a
ranking model; HDFS/Parquet provide the data, and the Node server exposes a small
read-only inference API. There is no application database in this checkpoint.

Run `npm run dev` from `frontend/`, then open **Try DeepFM**. Node spawns
`.venv/bin/python serving/preview.py` lazily and communicates using JSON lines.
There is no second port to start, no Python web framework to install and no need
to run containers while inspecting the already exported training data.

## Inputs and scope

`DeepFMPreview` validates the saved checkpoint's SHA256, configuration, selected
epoch and feature spec against `training/results/phase5_selection.json`. It loads
weights on CPU once, sets evaluation/inference mode and disables gradients.
Initialization failure remains sticky until the preview server/worker restarts;
restored or replaced artifacts are never silently accepted by an existing worker.

The loader projects only model features plus user/video IDs and timestamps from
sorted `data/gold/train/*.parquet` shards. It never reads `label`, its source
alias, `val`, `test` or `test_random`. At most 500,000 rows are considered. The
first three eligible numeric user IDs receive eight distinct-item examples,
selected deterministically in chronological order. Each example preserves its
own original recorded context; these are not simultaneous live candidates.

Scoring applies sigmoid to model logits, then sorts by descending probability
and numeric video ID to break ties. The HTTP response contains only the item ID,
recorded timestamp, logit, probability and rank, plus model/source provenance.
It never contains target outcomes, raw feature vectors or model weights.

These are **in-sample historical predictions**, not new evaluation metrics,
calibration claims or live feed personalization. The stock clips in `frontend/`
are not mapped to KuaiRand item IDs. Browser-local likes and follows affect only
the demo UI and do not modify the recorded examples or train the model.

## Read-only API

| GET route | Result |
|---|---|
| `/api/deepfm/status` | Ready/unavailable state, checkpoint identity and historical-source scope |
| `/api/deepfm/profiles` | `{profiles: [{id, name, example_count}]}` |
| `/api/deepfm/score?profile_id=2` | Predictions for an available recorded profile, with provenance |

Use the IDs returned by `/profiles`, rather than assuming a hard-coded user.
Only one numeric `profile_id` is accepted for scoring; extra parameters are
rejected. Unknown profiles return 404, invalid requests 400, and unavailable
inference 503. A readable unavailable status returns 200 with `state: unavailable`.
The bridge uses bounded responses and timeouts, matches concurrent request IDs,
and cleans up its own worker when the preview server closes. No API request
specifies arbitrary model paths or features. Everything binds to loopback.

## Verification

```bash
# repository root
.venv/bin/python -m pytest serving/tests -q

# frontend/
npm run check
npm test
```

Twenty Python tests verify projected source reads, exact independent forward
results, model-state invariance, deterministic ordering, provenance failures,
invalid examples, bounded sampling and worker protocol errors. The 44 Node
tests include API, transport, browser-response and existing feed/server checks.
Desktop/mobile browser verification also scores all three real development
profiles and checks the disconnected-model state. Local evidence is ignored
under `checkpoints/feed-ui-deepfm/`; original training results remain unchanged.
