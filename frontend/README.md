# Loop — product preview

**Checkpoint 2: DeepFM prediction preview.** A local, responsive short-video
feed with a separate panel connected to the project’s saved recommendation
model. Review this checkpoint before expanding model-backed product behavior.

From the repository root:

```bash
cd frontend
npm run dev
```

Open **http://localhost:4173**. Node.js 20+ is required. This checkpoint has no
npm dependencies to install. Ordinary demo browsing does not need Docker, HDFS
or Python. The new DeepFM panel uses the existing training venv and local model/data.
Use `PORT=4174 npm run dev` if another process owns the default port. Stop the
server with Ctrl+C. Refresh the browser after editing the files.

Try scrolling the video pane, using the arrow keys, or pressing Space to play
and pause. Playback starts muted; only the visible clip plays. Browsers or
reduced-motion preferences can prevent automatic playback; press Play instead.
Search, topic filters, Following, Saved, likes and interest settings are wired
up so the first visual review is usable. Likes, saved clips, follows and interests
are stored in this browser. Comments added in the preview last for this session.
The About dialog explains the demo boundary, and Keyboard shortcuts lists the
available controls.

## Try the saved DeepFM model

1. Start (or restart) the server with `npm run dev` and refresh the page.
2. Click **Try DeepFM** in the header. On a phone, use the sparkle button beside
   Search. The indicator changes to **DeepFM ready** only after a successful load.
3. Choose one of the three recorded profiles and click **Run prediction**.
4. Inspect the eight actual model outputs and expand **Model and data details**
   to see checkpoint provenance. Changing profiles clears the previous results.

The Node server starts a persistent Python inference worker lazily on the first
model request. It uses the repository's `.venv/bin/python`, the frozen selection
in `training/results/phase5_selection.json`, its referenced
`checkpoints/phase55-deepfm-default/best.pt`, and `data/gold/train/` plus the
feature spec. These local artifacts already exist in the development workspace;
datasets and checkpoints remain excluded from Git. A fresh clone must restore
the matching trusted training artifacts first. Python dependencies are in the
root `requirements-train.txt`; see the root README’s training setup.

If the environment, checkpoint or training export is missing or fails provenance
checks, the panel explains that the model is unavailable. Restore the matching
files and restart `npm run dev` before retrying. There are no invented fallback
scores. Demo browsing remains usable without the model connection.

The panel scores a bounded, deterministic selection of **historical training
examples**, with each row's original features and recorded context. It reads no
target column or validation/test split and performs no training. These in-sample
scores demonstrate inference, not accuracy on unseen data, probability calibration,
or live recommendations at one shared request time.

DeepFM is the **model**, while HDFS and the local Parquet export store data. This
checkpoint adds no application database. Browser-local likes/follows do not change
DeepFM weights or these recorded examples. See [the inference contract](../serving/README.md).

## What this preview represents

The eight videos are locally served, credited stock clips. Creators, captions,
comments and engagement counts are sample content. For you sorts this small
catalog by selected interests; Fresh mix rotates the available clips. Neither
uses live DeepFM predictions, and interacting with the demo does not retrain a
model. KuaiRand supplies interaction data rather than a playable video catalog.
See [media attribution](assets/MEDIA.md).

The next iteration depends on feedback about the layout, colors, typography,
video size and navigation. Real media-to-item mapping, candidate retrieval,
feature construction for new requests, live feed ranking, authentication and
shared user activity are later work. The inference panel is now connected;
stock clips remain separate from its KuaiRand item IDs. Training and the pipeline
are unchanged.

## Checks

```bash
npm run check
npm test
```

The 44 Node tests cover feed state, persistence, filtering, static video serving,
the persistent inference bridge, read-only API validation and browser response
validation. Python backend checks run from the repository root:

```bash
.venv/bin/python -m pytest serving/tests -q
```

The 20 backend tests cover checkpoint provenance, exact predictions, frozen
weights, train-only projection, bounded data loading and worker behavior.
The dev server binds only to `127.0.0.1`; it is a local preview server.

Source entry points: `index.html`, `styles.css`, `app.js`, `feed-state.js`,
`model-preview.js`, `deepfm-client.mjs`, `server.mjs` and `assets/catalog.js`.
Assets are bundled for reliable playback. Typography loads
DM Sans and Manrope from Google Fonts when available, with system fallbacks.
Only the inference worker reads the saved model and projected training features;
model files and raw features are never served as static assets or returned by
the API. Held-out data is never opened by this preview.
