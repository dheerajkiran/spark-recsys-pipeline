# Loop — product preview

**Checkpoint 3: growing playable catalog.** A local, responsive video feed with
database-backed pagination, credited Commons media, and a separate panel connected
to the saved DeepFM model. Review this checkpoint before expanding model-backed
product behavior.

From the repository root:

```bash
cd frontend
npm run dev
```

Open **http://localhost:4173**. Node.js **22.13+** is required. This checkpoint has no
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
The About dialog explains the source/model boundaries, and Keyboard shortcuts
lists the available controls.

## Grow the playable catalog

From the repository root:

```bash
.venv/bin/python -m catalog.import_commons \
  --limit 2000 --max-requests 600 --delay 0.5
```

The importer stores metadata and attribution in `data/catalog/catalog.sqlite`;
videos stream from the credited source when played. It does not fill disk with
thousands of original videos. Rerun to resume toward the same total target;
the CLI report states whether the target was reached. A fresh clone shows only
the eight bundled demo items until an import runs. See the
[catalog guide](../catalog/README.md) for license policy, limits and recovery.

Click **Refresh catalog** after an import. The feed loads the next page as you
approach the end of the loaded clips. Only 36 video cards stay rendered, so long
sessions do not keep adding players to the page. Going backward restores prior
clips. It reaches a real end when all matching catalog items have been browsed;
it does not repeat the same clips to pretend the catalog is infinite.

Search and library views query all matching catalog records, rather than only
the first page. Imported clips show real credits/license links and have no fake
engagement counts. They require internet access for playback; use retry or skip
if a source is temporarily unavailable.

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

DeepFM is the **model**. HDFS and local Parquet store training data; SQLite now
stores playable catalog metadata. Browser-local likes/follows do not change
DeepFM weights or its recorded examples. See [the inference contract](../serving/README.md).

## What this preview represents

The original eight locally served stock clips have fictional demo creators,
captions, comments and engagement counts; see [media attribution](assets/MEDIA.md).
Imported Commons records carry their own real author, source and license.
For you prioritizes selected topic groups, and Refresh catalog starts a new
browsing snapshot. No catalog item is mapped to a KuaiRand training ID, and the
scrolling order does not use DeepFM predictions. Interacting here does not retrain
a model. The separate DeepFM panel continues to score historical KuaiRand examples.

The next iteration depends on feedback about the layout, colors, typography,
video size, navigation and catalog behavior. The catalog gives playable items
stable IDs and metadata. Interaction storage, candidate retrieval, feature
construction/model adaptation, live feed ranking, authentication and shared user
activity are later work. Training and the Spark pipeline remain unchanged.

## Checks

```bash
npm run check
npm test
```

Node tests cover catalog storage and paging, feed state, persistence, filters,
static video serving, the inference bridge, API limits and browser response
validation. Python importer/backend checks run from the repository root:

```bash
.venv/bin/python -m pytest catalog/tests serving/tests -q
```

Importer tests cover per-item metadata, licenses and credits, deduplication,
resume safety and upstream retry budgets. Backend tests retain checkpoint
provenance, exact predictions, frozen weights and train-only input checks.
The dev server binds only to `127.0.0.1`; it is a local preview server.

Source entry points: `index.html`, `styles.css`, `app.js`, `feed-state.js`,
`catalog-client.js`, `catalog-store.mjs`, `model-preview.js`, `deepfm-client.mjs`,
`server.mjs` and `assets/catalog.js`. The original demo assets are bundled;
imported media streams from Wikimedia. Typography loads
DM Sans and Manrope from Google Fonts when available, with system fallbacks.
Only the inference worker reads the saved model and projected training features;
model files and raw features are never served as static assets or returned by
the API. Held-out data is never opened by this preview.
