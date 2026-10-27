# Playable video catalog

The catalog stores video **metadata in SQLite** and plays media from its credited
source on demand. This lets the local app browse thousands of items without
downloading full-resolution videos. HDFS/Parquet still hold the training data;
DeepFM remains a separate model preview.

The 2026-10-27 local preview checkpoint contains **2,000 imported Commons videos
plus 8 bundled demo clips**. The metadata database is about 4.4 MiB. These counts
describe this workspace's import; the catalog database is not committed.

## Run and expand

Node.js **22.13+** is required for the built-in SQLite reader. The development
workspace has Node 22.22. Node 22 labels `node:sqlite` experimental and may print
a warning; no npm dependency installation is needed.

From the repository root:

```bash
# Optional small discovery check: no catalog or media writes.
.venv/bin/python -m catalog.import_commons \
  --limit 20 --max-requests 5 --delay 0.5 --dry-run

# Grow to 2,000 total Commons records, using bounded serial API requests.
.venv/bin/python -m catalog.import_commons \
  --limit 2000 --max-requests 600 --delay 0.5

# Start the UI, then open http://localhost:4173.
npm --prefix frontend run dev
```

The importer uses only Python's standard library; the existing `.venv` is
convenient, but PyTorch is not needed for the import itself. No API key is needed.
The default catalog is `data/catalog/catalog.sqlite`, with an import summary at
`data/catalog/import-report.json`. Both are ignored by Git. A fresh checkout
starts with the eight bundled demo clips until an import runs.

`--limit` is a **total Commons-item target**, not a number to add every run.
Rerunning preserves stable IDs and continues stored source queries. The eight
demo items are additional. The current CLI supports targets up to 5,000 and
bounded runs of up to 2,000 API attempts. A run can stop below its target because
of its request budget, exhausted searches or upstream errors; it exits 1 and
records the reason. Already accepted items and query progress remain intact.
Retry later or increase the bounded request budget to continue. Do not treat a
partial import as a completed target.

Use **Refresh catalog** in the feed to include items imported since its current
browsing snapshot. Search, topics, Following, Saved and Liked query the catalog,
including items beyond the browser's initially loaded page. Shared video links
look up the stable item ID directly.

## Source and attribution

The importer uses the official
[Commons video API](https://www.mediawiki.org/wiki/Extension:TimedMediaHandler/API)
with the topical queries in `commons_sources.json`. It stores original file-page
URLs, full sanitized author credits and recognized CC BY, CC BY-SA, CC0 or public
domain license links. Unknown/inconsistent licenses, missing credits and media
without usable video derivatives are skipped. See
[Commons reuse guidance](https://commons.wikimedia.org/wiki/Commons:Reusing_content_outside_Wikimedia)
and each file's own license before further use.

Only short clips (1–120 seconds by default) with thumbnails and two
low-resolution derivatives are accepted: a WebM (or MP4) and Commons'
QuickTime/MPEG-4 `.mov`. Original full-resolution files are not downloaded. Imported media requires internet access when played; an
unavailable clip has retry and skip controls. Each video exposes **Credits &
source**, with its actual author and license. Imported records start with zero
likes and no invented community comments or verified-account badges.

Topic categories are curated search groupings, not model classifications.
A coarse title/description filter skips obvious sexual or graphic subjects;
this is not comprehensive human moderation. Source availability, metadata and
licenses can change, and public media can require additional review for a
production product. The existing eight Mixkit clips remain a distinct demo
source; [Mixkit's terms](https://mixkit.co/terms/) prohibit scripted mass downloads,
so this importer does not bulk scrape that provider.

### Safari playback

Safari plays Commons VP9 WebM audio inside the feed without rendering its
frames, while still reporting the format as playable, so the failure cannot be
feature-detected. The `.mov` derivative renders in Safari, and Chrome and
Firefox report it unplayable. Each imported record therefore lists ordered
`sources` (`.mov` first, then the WebM in `video`), and the player uses the
first type the browser accepts. Items without a `.mov` are skipped
(`no_safari_derivative`) so every clip has a picture in both browsers.

Catalogs imported before this rule can be updated in place. The backfill reads
all pages before writing, applies the changes in one transaction, and removes
items without a `.mov`; rerun the importer afterwards to restore the target:

```bash
.venv/bin/python -m catalog.import_commons --backfill-sources --dry-run
.venv/bin/python -m catalog.import_commons --backfill-sources
.venv/bin/python -m catalog.import_commons --limit 2000 --max-requests 200 --delay 0.5
```

## Identity, storage and paging

- Shared schema: `schema.sql`; Node reader: `frontend/catalog-store.mjs`.
- Imported IDs use `commons:<pageid>`, with uniqueness enforced on both the
  catalog ID and `(source, source_id)`. No Commons item is assigned a KuaiRand ID.
- Browser metadata arrives in pages of 12. SQL filters and bounded limits run
  in the database; the server does not load the whole catalog to answer a page.
- Cursors bind the query and a maximum-row snapshot. New imports do not move
  rows inside an existing feed. Changes/deletions to existing records invalidate
  old cursors explicitly rather than skipping or duplicating records silently.
- The browser retains at most 36 rendered video cards, restores earlier cards
  when scrolling backward, and releases distant video sources. It stops at the
  catalog's real end rather than repeating clips to simulate more inventory.
- Local likes, follows and interests are browser state. This checkpoint does
  not retrain DeepFM or make it rank these independently identified videos.

The catalog APIs share the local preview server:

| Route | Purpose |
|---|---|
| `POST /api/catalog/query` | Read-only paginated search/filter query in JSON |
| `GET /api/catalog/stats` | Actual source/category counts |
| `GET /api/catalog/items/<encoded-id>` | One record and its provenance |

The query accepts `view`, `category`, `query`, `preferences`, `following`, `ids`,
`limit` and `cursor`; unsupported fields and oversized requests are rejected.
Database files and model artifacts are never exposed through static serving.

## Verification

```bash
npm --prefix frontend run check
npm --prefix frontend test
.venv/bin/python -m pytest catalog/tests serving/tests -q
```

Tests cover importer licensing/normalization, bounded retries, resumable atomic
writes, deduplication, SQLite pagination under concurrent imports, browser paging
and error handling, and the existing media/model interfaces. Browser checks also
cover long forward/backward browsing, actual remote playback, source credit links,
saved/shared imported clips, mobile layout and the DeepFM preview. Local evidence
and import logs are under the ignored `checkpoints/catalog-preview/` paths.

Checkpoint verification: **94 Node tests and 71 Python tests pass**. The recorded
Chrome run checks 75 distinct clips across multiple pages and backward windows,
remote playback, credits, saved/shared items, mobile layout and model inference.
