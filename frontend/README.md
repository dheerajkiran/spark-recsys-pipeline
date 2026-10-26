# Loop — product preview

**Checkpoint 1: first visual review.** A local, responsive short-video feed for
the recommendation project. Review this layout and browsing experience before
we expand or connect the product to the trained model.

From the repository root:

```bash
cd frontend
npm run dev
```

Open **http://localhost:4173**. Node.js 20+ is required. This checkpoint has no
npm dependencies to install and does not need Docker, HDFS or the Python venv.
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

## What this preview represents

The eight videos are locally served, credited stock clips. Creators, captions,
comments and engagement counts are sample content. For you sorts this small
catalog by selected interests; Fresh mix rotates the available clips. Neither
uses live DeepFM predictions, and interacting with the demo does not retrain a
model. KuaiRand supplies interaction data rather than a playable video catalog.
See [media attribution](assets/MEDIA.md).

The next iteration depends on feedback about the layout, colors, typography,
video size and navigation. Real media-to-item mapping, candidate retrieval,
feature construction, online model serving, authentication and shared user
activity are later work. The existing training and pipeline are unchanged.

## Checks

```bash
npm run check
npm test
```

The 17 Node tests cover feed state, persistence, filtering and the local HTTP
server, including byte ranges for video seeking and confinement to this folder.
The dev server binds only to `127.0.0.1`; it is a local preview server.

Source entry points: `index.html`, `styles.css`, `app.js`, `feed-state.js`, and
`assets/catalog.js`. Assets are bundled for reliable playback. Typography loads
DM Sans and Manrope from Google Fonts when available, with system fallbacks.
No production model or held-out data is read by this app.
