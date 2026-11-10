# CPU ranker image

Build locally from the repository root:

```sh
docker build -f docker/platform/Dockerfile -t loop-ranker:local .
docker run --rm loop-ranker:local
```

The default self-check exercises the existing `training.models.DeepFM` on CPU without a gold export. For a trusted gold export, mount it read-only at `/input/gold`, mount writable directories at `/work` and `/artifacts`, and run `python -m services.ranker.main train --gold-dir /input/gold --run-id local-1`. The wrapper requires explicit absolute paths and calls the existing research training entry point. It does not accept uploaded code or pickle artifacts from users. Training feasibility on the bounded customer CSV path is a later checkpoint.
