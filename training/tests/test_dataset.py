"""Loader tests. Run with the training venv: .venv/bin/python -m pytest training/tests"""

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from torch.utils.data import DataLoader

from training.dataset import GoldDataset, load_spec

L = 4
SPEC = {
    "label": "is_click",
    "categorical": [{"name": "user_idx", "vocab_size": 100}, {"name": "tab_idx", "vocab_size": 5}],
    "numeric": ["x"],
    "flags": ["x_missing"],
    "sequence": {"name": "hist_video_idx", "vocab_size": 50, "length": L, "pad": 0},
}


def write_gold(root: Path, sizes=(10, 7, 5)) -> Path:
    """Gold-like split: row r has user_idx = r, so every row is identifiable."""
    split = root / "train"
    split.mkdir(parents=True)
    r = 0
    for i, n in enumerate(sizes):
        ids = list(range(r, r + n))
        pq.write_table(pa.table({
            "label": [i % 2 for i in ids],
            "user_idx": ids,
            "tab_idx": [i % 5 for i in ids],
            "x": [float(i) / 10 for i in ids],
            "x_missing": [i % 3 == 0 for i in ids],
            "hist_video_idx": [[0] * (L - 1) + [i % 50] for i in ids],
            "user_id": ids,                       # not a feature: must be ignored
        }), split / f"part-{i:05d}.parquet")
        r += n
    (root / "_metadata").mkdir()
    (root / "_metadata" / "feature_spec.json").write_text(json.dumps(SPEC))
    return split


def ids_seen(ds, workers=0):
    loader = DataLoader(ds, batch_size=None, num_workers=workers)
    return [int(i) for b in loader for i in b["categorical"][:, 0]]


# Each DataLoader worker is a new process (macOS "spawn") that re-imports torch,
# a few seconds each, so only the distinct cases are covered: in-process,
# a normal multi-worker split, and more workers than files.
@pytest.mark.parametrize("workers,shuffle", [(0, False), (0, True), (2, True), (4, False)])
def test_every_row_exactly_once(tmp_path, workers, shuffle):
    # 4 workers for 3 files: one worker gets no files, and nothing may be lost.
    ds = GoldDataset(write_gold(tmp_path), SPEC, batch_size=4, shuffle=shuffle, seed=1)
    seen = ids_seen(ds, workers)
    assert sorted(seen) == list(range(22))


def test_batch_layout_and_dtypes(tmp_path):
    ds = GoldDataset(write_gold(tmp_path), SPEC, batch_size=4)
    b = next(iter(ds))
    assert b["categorical"].shape == (4, 2) and str(b["categorical"].dtype) == "torch.int64"
    assert b["numeric"].shape == (4, 2) and str(b["numeric"].dtype) == "torch.float32"
    assert b["sequence"].shape == (4, L) and str(b["sequence"].dtype) == "torch.int64"
    assert b["label"].shape == (4,) and str(b["label"].dtype) == "torch.float32"
    # Columns line up row by row: row 3 has user 3, tab 3, x 0.3, flag 1, last video 3.
    assert b["categorical"][3].tolist() == [3, 3]
    assert b["numeric"][3].tolist() == pytest.approx([0.3, 1.0])
    assert b["sequence"][3].tolist() == [0, 0, 0, 3]
    assert b["label"][3].item() == 1.0


def test_unshuffled_order_and_partial_batches(tmp_path):
    ds = GoldDataset(write_gold(tmp_path), SPEC, batch_size=4)
    sizes = [len(b["label"]) for b in ds]
    assert sizes == [4, 4, 2, 4, 3, 4, 1]      # batches never cross a file boundary
    assert ids_seen(ds) == list(range(22))


def test_shuffle_is_deterministic_per_epoch_and_changes_between_epochs(tmp_path):
    split = write_gold(tmp_path)
    a = GoldDataset(split, SPEC, batch_size=4, shuffle=True, seed=7)
    b = GoldDataset(split, SPEC, batch_size=4, shuffle=True, seed=7)
    first = ids_seen(a)
    assert first == ids_seen(b)                 # same seed and epoch: same order
    assert first != list(range(22))             # actually shuffled
    a.set_epoch(1)
    assert ids_seen(a) != first                 # new epoch: new order
    assert sorted(ids_seen(a)) == list(range(22))


def test_missing_split_fails_clearly(tmp_path):
    with pytest.raises(FileNotFoundError):
        GoldDataset(tmp_path / "nope", SPEC, batch_size=4)


GOLD = Path(__file__).resolve().parents[2] / "data" / "gold"


@pytest.mark.skipif(not (GOLD / "_metadata" / "feature_spec.json").exists(),
                    reason="real gold not exported (./scripts/export_gold.sh)")
@pytest.mark.parametrize("split", ["train", "val", "test", "test_random"])
def test_real_gold_complete_and_within_vocabularies(split):
    spec = load_spec(GOLD)
    ds = GoldDataset(GOLD / split, spec, batch_size=65536)
    rows, pos = 0, 0.0
    cat_max = np.zeros(len(spec["categorical"]), dtype=np.int64)
    seq_max = 0
    for b in DataLoader(ds, batch_size=None):   # in-process; workers are covered above
        rows += len(b["label"])
        pos += b["label"].sum().item()
        cat_max = np.maximum(cat_max, b["categorical"].max(dim=0).values.numpy())
        seq_max = max(seq_max, int(b["sequence"].max()))
        assert b["categorical"].min() >= 0 and not b["numeric"].isnan().any()
    assert rows == spec["splits"][split]["rows"]
    sizes = np.array([c["vocab_size"] for c in spec["categorical"]])
    assert (cat_max < sizes).all(), dict(zip([c["name"] for c in spec["categorical"]], cat_max))
    assert seq_max < spec["sequence"]["vocab_size"]
    assert 0 < pos < rows
