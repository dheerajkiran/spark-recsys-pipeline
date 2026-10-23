"""The leakage control changes labels globally, without touching held-out data."""

import hashlib
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from training.shuffle_labels import prepare_shuffled_gold


def write_source(root):
    spec = {
        "categorical": [{"name": "category", "vocab_size": 32}],
        "numeric": ["value"], "flags": ["missing"],
        "sequence": {"name": "history", "vocab_size": 32, "pad": 0, "length": 3},
        "splits": {"train": {"rows": 18}, "val": {"rows": 7},
                   "test": {"rows": 100}, "test_random": {"rows": 100}},
    }
    for split, sizes in (("train", (5, 7, 6)), ("val", (7,))):
        (root / split).mkdir(parents=True)
        offset = 0
        for part, n in enumerate(sizes):
            ids = np.arange(offset, offset + n)
            # Homogeneous source shards make within-shard shuffling detectable.
            labels = np.array([int(part == 1)] * n) if split == "train" else ids % 2
            table = pa.table({
                "label": labels, "category": ids,
                "value": ids.astype(np.float32) / 10,
                "missing": ids % 3 == 0,
                "history": [[0, int(i), int(i + 1)] for i in ids],
            }).replace_schema_metadata({b"fixture": b"preserve schema metadata"})
            pq.write_table(table, root / split / f"part-{part:05d}.parquet")
            offset += n
    (root / "_metadata").mkdir()
    (root / "_metadata/feature_spec.json").write_text(json.dumps(spec))
    return root


def train_labels(root):
    return np.concatenate([pq.read_table(path, columns=["label"]).column("label").to_numpy()
                           for path in sorted((root / "train").glob("*.parquet"))])


def test_global_permutation_preserves_every_feature_and_label_multiset(tmp_path):
    source = write_source(tmp_path / "source")
    destination = tmp_path / "shuffled"
    original = train_labels(source)
    source_bytes = {path: path.read_bytes() for path in (source / "train").glob("*.parquet")}
    manifest = prepare_shuffled_gold(source, destination, seed=42)
    permutation = np.random.default_rng(42).permutation(len(original))
    actual = train_labels(destination)
    np.testing.assert_array_equal(actual, original[permutation])
    np.testing.assert_array_equal(np.sort(actual), np.sort(original))
    assert not np.array_equal(actual, original)
    for src in sorted((source / "train").glob("*.parquet")):
        before, after = pq.read_table(src), pq.read_table(destination / "train" / src.name)
        assert before.schema.equals(after.schema, check_metadata=True)
        assert before.drop(["label"]).equals(after.drop(["label"]))
        assert src.read_bytes() == source_bytes[src]
    assert manifest["train_rows"] == 18 and manifest["train_files"] == 3
    assert manifest["label_counts"] == {"0": 11, "1": 7}
    assert manifest["permutation_sha256"] == hashlib.sha256(permutation.astype("<i8").tobytes()).hexdigest()
    assert json.loads((destination / "_metadata/shuffle_manifest.json").read_text()) == manifest


def test_same_seed_is_deterministic_and_different_seed_changes_labels(tmp_path):
    source = write_source(tmp_path / "source")
    a, b, c = [tmp_path / name for name in ("a", "b", "c")]
    first = prepare_shuffled_gold(source, a, seed=19)
    assert first == prepare_shuffled_gold(source, b, seed=19)
    prepare_shuffled_gold(source, c, seed=20)
    np.testing.assert_array_equal(train_labels(a), train_labels(b))
    assert not np.array_equal(train_labels(a), train_labels(c))


def test_validation_and_spec_are_copied_exactly_without_opening_holdouts(tmp_path, monkeypatch):
    source = write_source(tmp_path / "source")
    # Invalid Parquet bytes fail immediately if a held-out split is read.
    for split in ("test", "test_random"):
        (source / split).mkdir()
        (source / split / "part-00000.parquet").write_bytes(b"DO NOT READ")
    original_glob = Path.glob

    def guarded_glob(path, pattern):
        assert path.name not in ("test", "test_random"), "Enumerated a held-out split"
        return original_glob(path, pattern)

    monkeypatch.setattr(Path, "glob", guarded_glob)
    destination = tmp_path / "output"
    manifest = prepare_shuffled_gold(source, destination)
    assert set(path.name for path in destination.iterdir()) == {"train", "val", "_metadata"}
    assert manifest["val_rows"] == 7
    for path in (source / "val").iterdir():
        assert path.read_bytes() == (destination / "val" / path.name).read_bytes()
    assert (source / "_metadata/feature_spec.json").read_bytes() == (destination / "_metadata/feature_spec.json").read_bytes()


def test_refuses_same_source_alias_and_preserves_nonempty_destination(tmp_path):
    source = write_source(tmp_path / "source")
    with pytest.raises(ValueError, match="differ"):
        prepare_shuffled_gold(source, source)
    alias = tmp_path / "source-alias"
    alias.symlink_to(source, target_is_directory=True)
    with pytest.raises(ValueError, match="differ"):
        prepare_shuffled_gold(source, alias)
    output = tmp_path / "occupied"
    output.mkdir()
    marker = output / "keep.txt"
    marker.write_text("preserve me")
    with pytest.raises(FileExistsError):
        prepare_shuffled_gold(source, output)
    assert marker.read_text() == "preserve me"
    assert list(output.iterdir()) == [marker]


@pytest.mark.parametrize("split", ["train", "val"])
def test_refuses_stale_spec_before_writing_output(tmp_path, split):
    source = write_source(tmp_path / "source")
    spec_file = source / "_metadata/feature_spec.json"
    spec = json.loads(spec_file.read_text())
    spec["splits"][split]["rows"] += 1
    spec_file.write_text(json.dumps(spec))
    output = tmp_path / "output"
    with pytest.raises(ValueError, match=split):
        prepare_shuffled_gold(source, output)
    assert not output.exists()


def test_direct_cli_can_prepare_synthetic_control(tmp_path):
    source = write_source(tmp_path / "source")
    output = tmp_path / "output"
    script = Path(__file__).resolve().parents[1] / "shuffle_labels.py"
    result = subprocess.run([sys.executable, str(script), "--gold-dir", str(source),
                             "--output-dir", str(output), "--seed", "42"],
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["train_rows"] == 18
    np.testing.assert_array_equal(train_labels(output), train_labels(source)[np.random.default_rng(42).permutation(18)])
