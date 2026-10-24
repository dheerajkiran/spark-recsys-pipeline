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


def write_source(root, equal_val=False):
    spec = {
        "categorical": [{"name": "category", "vocab_size": 32}],
        "numeric": ["value"], "flags": ["missing"],
        "sequence": {"name": "history", "vocab_size": 32, "pad": 0, "length": 3},
        "splits": {"train": {"rows": 18}, "val": {"rows": 18 if equal_val else 7},
                   "test": {"rows": 100}, "test_random": {"rows": 100}},
    }
    for split, sizes in (("train", (5, 7, 6)), ("val", (5, 7, 6) if equal_val else (7,))):
        (root / split).mkdir(parents=True)
        offset = 0
        for part, n in enumerate(sizes):
            ids = np.arange(offset, offset + n)
            # Homogeneous source shards make within-shard shuffling detectable.
            labels = np.array([int(part == 1)] * n) if split == "train" or equal_val else ids % 2
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


def split_labels(root, split):
    return np.concatenate([pq.read_table(path, columns=["label"]).column("label").to_numpy()
                           for path in sorted((root / split).glob("*.parquet"))])


def train_labels(root):
    return split_labels(root, "train")


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


@pytest.mark.parametrize("permute_validation", [False, True])
def test_validation_and_spec_preserved_without_opening_holdouts(tmp_path, monkeypatch, permute_validation):
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
    manifest = prepare_shuffled_gold(source, destination, permute_validation=permute_validation)
    assert set(path.name for path in destination.iterdir()) == {"train", "val", "_metadata"}
    assert manifest["val_rows"] == 7
    for path in (source / "val").iterdir():
        if permute_validation:
            assert pq.read_table(path).drop(["label"]).equals(
                pq.read_table(destination / "val" / path.name).drop(["label"]), check_metadata=True)
        else:
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


@pytest.mark.parametrize("permute_validation", [False, True])
def test_direct_cli_can_prepare_synthetic_control(tmp_path, permute_validation):
    source = write_source(tmp_path / "source")
    output = tmp_path / "output"
    script = Path(__file__).resolve().parents[1] / "shuffle_labels.py"
    command = [sys.executable, str(script), "--gold-dir", str(source),
               "--output-dir", str(output), "--seed", "42"]
    if permute_validation:
        command.append("--permute-validation")
    result = subprocess.run(command,
                            capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["train_rows"] == 18
    np.testing.assert_array_equal(train_labels(output), train_labels(source)[np.random.default_rng(42).permutation(18)])
    if permute_validation:
        np.testing.assert_array_equal(split_labels(output, "val"),
            split_labels(source, "val")[np.random.default_rng([42, 1]).permutation(7)])


def test_validation_permutation_is_global_independent_and_preserves_features(tmp_path):
    source = write_source(tmp_path / "source", equal_val=True)
    source_bytes = {path: path.read_bytes() for split in ("train", "val") for path in (source / split).iterdir()}
    output, repeat, legacy = [tmp_path / name for name in ("output", "repeat", "legacy")]
    manifest = prepare_shuffled_gold(source, output, seed=42, permute_validation=True)
    assert manifest == prepare_shuffled_gold(source, repeat, seed=42, permute_validation=True)
    legacy_manifest = prepare_shuffled_gold(source, legacy, seed=42)
    train_order = np.random.default_rng(42).permutation(18)
    val_order = np.random.default_rng([42, 1]).permutation(18)
    assert not np.array_equal(train_order, val_order)
    for split, order in (("train", train_order), ("val", val_order)):
        original, shuffled = split_labels(source, split), split_labels(output, split)
        np.testing.assert_array_equal(shuffled, original[order])
        np.testing.assert_array_equal(shuffled, split_labels(repeat, split))
        np.testing.assert_array_equal(np.sort(shuffled), np.sort(original))
        assert not np.array_equal(shuffled, original)
        for path in (source / split).iterdir():
            before, after = pq.read_table(path), pq.read_table(output / split / path.name)
            assert before.schema.equals(after.schema, check_metadata=True)
            assert before.drop(["label"]).equals(after.drop(["label"]))
            assert path.read_bytes() == source_bytes[path]
    assert not np.array_equal(train_labels(output), split_labels(output, "val"))
    np.testing.assert_array_equal(train_labels(output), train_labels(legacy))
    assert manifest["permutation_sha256"] == legacy_manifest["permutation_sha256"]
    assert manifest["protocol"] == "permuted-train-and-validation-v1"
    assert manifest["validation_permuted"] is True
    assert manifest["val_label_counts"] == {"0": 11, "1": 7}
    assert manifest["val_permutation_sha256"] == hashlib.sha256(val_order.astype("<i8").tobytes()).hexdigest()
    assert "validation_permuted" not in legacy_manifest and "protocol" not in legacy_manifest
    assert json.loads((output / "_metadata/shuffle_manifest.json").read_text()) == manifest
