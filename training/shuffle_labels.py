"""Prepare a reproducible shuffled-label control using train and val only.

Train labels are permuted globally once, preserving every feature and row order.
Validation is copied unchanged by default, or independently permuted when
requested. Held-out splits are never read or copied.
"""

import argparse
import hashlib
import json
from pathlib import Path
import shutil

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def _write_permuted_labels(files, labels, destination):
    offset = 0
    for path in files:
        table = pq.read_table(path)
        label_index = table.schema.get_field_index("label")
        label_field = table.schema.field(label_index)
        replacement = pa.array(labels[offset:offset + table.num_rows], type=label_field.type)
        table = table.set_column(label_index, label_field, replacement)
        pq.write_table(table, destination / path.name, compression="snappy")
        offset += table.num_rows


def prepare_shuffled_gold(gold_dir, output_dir, seed=42, permute_validation=False) -> dict:
    """Create fresh gold; optionally permute val independently for a null control."""
    source, destination = Path(gold_dir).resolve(), Path(output_dir).resolve()
    if source == destination:
        raise ValueError("Source and output directories must differ")
    if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
        raise FileExistsError(f"Output directory must be empty: {destination}")
    if type(seed) is not int or seed < 0:
        raise ValueError("seed must be a nonnegative integer")
    if type(permute_validation) is not bool:
        raise ValueError("permute_validation must be a boolean")
    spec_path = source / "_metadata" / "feature_spec.json"
    spec = json.loads(spec_path.read_text())
    train_files = sorted((source / "train").glob("*.parquet"))
    val_files = sorted((source / "val").glob("*.parquet"))
    if not train_files or not val_files:
        raise FileNotFoundError("Source needs train and val Parquet files")
    labels = np.concatenate([pq.read_table(path, columns=["label"]).column("label").to_numpy()
                             for path in train_files])
    if len(labels) != spec["splits"]["train"]["rows"]:
        raise ValueError(f"train: read {len(labels)} rows; feature spec expects {spec['splits']['train']['rows']}")
    if not len(labels) or not np.isin(labels, [0, 1]).all():
        raise ValueError("Train labels must be nonempty and binary")
    val_rows = sum(pq.ParquetFile(path).metadata.num_rows for path in val_files)
    if val_rows != spec["splits"]["val"]["rows"]:
        raise ValueError(f"val: read {val_rows} rows; feature spec expects {spec['splits']['val']['rows']}")
    permutation = np.random.default_rng(seed).permutation(len(labels))
    shuffled = labels[permutation]
    manifest = {
        "source_gold_dir": str(source), "seed": seed,
        "train_rows": int(len(labels)), "train_files": len(train_files),
        "val_rows": val_rows, "val_files": len(val_files),
        "label_counts": {str(label): int(np.count_nonzero(labels == label)) for label in (0, 1)},
        "permutation_sha256": hashlib.sha256(permutation.astype("<i8").tobytes()).hexdigest(),
        "permutation_encoding": "zero-based indices, little-endian int64, sorted train file order",
    }
    shuffled_val = None
    if permute_validation:
        val_labels = np.concatenate([pq.read_table(path, columns=["label"]).column("label").to_numpy()
                                     for path in val_files])
        if not len(val_labels) or not np.isin(val_labels, [0, 1]).all():
            raise ValueError("Validation labels must be nonempty and binary")
        val_permutation = np.random.default_rng([seed, 1]).permutation(len(val_labels))
        shuffled_val = val_labels[val_permutation]
        manifest.update(
            protocol="permuted-train-and-validation-v1", validation_permuted=True,
            val_permutation_sha256=hashlib.sha256(val_permutation.astype("<i8").tobytes()).hexdigest(),
            val_label_counts={str(label): int(np.count_nonzero(val_labels == label)) for label in (0, 1)})
    for name in ("train", "val", "_metadata"):
        (destination / name).mkdir(parents=True, exist_ok=True)
    _write_permuted_labels(train_files, shuffled, destination / "train")
    if shuffled_val is not None:
        _write_permuted_labels(val_files, shuffled_val, destination / "val")
    else:
        for path in val_files:
            shutil.copy2(path, destination / "val" / path.name)
    shutil.copy2(spec_path, destination / "_metadata" / "feature_spec.json")
    (destination / "_metadata" / "shuffle_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--permute-validation", action="store_true",
                        help="Independently permute val targets for a train/val permutation-null control")
    args = parser.parse_args(argv)
    try:
        manifest = prepare_shuffled_gold(args.gold_dir, args.output_dir, args.seed, args.permute_validation)
    except (OSError, ValueError, KeyError) as error:
        parser.error(str(error))
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
