"""Gold Parquet -> PyTorch batches.

The column roles come from gold/_metadata/feature_spec.json (written by
pipeline/build_dataset.py), so this file hard-codes no feature names.

GoldDataset is an IterableDataset that yields whole batches (a dict of
tensors), so use it with DataLoader(batch_size=None):

    spec = load_spec("data/gold")
    ds = GoldDataset("data/gold/train", spec, batch_size=4096, shuffle=True, seed=42)
    loader = DataLoader(ds, batch_size=None, num_workers=2)
    for epoch in range(n):
        ds.set_epoch(epoch)
        for batch in loader: ...

Each DataLoader worker reads a different subset of the Parquet files, so
every row is read exactly once per epoch. Gold files are already in a
pseudo-random row order; with shuffle=True the file order and the rows
within each file are reshuffled every epoch, deterministically from
(seed, epoch).
"""

import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import torch
from torch.utils.data import IterableDataset, get_worker_info


def load_spec(gold_dir) -> dict:
    return json.loads((Path(gold_dir) / "_metadata" / "feature_spec.json").read_text())


class GoldDataset(IterableDataset):
    """Batches from one gold split. Each batch is a dict:

        categorical: int64   [B, n_categorical]   indices into embedding tables
        numeric:     float32 [B, n_numeric + n_flags]   scaled numerics, then 0/1 flags
        sequence:    int64   [B, history_length]  video indices, 0 = padding
        label:       float32 [B]
    """

    def __init__(self, split_dir, spec: dict, batch_size: int, shuffle: bool = False, seed: int = 0):
        self.files = sorted(Path(split_dir).glob("*.parquet"))
        if not self.files:
            raise FileNotFoundError(f"No Parquet files in {split_dir}")
        self.categorical = [c["name"] for c in spec["categorical"]]
        self.numeric = spec["numeric"] + spec["flags"]
        self.sequence = spec["sequence"]["name"]
        self.history_length = spec["sequence"]["length"]
        self.columns = ["label", *self.categorical, *self.numeric, self.sequence]
        self.batch_size = batch_size
        self.shuffle = shuffle
        self.seed = seed
        self.epoch = 0

    def set_epoch(self, epoch: int) -> None:
        """Call before each epoch so shuffling differs between epochs."""
        self.epoch = epoch

    def _my_files(self) -> list:
        files = list(self.files)
        if self.shuffle:
            order = np.random.default_rng([self.seed, self.epoch]).permutation(len(files))
            files = [files[i] for i in order]
        info = get_worker_info()
        if info is None:
            return files
        # Worker k takes files k, k + n, k + 2n, ... Workers beyond the file
        # count get none, which is fine: every file is still read exactly once.
        return files[info.id::info.num_workers]

    def _read(self, path: Path) -> dict:
        table = pq.read_table(path, columns=self.columns)
        seq = table.column(self.sequence).combine_chunks().flatten().to_numpy()
        return {
            "categorical": np.stack([table.column(c).to_numpy() for c in self.categorical], axis=1).astype(np.int64),
            "numeric": np.stack([table.column(c).to_numpy() for c in self.numeric], axis=1).astype(np.float32),
            "sequence": seq.reshape(table.num_rows, self.history_length).astype(np.int64),
            "label": table.column("label").to_numpy().astype(np.float32),
        }

    def __iter__(self):
        for path in self._my_files():
            arrays = self._read(path)
            n = len(arrays["label"])
            if self.shuffle:
                # Seeded per (seed, epoch, file) so it is the same whichever worker reads it.
                order = np.random.default_rng([self.seed, self.epoch, self.files.index(path)]).permutation(n)
                arrays = {k: v[order] for k, v in arrays.items()}
            for start in range(0, n, self.batch_size):
                yield {k: torch.from_numpy(v[start:start + self.batch_size]) for k, v in arrays.items()}
