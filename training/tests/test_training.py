"""Train-only selection, saved predictions, reproducibility, and CLI integration."""

import copy
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sklearn.metrics import log_loss, roc_auc_score
import torch
from torch import nn

from training import train_deepfm as training
from training.models import LogisticRegression


SPEC = {
    "categorical": [{"name": "category", "vocab_size": 8}],
    "numeric": ["value"], "flags": ["missing"],
    "sequence": {"name": "history", "vocab_size": 10, "pad": 0, "length": 4},
}
ROOT = Path(__file__).resolve().parents[2]


def write_gold(root, splits=("train", "val", "test", "test_random")):
    """Small real Parquet files, with partial batches in both train files."""
    spec = copy.deepcopy(SPEC)
    spec["splits"] = {}
    for split in splits:
        directory = root / split
        directory.mkdir(parents=True)
        sizes = (9, 8) if split == "train" else (7,)
        offset = 0
        for part, size in enumerate(sizes):
            ids = np.arange(offset, offset + size)
            pq.write_table(pa.table({
                "label": (ids % 2).astype(np.int64),
                "category": ids % 8,
                "value": np.where(ids % 2, .5, -1.5).astype(np.float32),
                "missing": ids % 3 == 0,
                "history": [[0, 0, int(i % 9 + 1), int((i + 1) % 9 + 1)] for i in ids],
            }), directory / f"part-{part:05d}.parquet")
            offset += size
        spec["splits"][split] = {"rows": offset}
    metadata = root / "_metadata"
    metadata.mkdir()
    (metadata / "feature_spec.json").write_text(json.dumps(spec))
    return root


def config(**kwargs):
    values = dict(epochs=2, batch_size=4, patience=3, threads=1,
                  embedding_dim=4, hidden_dims=(8,), learning_rate=.01)
    values.update(kwargs)
    return training.TrainConfig(**values)


def assert_metrics(metrics, predictions):
    labels, probabilities = predictions["label"], predictions["probability"]
    assert metrics["rows"] == len(labels)
    assert metrics["auc"] == pytest.approx(roc_auc_score(labels, probabilities), abs=1e-12)
    assert metrics["log_loss"] == pytest.approx(log_loss(labels, probabilities, labels=[0, 1]), abs=1e-12)
    assert metrics["mean_probability"] == pytest.approx(float(probabilities.mean()), abs=1e-12)
    assert metrics["positive_rate"] == pytest.approx(float(labels.astype(np.float64).mean()), abs=1e-12)


@pytest.mark.parametrize("model", ["lr", "deepfm"])
def test_training_saves_best_model_and_independent_metrics(tmp_path, model):
    gold = write_gold(tmp_path / "gold")
    run = tmp_path / "run"
    metrics = training.run_training(gold, run, config(model=model, evaluate_test=True))
    assert json.loads((run / "metrics.json").read_text()) == json.loads(json.dumps(metrics))
    assert metrics["model"] == model and metrics["test_evaluated"] is True
    spec = json.loads((gold / "_metadata/feature_spec.json").read_text())
    assert metrics["feature_spec_sha256"] == hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    assert len(metrics["history"]) == 2
    assert all(epoch["train_rows"] == 17 for epoch in metrics["history"])
    for split in ("val", "test", "test_random"):
        with np.load(run / "predictions" / f"{split}.npz") as saved:
            assert set(saved.files) >= {"label", "probability", "logit"}
            assert saved["probability"].dtype == np.float64
            assert np.isfinite(saved["logit"]).all()
            np.testing.assert_allclose(saved["probability"], np.exp(-np.logaddexp(0., -saved["logit"].astype(np.float64))), atol=1e-15)
            assert_metrics(metrics["final"][split], saved)
    checkpoint = torch.load(run / "best.pt", weights_only=True)
    assert set(checkpoint) >= {"model_state_dict", "optimizer_state_dict", "model_config", "feature_spec", "epoch", "val_metrics"}
    assert checkpoint["epoch"] == metrics["best_epoch"]
    assert checkpoint["val_metrics"] == metrics["best_val"]


@pytest.mark.parametrize("model", ["lr", "deepfm"])
def test_cpu_same_seed_runs_are_identical_and_skip_holdouts(tmp_path, model):
    gold = write_gold(tmp_path / "gold", splits=("train", "val"))
    first = training.run_training(gold, tmp_path / "first", config(model=model, dropout=.2))
    second = training.run_training(gold, tmp_path / "second", config(model=model, dropout=.2))
    assert first["history"] == second["history"]
    assert first["final"] == second["final"]
    assert first["test_evaluated"] is False and set(first["final"]) == {"val"}
    a = torch.load(tmp_path / "first/best.pt", weights_only=True)["model_state_dict"]
    b = torch.load(tmp_path / "second/best.pt", weights_only=True)["model_state_dict"]
    assert all(torch.equal(a[key], b[key]) for key in a)
    assert sorted(path.name for path in (tmp_path / "first/predictions").iterdir()) == ["val.npz"]


def test_early_stop_restores_best_before_holdouts(tmp_path, monkeypatch):
    gold = write_gold(tmp_path / "gold")
    original = training.evaluate
    snapshots, calls = [], []
    controlled_aucs = [.6, .9, .8, .7]

    def controlled_evaluate(model, loader, max_batches=None):
        dataset = getattr(loader, "dataset", loader)
        split = dataset.files[0].parent.name
        calls.append(split)
        metrics, predictions = original(model, loader, max_batches=max_batches)
        if split == "val" and len(snapshots) < len(controlled_aucs):
            snapshots.append({k: v.detach().clone() for k, v in model.state_dict().items()})
            metrics["auc"] = controlled_aucs[len(snapshots) - 1]
        else:
            assert all(torch.equal(value, snapshots[1][key]) for key, value in model.state_dict().items())
        return metrics, predictions

    monkeypatch.setattr(training, "evaluate", controlled_evaluate)
    metrics = training.run_training(gold, tmp_path / "run", config(model="lr", epochs=10, patience=2, evaluate_test=True))
    assert len(metrics["history"]) == 4
    assert metrics["best_epoch"] == metrics["history"][1]["epoch"]
    assert metrics["best_val"]["auc"] == .9
    assert calls[:4] == ["val"] * 4
    assert set(calls[4:]) == {"test", "test_random"}
    assert calls.count("test") == calls.count("test_random") == 1
    checkpoint = torch.load(tmp_path / "run/best.pt", weights_only=True)
    assert all(torch.equal(value, snapshots[1][key]) for key, value in checkpoint["model_state_dict"].items())


def test_train_loss_weights_rows_in_partial_batches(tmp_path, monkeypatch):
    gold = write_gold(tmp_path / "gold", splits=("train", "val"))

    def fixed_logits(self, batch):
        return batch["numeric"][:, 0] + self.numeric.bias.squeeze() * 0

    monkeypatch.setattr(LogisticRegression, "forward", fixed_logits)
    monkeypatch.setattr(torch.optim.Adam, "step", lambda self, closure=None: None)
    metrics = training.run_training(gold, tmp_path / "run", config(model="lr", epochs=1))
    labels = np.arange(17) % 2
    logits = np.where(labels, .5, -1.5)
    expected = np.mean(np.logaddexp(0., logits) - labels * logits)
    assert metrics["history"][0]["train_rows"] == 17
    assert metrics["history"][0]["train_loss"] == pytest.approx(expected, abs=1e-7)


def test_smoke_limits_apply_to_train_and_evaluation(tmp_path):
    gold = write_gold(tmp_path / "gold", splits=("train", "val"))
    metrics = training.run_training(gold, tmp_path / "run",
                                    config(model="lr", epochs=1, max_train_batches=2, max_eval_batches=1))
    assert metrics["history"][0]["train_rows"] == 8
    assert metrics["history"][0]["val"]["rows"] == metrics["final"]["val"]["rows"] == 4


@pytest.mark.parametrize("split", ["train", "val"])
def test_full_run_rejects_feature_spec_row_count_mismatch(tmp_path, split):
    gold = write_gold(tmp_path / "gold", splits=("train", "val"))
    spec_path = gold / "_metadata/feature_spec.json"
    spec = json.loads(spec_path.read_text())
    spec["splits"][split]["rows"] += 1
    spec_path.write_text(json.dumps(spec))
    with pytest.raises(ValueError, match=split):
        training.run_training(gold, tmp_path / "run", config(model="lr", epochs=1))


class NumericLogits(nn.Module):
    def forward(self, batch):
        return batch["numeric"][:, 0]


def test_evaluate_matches_sklearn_for_extreme_logits_and_partial_batches():
    logits = np.array([-1000., -2., 0., 1., 1000.], dtype=np.float32)
    labels = np.array([0., 0., 1., 0., 1.], dtype=np.float32)
    batches = [{"numeric": torch.from_numpy(logits[start:start + 3, None]),
                "label": torch.from_numpy(labels[start:start + 3])} for start in (0, 3)]
    metrics, predictions = training.evaluate(NumericLogits(), batches)
    assert predictions["probability"].dtype == np.float64
    assert_metrics(metrics, predictions)
    np.testing.assert_array_equal(predictions["label"], labels)
    limited, _ = training.evaluate(NumericLogits(), batches, max_batches=1)
    assert limited["rows"] == 3


def test_evaluate_does_not_fetch_beyond_batch_limit():
    def loader():
        yield {"numeric": torch.tensor([[-1.], [1.]]), "label": torch.tensor([0., 1.])}
        raise AssertionError("Requested a batch beyond max_batches")

    metrics, predictions = training.evaluate(NumericLogits(), loader(), max_batches=1)
    assert metrics["rows"] == len(predictions["label"]) == 2


@pytest.mark.parametrize("labels,probabilities", [([], []), ([0, 0], [.1, .2]), ([1, 1], [.8, .9])])
def test_metrics_refuse_empty_or_one_class_evaluation(labels, probabilities):
    with pytest.raises(ValueError):
        training.binary_metrics(np.asarray(labels), np.asarray(probabilities))


def test_evaluate_refuses_empty_loader():
    with pytest.raises(ValueError):
        training.evaluate(NumericLogits(), [])


def test_existing_nonempty_run_directory_is_preserved(tmp_path):
    gold = write_gold(tmp_path / "gold", splits=("train", "val"))
    run = tmp_path / "run"
    run.mkdir()
    marker = run / "existing-result.txt"
    marker.write_text("preserve this result")
    with pytest.raises((ValueError, FileExistsError)):
        training.run_training(gold, run, config(model="lr", epochs=1))
    assert marker.read_text() == "preserve this result"
    assert not (run / "best.pt").exists()


def test_direct_script_cli_help_and_synthetic_smoke(tmp_path):
    command = [sys.executable, str(ROOT / "training/train_deepfm.py")]
    help_result = subprocess.run(command + ["--help"], capture_output=True, text=True, timeout=30)
    assert help_result.returncode == 0, help_result.stderr
    assert "--model" in help_result.stdout
    gold = write_gold(tmp_path / "gold", splits=("train", "val"))
    result = subprocess.run(command + ["--gold-dir", str(gold), "--run-dir", str(tmp_path / "cli"),
                            "--model", "lr", "--epochs", "1", "--batch-size", "4", "--threads", "1"],
                            capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr
    metrics = json.loads((tmp_path / "cli/metrics.json").read_text())
    assert metrics["history"][0]["train_rows"] == 17
    assert metrics["test_evaluated"] is False
