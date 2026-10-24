"""Fixed control budgets, fault sensitivity, and validation-label isolation."""

from dataclasses import asdict
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sklearn.metrics import roc_auc_score
import torch
from torch import nn

from training import permutation_control as control
from training import train_deepfm as training
from training.dataset import GoldDataset
from training.tests.test_training import config, write_gold


@pytest.fixture
def selected(tmp_path):
    gold = write_gold(tmp_path / "gold")
    spec = training.load_spec(gold)
    spec_sha = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    models = {}
    # Real fitted reference states, with distinct frozen terminal budgets.
    for name, budget in (("lr", 2), ("deepfm", 1)):
        cfg = config(model=name, epochs=3, dropout=.2)
        torch.set_num_threads(cfg.threads)
        torch.manual_seed(cfg.seed)
        model = training.make_model(spec, cfg)
        optimizer = torch.optim.Adam(model.parameters(), lr=cfg.learning_rate)
        dataset = GoldDataset(gold / "train", spec, cfg.batch_size, shuffle=True, seed=cfg.seed)
        for epoch in range(budget):
            dataset.set_epoch(epoch)
            training.train_epoch(model, optimizer, training.make_loader(dataset, cfg))
        val, _ = training.evaluate(model, training.make_loader(GoldDataset(gold / "val", spec, cfg.batch_size), cfg))
        reference = tmp_path / f"selected-{name}"
        reference.mkdir()
        checkpoint = reference / "best.pt"
        torch.save({"model_state_dict": model.state_dict(), "model_config": asdict(cfg),
                    "feature_spec": spec, "epoch": budget, "val_metrics": val}, checkpoint)
        models[name] = {"run_dir": str(reference), "config": asdict(cfg), "best_epoch": budget,
                        "checkpoint_sha256": hashlib.sha256(checkpoint.read_bytes()).hexdigest(),
                        "feature_spec_sha256": spec_sha, "val": val}
    selection = tmp_path / "selection.json"
    selection.write_text(json.dumps({"selection_data": "train and val only", "models": models}, indent=2))
    return gold, selection


def test_frozen_budgets_only_terminal_validation_and_no_holdouts(selected, tmp_path, monkeypatch):
    gold, selection = selected
    run = tmp_path / "control"
    original_bytes = selection.read_bytes()
    frozen = json.loads(original_bytes)
    before = {name: (Path(item["run_dir"]) / "best.pt").read_bytes()
              for name, item in frozen["models"].items()}
    # Accessing either real hold-out directory would fail immediately.
    for split in ("test", "test_random"):
        for path in (gold / split).glob("*.parquet"):
            path.write_bytes(b"DO NOT READ HELD-OUT DATA")
    original_glob = Path.glob

    def guard_glob(path, pattern):
        assert path.name not in ("test", "test_random")
        return original_glob(path, pattern)

    monkeypatch.setattr(Path, "glob", guard_glob)
    train_epochs, evaluations = [], []
    original_train, original_evaluate = control.train_epoch, control.evaluate

    def train(model, optimizer, loader, max_batches=None):
        protocol = json.loads((run / "protocol.json").read_text())
        assert (run / "reference_selection.json").read_bytes() == original_bytes
        assert protocol["budgets"] == {"lr": 2, "deepfm": 1}
        assert protocol["selection_rule"] == "fixed_terminal_epoch"
        train_epochs.append(type(model).__name__)
        return original_train(model, optimizer, loader, max_batches)

    def evaluate(model, loader, max_batches=None):
        name = "lr" if isinstance(model, training.LogisticRegression) else "deepfm"
        assert train_epochs.count(type(model).__name__) == frozen["models"][name]["best_epoch"]
        assert (run / name / "final.pt").exists()
        assert loader.dataset.files[0].parent.name == "val"
        evaluations.append(name)
        return original_evaluate(model, loader, max_batches)

    monkeypatch.setattr(control, "train_epoch", train)
    monkeypatch.setattr(control, "evaluate", evaluate)
    result = control.run_permutation_control(gold, run, selection)
    assert evaluations == ["lr", "deepfm"]
    assert len(train_epochs) == 3
    assert not (run / "gold/test").exists() and not (run / "gold/test_random").exists()
    assert selection.read_bytes() == original_bytes
    for name, model in result["models"].items():
        assert model["epochs"] == len(model["history"]) == frozen["models"][name]["best_epoch"]
        assert all(epoch["train_rows"] == 17 for epoch in model["history"])
        assert model["validation"]["rows"] == 7
        assert not any("val" in epoch for epoch in model["history"])
        checkpoint = torch.load(run / name / "final.pt", weights_only=True)
        assert checkpoint["epoch"] == model["epochs"]
        assert checkpoint["model_config"]["epochs"] == 3
        assert (Path(frozen["models"][name]["run_dir"]) / "best.pt").read_bytes() == before[name]
    assert json.loads((run / "results.json").read_text()) == json.loads(json.dumps(result))


@pytest.mark.parametrize("field,value", [("evaluate_test", True), ("max_train_batches", 1),
                                         ("max_eval_batches", 1), ("num_workers", 1)])
def test_invalid_selected_configs_rejected_before_training(selected, tmp_path, monkeypatch, field, value):
    gold, selection = selected
    manifest = json.loads(selection.read_text())
    manifest["models"]["lr"]["config"][field] = value
    selection.write_text(json.dumps(manifest))
    monkeypatch.setattr(control, "train_epoch", lambda *a, **k: pytest.fail("Must reject before training"))
    with pytest.raises(ValueError, match="full validation-only"):
        control.run_permutation_control(gold, tmp_path / "invalid", selection)


@pytest.mark.parametrize("field,value", [("checkpoint_sha256", "0" * 64), ("best_epoch", 0)])
def test_invalid_selection_provenance_rejected(selected, tmp_path, field, value):
    gold, selection = selected
    manifest = json.loads(selection.read_text())
    manifest["models"]["lr"][field] = value
    selection.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        control.run_permutation_control(gold, tmp_path / "invalid", selection)


class ConstantScore(nn.Module):
    def __init__(self):
        super().__init__()
        self.bias = nn.Parameter(torch.zeros(()))

    def forward(self, batch):
        return self.bias.expand(batch["numeric"].shape[0])


class LabelLeak(ConstantScore):
    def forward(self, batch):
        return (batch["label"] * 2 - 1) * 10 + self.bias * 0


@pytest.mark.parametrize("model_type,expected_auc,passed", [(ConstantScore, .5, True), (LabelLeak, 1., False)])
def test_null_gate_accepts_constant_scores_and_rejects_label_access(selected, tmp_path, monkeypatch,
                                                                  model_type, expected_auc, passed):
    gold, selection = selected
    monkeypatch.setattr(control, "make_model", lambda spec, cfg: model_type())
    run = tmp_path / "trial"
    result = control.run_permutation_control(gold, run, selection)
    assert result["passed"] is passed
    for name, model in result["models"].items():
        with np.load(run / name / "predictions.npz") as predictions:
            independent_auc = roc_auc_score(predictions["label"], predictions["probability"])
        assert independent_auc == expected_auc == model["validation"]["auc"]
        assert model["passed"] is passed


def test_randomized_validation_labels_cannot_change_trained_state(selected, tmp_path, monkeypatch):
    gold, selection = selected
    first, second = tmp_path / "first", tmp_path / "second"
    control.run_permutation_control(gold, first, selection, seed=42)
    prepare = control.prepare_shuffled_gold

    def change_only_val(*args, **kwargs):
        manifest = prepare(*args, **kwargs)
        destination = Path(args[1])
        path = next((destination / "val").glob("*.parquet"))
        table = pq.read_table(path)
        index = table.schema.get_field_index("label")
        labels = np.roll(table.column("label").to_numpy(), 1)
        pq.write_table(table.set_column(index, table.schema.field(index), pa.array(labels)), path)
        permutation = np.random.default_rng([kwargs["seed"], 1]).permutation(len(labels))
        manifest["val_permutation_sha256"] = hashlib.sha256(np.roll(permutation, 1).astype("<i8").tobytes()).hexdigest()
        (destination / "_metadata/shuffle_manifest.json").write_text(json.dumps(manifest))
        return manifest

    monkeypatch.setattr(control, "prepare_shuffled_gold", change_only_val)
    control.run_permutation_control(gold, second, selection, seed=42)
    for name in ("lr", "deepfm"):
        a = torch.load(first / name / "final.pt", weights_only=True)["model_state_dict"]
        b = torch.load(second / name / "final.pt", weights_only=True)["model_state_dict"]
        assert all(torch.equal(a[key], b[key]) for key in a)
        with np.load(first / name / "predictions.npz") as x, np.load(second / name / "predictions.npz") as y:
            assert not np.array_equal(x["label"], y["label"])
            np.testing.assert_array_equal(x["logit"], y["logit"])


def test_nonempty_output_is_preserved(selected, tmp_path):
    gold, selection = selected
    run = tmp_path / "existing"
    run.mkdir()
    marker = run / "previous-results.txt"
    marker.write_text("preserve")
    with pytest.raises(FileExistsError):
        control.run_permutation_control(gold, run, selection)
    assert marker.read_text() == "preserve"


def test_cli_dispatches_frozen_selection_and_returns_gate_status(monkeypatch, tmp_path):
    calls = []
    monkeypatch.setattr(control, "run_permutation_control", lambda *args: calls.append(args) or {"passed": False})
    output = tmp_path / "control"
    assert control.main(["--run-dir", str(output), "--seed", "73"]) == 1
    assert calls == [(control.ROOT / "data/gold", output,
                      control.ROOT / "training/results/phase5_selection.json", 73)]
