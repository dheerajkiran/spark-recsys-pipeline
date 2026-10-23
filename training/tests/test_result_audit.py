"""Independent rank math and detection of corrupted saved run artifacts."""

import json

import numpy as np
import pytest
from sklearn.metrics import roc_auc_score
import torch

from scripts import verify_training_results as audit
from training import train_deepfm as training
from training.tests.test_training import config, write_gold


def test_manual_auc_handles_tied_scores():
    labels = np.array([0., 1., 0., 1.])
    scores = np.array([.1, .5, .5, .9])
    assert audit.manual_auc(labels, scores) == pytest.approx(.875, abs=1e-12)
    assert audit.manual_auc(labels, scores) == pytest.approx(roc_auc_score(labels, scores), abs=1e-12)
    assert audit.manual_auc(labels, np.ones(4)) == .5


def test_saved_metric_corruption_is_detected(tmp_path):
    gold = write_gold(tmp_path / "gold", splits=("train", "val"))
    run = tmp_path / "run"
    training.run_training(gold, run, config(model="lr", epochs=1))
    spec = training.load_spec(gold)
    assert audit.verify_run(run, gold, spec, False, 1e-12)["passed"]
    path = run / "metrics.json"
    metrics = json.loads(path.read_text())
    metrics["final"]["val"]["auc"] -= .1
    path.write_text(json.dumps(metrics))
    result = audit.verify_run(run, gold, spec, False, 1e-12)
    assert result["passed"] is False
    assert any("Saved auc" in error for error in result["errors"])


def test_reproducibility_audit_detects_changed_model_weights(tmp_path):
    gold = write_gold(tmp_path / "gold", splits=("train", "val"))
    first, second = tmp_path / "first", tmp_path / "second"
    for run in (first, second):
        training.run_training(gold, run, config(model="lr", epochs=1))
    assert audit.compare_reproducibility(first, second)["bit_identical"]
    path = second / "best.pt"
    checkpoint = torch.load(path, weights_only=True)
    checkpoint["model_state_dict"]["numeric.bias"].add_(.2)
    torch.save(checkpoint, path)
    result = audit.compare_reproducibility(first, second)
    assert result["bit_identical"] is False
    assert "model_state_dict.numeric.bias" in result["differences"]
