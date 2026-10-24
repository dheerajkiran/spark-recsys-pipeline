"""Saved-checkpoint evaluation without retraining or repeated hold-out access."""

import hashlib
import json

import numpy as np
import pytest
import torch

from training import train_deepfm as training
from training.tests.test_training import assert_metrics, config, write_gold


@pytest.fixture
def selected_run(tmp_path):
    gold = write_gold(tmp_path / "gold")
    run = tmp_path / "selected"
    training.run_training(gold, run, config(model="deepfm", dropout=.2))
    return gold, run


def no_evaluation(*args, **kwargs):
    pytest.fail("Rejected evaluation must not evaluate a dataset")


def test_saved_checkpoint_evaluates_each_holdout_once_without_retraining(selected_run, monkeypatch):
    gold, run = selected_run
    before = json.loads((run / "metrics.json").read_text())
    checkpoint_bytes = (run / "best.pt").read_bytes()
    val_bytes = (run / "predictions/val.npz").read_bytes()
    checkpoint = torch.load(run / "best.pt", weights_only=True)
    calls = []
    original = training.evaluate

    def record_evaluation(model, loader, max_batches=None):
        calls.append(loader.dataset.files[0].parent.name)
        assert all(torch.equal(value, checkpoint["model_state_dict"][name])
                   for name, value in model.state_dict().items())
        assert max_batches is None
        return original(model, loader, max_batches)

    monkeypatch.setattr(training, "evaluate", record_evaluation)
    monkeypatch.setattr(training, "run_training", lambda *a, **k: pytest.fail("Must not retrain"))
    monkeypatch.setattr(torch.optim, "Adam", lambda *a, **k: pytest.fail("Must not create an optimizer"))
    result = training.evaluate_saved_checkpoint(gold, run)
    assert calls == ["test", "test_random"]
    assert result["test_evaluated"] is True and set(result["final"]) == {"val", "test", "test_random"}
    for field in ("history", "best_epoch", "best_val", "config"):
        assert result[field] == before[field]
    assert result["final"]["val"] == before["final"]["val"]
    assert (run / "best.pt").read_bytes() == checkpoint_bytes
    assert (run / "predictions/val.npz").read_bytes() == val_bytes
    assert result["evaluation"]["checkpoint_sha256"] == hashlib.sha256(checkpoint_bytes).hexdigest()
    marker = json.loads((run / "heldout_evaluation.json").read_text())
    assert marker == result["evaluation"] and marker["status"] == "complete"
    assert json.loads((run / "metrics.json").read_text()) == result
    for split in ("test", "test_random"):
        with np.load(run / "predictions" / f"{split}.npz") as predictions:
            assert_metrics(result["final"][split], predictions)

    monkeypatch.setattr(training, "evaluate", no_evaluation)
    with pytest.raises((ValueError, FileExistsError)):
        training.evaluate_saved_checkpoint(gold, run)


@pytest.mark.parametrize("limit", ["max_train_batches", "max_eval_batches"])
def test_partial_runs_cannot_evaluate_holdouts(tmp_path, monkeypatch, limit):
    gold = write_gold(tmp_path / "gold")
    run = tmp_path / "partial"
    training.run_training(gold, run, config(model="lr", epochs=1, **{limit: 1}))
    monkeypatch.setattr(training, "evaluate", no_evaluation)
    with pytest.raises(ValueError, match="full|limit|partial"):
        training.evaluate_saved_checkpoint(gold, run)
    assert not (run / "predictions/test.npz").exists()


@pytest.mark.parametrize("mismatch", ["feature_spec", "best_epoch"])
def test_selected_checkpoint_mismatches_reject_before_evaluation(selected_run, monkeypatch, mismatch):
    gold, run = selected_run
    if mismatch == "feature_spec":
        path = gold / "_metadata/feature_spec.json"
        spec = json.loads(path.read_text())
        spec["splits"]["test"]["rows"] += 1
        path.write_text(json.dumps(spec))
    else:
        path = run / "best.pt"
        checkpoint = torch.load(path, weights_only=True)
        checkpoint["epoch"] += 1
        torch.save(checkpoint, path)
    monkeypatch.setattr(training, "evaluate", no_evaluation)
    with pytest.raises(ValueError):
        training.evaluate_saved_checkpoint(gold, run)


@pytest.mark.parametrize("status", ["started", "complete"])
def test_existing_evaluation_marker_rejects_reentry(selected_run, monkeypatch, status):
    gold, run = selected_run
    marker = run / "heldout_evaluation.json"
    marker.write_text(json.dumps({"status": status}))
    original = marker.read_bytes()
    monkeypatch.setattr(training, "evaluate", no_evaluation)
    with pytest.raises((ValueError, FileExistsError)):
        training.evaluate_saved_checkpoint(gold, run)
    assert marker.read_bytes() == original


@pytest.mark.parametrize("split", ["test", "test_random"])
def test_existing_holdout_predictions_are_preserved(selected_run, monkeypatch, split):
    gold, run = selected_run
    prediction = run / "predictions" / f"{split}.npz"
    prediction.write_bytes(b"preserve existing held-out results")
    monkeypatch.setattr(training, "evaluate", no_evaluation)
    with pytest.raises(ValueError):
        training.evaluate_saved_checkpoint(gold, run)
    assert prediction.read_bytes() == b"preserve existing held-out results"


def test_failed_evaluation_keeps_marker_and_blocks_retry(selected_run, monkeypatch):
    gold, run = selected_run

    def fail_evaluation(*args, **kwargs):
        raise RuntimeError("simulated evaluation failure")

    monkeypatch.setattr(training, "evaluate", fail_evaluation)
    with pytest.raises(RuntimeError, match="simulated evaluation failure"):
        training.evaluate_saved_checkpoint(gold, run)
    marker = run / "heldout_evaluation.json"
    assert marker.exists()
    assert json.loads(marker.read_text())["status"] in ("started", "failed")
    assert json.loads((run / "metrics.json").read_text())["test_evaluated"] is False
    monkeypatch.setattr(training, "evaluate", no_evaluation)
    with pytest.raises((ValueError, FileExistsError)):
        training.evaluate_saved_checkpoint(gold, run)


def test_evaluate_only_cli_uses_saved_configuration(selected_run, monkeypatch):
    gold, run = selected_run
    monkeypatch.setattr(training, "run_training", lambda *a, **k: pytest.fail("Must not retrain"))
    training.main(["--evaluate-only", "--run-dir", str(run), "--gold-dir", str(gold)])
    result = json.loads((run / "metrics.json").read_text())
    assert result["model"] == "deepfm" and result["config"]["embedding_dim"] == 4
    assert result["test_evaluated"] is True


@pytest.mark.parametrize("override", [["--model", "lr"], ["--epochs", "1"], ["--learning-rate", ".1"]])
def test_evaluate_only_cli_rejects_training_overrides(selected_run, monkeypatch, override):
    gold, run = selected_run
    monkeypatch.setattr(training, "evaluate_saved_checkpoint", no_evaluation)
    with pytest.raises(SystemExit) as error:
        training.main(["--evaluate-only", "--run-dir", str(run), "--gold-dir", str(gold), *override])
    assert error.value.code == 2


def test_evaluate_only_cli_requires_existing_run_directory(monkeypatch):
    monkeypatch.setattr(training, "evaluate_saved_checkpoint", no_evaluation)
    with pytest.raises(SystemExit) as error:
        training.main(["--evaluate-only"])
    assert error.value.code == 2
