"""Independent rank math and detection of corrupted saved run artifacts."""

import json

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
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


def _save_model_result(bundle, name, result):
    (bundle / name / "results.json").write_text(json.dumps(result))
    summary_path = bundle / "results.json"
    summary = json.loads(summary_path.read_text())
    summary["models"][name] = result
    summary["passed"] = all(value["passed"] for value in summary["models"].values())
    summary_path.write_text(json.dumps(summary))


def _save_scores(bundle, name, logits):
    labels = np.concatenate([pq.read_table(path, columns=["label"])["label"].to_numpy()
                             for path in sorted((bundle / "gold/val").glob("*.parquet"))])
    probabilities = np.exp(-np.logaddexp(0., -logits))
    path = bundle / name / "predictions.npz"
    np.savez(path, label=labels, logit=logits, probability=probabilities)
    result = json.loads((bundle / name / "results.json").read_text())
    result["validation"] = training.binary_metrics(labels, probabilities)
    result["prediction_sha256"] = audit.digest(path)
    result["passed"] = .49 <= result["validation"]["auc"] <= .51
    _save_model_result(bundle, name, result)


@pytest.fixture
def null_bundle(tmp_path):
    from training.permutation_control import run_permutation_control

    gold = write_gold(tmp_path / "source", splits=("train", "val"))
    selection = {"selection_data": "train and val only", "models": {}}
    for name in ("lr", "deepfm"):
        run = tmp_path / ("selected-" + name)
        metrics = training.run_training(gold, run, config(model=name, epochs=2))
        selection["models"][name] = {
            "run_dir": str(run), "checkpoint_sha256": audit.digest(run / "best.pt"),
            "config": metrics["config"], "best_epoch": metrics["best_epoch"],
            "val": metrics["best_val"], "feature_spec_sha256": metrics["feature_spec_sha256"],
        }
    selection_path = tmp_path / "selection.json"
    selection_path.write_text(json.dumps(selection))
    bundle = tmp_path / "bundle"
    run_permutation_control(gold, bundle, selection_path)
    # A known constant predictor supplies a deterministic positive AUC=.5 fixture.
    # Zero weights in the real model architectures make the saved logits valid.
    for name in ("lr", "deepfm"):
        path = bundle / name / "final.pt"
        checkpoint = torch.load(path, weights_only=True)
        for tensor in checkpoint["model_state_dict"].values():
            tensor.zero_()
        torch.save(checkpoint, path)
        result = json.loads((bundle / name / "results.json").read_text())
        result["checkpoint_sha256"] = audit.digest(path)
        _save_model_result(bundle, name, result)
        _save_scores(bundle, name, np.zeros(7, dtype=np.float64))
    return bundle, gold, training.load_spec(gold), selection_path


def test_permutation_audit_accepts_independent_data_and_constant_null(null_bundle):
    bundle, gold, spec, selection = null_bundle
    result = audit.verify_permutation_control(bundle, gold, spec, selection_path=selection)
    assert result["passed"], result["errors"]
    assert result["data"]["passed"]
    assert result["protocol"]["auc_band"] == [.49, .51]
    assert set(result["models"]) == {"lr", "deepfm"}
    assert all(model["validation"]["sklearn"]["auc"] == .5 for model in result["models"].values())


@pytest.mark.parametrize("corruption", ["feature", "val_labels"])
def test_permutation_audit_detects_feature_or_permuted_label_corruption(null_bundle, corruption):
    bundle, gold, spec, selection = null_bundle
    split = "train" if corruption == "feature" else "val"
    path = next((bundle / "gold" / split).glob("*.parquet"))
    table = pq.read_table(path)
    name = "value" if corruption == "feature" else "label"
    values = table[name].to_numpy()
    changed = values + 1 if corruption == "feature" else 1 - values
    table = table.set_column(table.schema.get_field_index(name), table.schema.field(name), pa.array(changed))
    pq.write_table(table, path)
    result = audit.verify_permutation_control(bundle, gold, spec, selection_path=selection)
    assert result["passed"] is False
    expected = "Features or row order" if corruption == "feature" else "global permutation"
    assert any(expected in error for error in result["errors"])


def test_permutation_audit_detects_wrong_terminal_epoch_even_with_updated_hash(null_bundle):
    bundle, gold, spec, selection = null_bundle
    path = bundle / "lr/final.pt"
    checkpoint = torch.load(path, weights_only=True)
    checkpoint["epoch"] += 1
    torch.save(checkpoint, path)
    result = json.loads((bundle / "lr/results.json").read_text())
    result["checkpoint_sha256"] = audit.digest(path)
    _save_model_result(bundle, "lr", result)
    verified = audit.verify_permutation_control(bundle, gold, spec, selection_path=selection)
    assert verified["passed"] is False
    assert any("terminal budget" in error for error in verified["errors"])


def test_permutation_audit_rejects_direct_target_scores_and_false_pass_claim(null_bundle):
    bundle, gold, spec, selection = null_bundle
    labels = np.concatenate([pq.read_table(path, columns=["label"])["label"].to_numpy()
                             for path in sorted((bundle / "gold/val").glob("*.parquet"))])
    _save_scores(bundle, "lr", np.where(labels, 5., -5.))
    result = json.loads((bundle / "lr/results.json").read_text())
    result["passed"] = True
    _save_model_result(bundle, "lr", result)
    verified = audit.verify_permutation_control(bundle, gold, spec, selection_path=selection)
    assert verified["passed"] is False
    assert verified["models"]["lr"]["validation"]["sklearn"]["auc"] == 1.
    assert any("0.49--0.51" in error for error in verified["errors"])
    assert any("pass claim" in error for error in verified["errors"])


def test_permutation_audit_does_not_accept_widened_protocol_band(null_bundle):
    bundle, gold, spec, selection = null_bundle
    path = bundle / "protocol.json"
    protocol = json.loads(path.read_text())
    protocol["auc_band"] = [0., 1.]
    path.write_text(json.dumps(protocol))
    summary = json.loads((bundle / "results.json").read_text())
    summary["protocol"] = protocol
    (bundle / "results.json").write_text(json.dumps(summary))
    verified = audit.verify_permutation_control(bundle, gold, spec, selection_path=selection)
    assert verified["passed"] is False
    assert any("Protocol auc_band" in error for error in verified["errors"])


def test_permutation_audit_cli_accepts_repeated_bundles(null_bundle, tmp_path, monkeypatch):
    bundle, gold, spec, selection = null_bundle
    original = audit.verify_permutation_control
    monkeypatch.setattr(audit, "verify_permutation_control", lambda b, g, s, t: original(b, g, s, t, selection))
    output = tmp_path / "audit.json"
    monkeypatch.setattr("sys.argv", ["verify_training_results", "--gold-dir", str(gold),
                        "--permutation-control", str(bundle), "--permutation-control", str(bundle),
                        "--output", str(output)])
    assert audit.main() == 0
    report = json.loads(output.read_text())
    assert report["passed"] is True and len(report["permutation_controls"]) == 2


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -float("inf"), "0.5", None, True])
def test_permutation_audit_rejects_invalid_saved_scalar_even_when_summary_matches(null_bundle, invalid):
    bundle, gold, spec, selection = null_bundle
    result = json.loads((bundle / "lr/results.json").read_text())
    result["validation"]["auc"] = invalid
    _save_model_result(bundle, "lr", result)
    verified = audit.verify_permutation_control(bundle, gold, spec, selection_path=selection)
    assert verified["passed"] is False
    assert any("Saved auc must be a finite numeric scalar" in error for error in verified["errors"])


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -1e-12, True, "0"])
def test_prediction_audit_rejects_invalid_tolerance_before_reading(invalid, tmp_path):
    with pytest.raises(ValueError, match="finite nonnegative"):
        audit.verify_predictions(tmp_path / "unread.npz", {}, "val", {}, tmp_path, 0, invalid)


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -1e-12])
def test_permutation_audit_rejects_invalid_tolerance(null_bundle, invalid):
    bundle, gold, spec, selection = null_bundle
    verified = audit.verify_permutation_control(bundle, gold, spec, tolerance=invalid, selection_path=selection)
    assert verified["passed"] is False
    assert any("Tolerance must be a finite nonnegative" in error for error in verified["errors"])


@pytest.mark.parametrize("invalid", ["nan", "inf", "-1e-12"])
def test_audit_cli_rejects_invalid_tolerance_before_reading(invalid, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["verify_training_results", "--tolerance=" + invalid,
                                    "--permutation-control", str(tmp_path / "unread")])
    with pytest.raises(SystemExit) as error:
        audit.main()
    assert error.value.code == 2
    assert "Tolerance must be a finite nonnegative" in capsys.readouterr().err


def test_permutation_audit_accepts_zero_tolerance_for_exact_metrics(null_bundle):
    bundle, gold, spec, selection = null_bundle
    verified = audit.verify_permutation_control(bundle, gold, spec, tolerance=0, selection_path=selection)
    assert verified["passed"], verified["errors"]


@pytest.mark.parametrize("corruption", ["model_nan", "optimizer_inf", "empty_model", "empty_optimizer"])
def test_permutation_audit_rejects_invalid_checkpoint_states_with_updated_hash(null_bundle, corruption):
    bundle, gold, spec, selection = null_bundle
    path = bundle / "lr/final.pt"
    checkpoint = torch.load(path, weights_only=True)
    if corruption == "model_nan":
        checkpoint["model_state_dict"]["numeric.bias"].fill_(float("nan"))
    elif corruption == "optimizer_inf":
        adam_state = next(iter(checkpoint["optimizer_state_dict"]["state"].values()))
        adam_state["exp_avg"].fill_(float("inf"))
    elif corruption == "empty_model":
        checkpoint["model_state_dict"] = {}
    else:
        checkpoint["optimizer_state_dict"] = {}
    torch.save(checkpoint, path)
    saved = json.loads((bundle / "lr/results.json").read_text())
    saved["checkpoint_sha256"] = audit.digest(path)
    _save_model_result(bundle, "lr", saved)
    verified = audit.verify_permutation_control(bundle, gold, spec, selection_path=selection)
    assert verified["passed"] is False
    message = "nonfinite tensor" if corruption in ("model_nan", "optimizer_inf") else "nonempty dictionary"
    assert any(message in error for error in verified["errors"])
