"""Independently verify saved training artifacts; never run model inference."""

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
from sklearn.metrics import log_loss, roc_auc_score
import torch


ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def canonical(value):
    return json.loads(json.dumps(value, sort_keys=True))


def manual_auc(labels, scores):
    """Mann--Whitney rank-sum calculation with averaged ranks for score ties."""
    order = np.argsort(scores, kind="stable")
    sorted_scores, sorted_labels = scores[order], labels[order]
    starts = np.r_[0, np.flatnonzero(sorted_scores[1:] != sorted_scores[:-1]) + 1]
    ends = np.r_[starts[1:], len(labels)]
    positive_counts = np.add.reduceat(sorted_labels, starts)
    positive_rank_sum = np.sum((starts + ends + 1) * 0.5 * positive_counts)
    positives = float(labels.sum())
    negatives = len(labels) - positives
    if not positives or not negatives:
        raise ValueError("AUC requires positive and negative examples")
    return float((positive_rank_sum - positives * (positives + 1) / 2) / (positives * negatives))


def calibration(labels, probabilities):
    bins = np.minimum((probabilities * 10).astype(np.int64), 9)
    reliability = []
    for index in range(10):
        mask = bins == index
        if mask.any():
            reliability.append({"lower": index / 10, "upper": (index + 1) / 10,
                                "rows": int(mask.sum()),
                                "mean_probability": float(probabilities[mask].mean()),
                                "positive_rate": float(labels[mask].mean())})
    return {"mean_probability": float(probabilities.mean()), "positive_rate": float(labels.mean()),
            "signed_mean_gap": float(probabilities.mean() - labels.mean()),
            "brier_score": float(np.mean((probabilities - labels) ** 2)),
            "reliability_bins": reliability}


def verify_predictions(path, expected, split, spec, gold_dir, workers, tolerance):
    with np.load(path, allow_pickle=False) as saved:
        labels = np.asarray(saved["label"], dtype=np.float64)
        probabilities = np.asarray(saved["probability"], dtype=np.float64)
        logits = np.asarray(saved["logit"], dtype=np.float64)
    if not (labels.ndim == 1 and labels.shape == probabilities.shape == logits.shape and labels.size):
        raise ValueError(f"{split}: predictions have invalid shapes or are empty")
    if not all(np.isfinite(values).all() for values in (labels, probabilities, logits)):
        raise ValueError(f"{split}: predictions contain nonfinite values")
    if not np.isin(labels, [0, 1]).all() or ((probabilities < 0) | (probabilities > 1)).any():
        raise ValueError(f"{split}: invalid binary labels or probabilities")
    clipped = np.clip(probabilities, np.finfo(np.float64).eps, 1 - np.finfo(np.float64).eps)
    metrics = {"rows": len(labels), "auc": float(roc_auc_score(labels, probabilities)),
               "log_loss": float(log_loss(labels, probabilities, labels=[0, 1])),
               "mean_probability": float(probabilities.mean()), "positive_rate": float(labels.mean())}
    independent = {"auc": manual_auc(labels, probabilities),
                   "log_loss": float(np.mean(-labels * np.log(clipped)
                                              - (1 - labels) * np.log1p(-clipped)))}
    errors = []
    if len(labels) != spec["splits"][split]["rows"]:
        errors.append(f"Rows {len(labels)} != feature spec {spec['splits'][split]['rows']}")
    for key, value in metrics.items():
        if key not in expected or abs(value - expected[key]) > tolerance:
            errors.append(f"Saved {key}: expected={expected.get(key)} recomputed={value}")
    for key, value in independent.items():
        if abs(value - metrics[key]) > tolerance:
            errors.append(f"Manual {key}={value} differs from sklearn={metrics[key]}")
    sigmoid_error = float(np.max(np.abs(probabilities - np.exp(-np.logaddexp(0., -logits)))))
    if sigmoid_error > tolerance:
        errors.append(f"Saved probability differs from sigmoid(logit): max error {sigmoid_error}")
    label_order = {"checked": False, "reason": "Multiple workers interleave file batches"}
    if workers == 0:
        files = sorted((gold_dir / split).glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"No gold Parquet files for {split}")
        gold_labels = np.concatenate([pq.read_table(path, columns=["label"]).column("label").to_numpy()
                                      for path in files]).astype(np.float64)
        equal = np.array_equal(labels, gold_labels)
        label_order = {"checked": True, "equal": equal, "gold_rows": len(gold_labels)}
        if not equal:
            errors.append("Saved labels differ from independently read gold labels/row order")
    return {"passed": not errors, "errors": errors, "sklearn": metrics, "manual": independent,
            "maximum_sigmoid_error": sigmoid_error, "gold_label_order": label_order,
            "calibration": calibration(labels, probabilities), "prediction_file_sha256": digest(path)}


def verify_run(run_dir, gold_dir, spec, include_holdouts, tolerance):
    metrics = json.loads((run_dir / "metrics.json").read_text())
    checkpoint = torch.load(run_dir / "best.pt", map_location="cpu", weights_only=True)
    errors = []
    if metrics.get("partial_run"):
        errors.append("Acceptance run is marked partial")
    for epoch in metrics["history"]:
        if epoch["train_rows"] != spec["splits"]["train"]["rows"]:
            errors.append(f"Epoch {epoch['epoch']} training row count differs from spec")
        if epoch["val"]["rows"] != spec["splits"]["val"]["rows"]:
            errors.append(f"Epoch {epoch['epoch']} validation row count differs from spec")
    expected_best = max(metrics["history"], key=lambda epoch: epoch["val"]["auc"])
    if not (checkpoint["epoch"] == metrics["best_epoch"] == expected_best["epoch"]):
        errors.append("Checkpoint/best_epoch is not the first maximum validation AUC epoch")
    if checkpoint["val_metrics"] != metrics["best_val"] or metrics["best_val"] != expected_best["val"]:
        errors.append("Best validation metrics do not match checkpoint/history")
    if checkpoint["feature_spec"] != spec:
        errors.append("Checkpoint feature spec differs from current gold feature spec")
    spec_hash = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    if metrics["feature_spec_sha256"] != spec_hash:
        errors.append("Metrics feature spec digest differs from independently calculated digest")
    if canonical(checkpoint["model_config"]) != canonical(metrics["config"]):
        errors.append("Metrics training config differs from checkpoint config")
    if not include_holdouts:
        if metrics.get("test_evaluated") or metrics["config"].get("evaluate_test"):
            errors.append("Held-out evaluation occurred before the final verification gate")
        if any((run_dir / "predictions" / f"{split}.npz").exists() for split in ("test", "test_random")):
            errors.append("Held-out prediction artifacts exist before the final verification gate")
    elif not metrics.get("test_evaluated"):
        errors.append("Final run is not marked as having evaluated held-out data")
    if include_holdouts:
        provenance = metrics.get("evaluation", {})
        if provenance.get("checkpoint_sha256") != digest(run_dir / "best.pt"):
            errors.append("Final evaluation provenance does not match the saved checkpoint SHA256")
        marker_path = run_dir / "heldout_evaluation.json"
        if not marker_path.exists():
            errors.append("Final evaluation completion marker is missing")
        elif json.loads(marker_path.read_text()) != provenance or provenance.get("status") != "complete":
            errors.append("Final evaluation marker and recorded completion provenance differ")
    splits = ["val"]
    if include_holdouts:
        splits += [split for split in ("test", "test_random") if split in spec["splits"]]
    results = {}
    for split in splits:
        result = verify_predictions(run_dir / "predictions" / f"{split}.npz",
                                    metrics["final"][split], split, spec, gold_dir,
                                    metrics["config"]["num_workers"], tolerance)
        results[split] = result
        errors.extend(f"{split}: {error}" for error in result["errors"])
    return {"run_dir": str(run_dir), "model": metrics["model"], "passed": not errors,
            "errors": errors, "checkpoint_sha256": digest(run_dir / "best.pt"),
            "best_epoch": metrics["best_epoch"], "epochs": len(metrics["history"]),
            "training_config": metrics["config"], "splits": results}


def state_differences(left, right, path="", limit=30):
    errors = []
    if isinstance(left, torch.Tensor) and isinstance(right, torch.Tensor):
        if not torch.equal(left, right):
            errors.append(path)
    elif isinstance(left, dict) and isinstance(right, dict):
        if left.keys() != right.keys():
            errors.append(path + ".keys")
        for key in left.keys() & right.keys():
            errors.extend(state_differences(left[key], right[key], f"{path}.{key}", limit))
            if len(errors) >= limit:
                break
    elif isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        if len(left) != len(right):
            errors.append(path + ".length")
        for i, (a, b) in enumerate(zip(left, right)):
            errors.extend(state_differences(a, b, f"{path}[{i}]", limit))
            if len(errors) >= limit:
                break
    elif left != right:
        errors.append(path)
    return errors[:limit]


def compare_reproducibility(left_dir, right_dir):
    left = json.loads((left_dir / "metrics.json").read_text())
    right = json.loads((right_dir / "metrics.json").read_text())
    errors = []
    for key in ("config", "feature_spec_sha256", "history", "best_epoch", "best_val"):
        if left[key] != right[key]:
            errors.append(f"Metrics field differs: {key}")
    a = torch.load(left_dir / "best.pt", map_location="cpu", weights_only=True)
    b = torch.load(right_dir / "best.pt", map_location="cpu", weights_only=True)
    for key in ("model_state_dict", "optimizer_state_dict"):
        errors.extend(state_differences(a[key], b[key], key))
    with np.load(left_dir / "predictions/val.npz", allow_pickle=False) as x, \
            np.load(right_dir / "predictions/val.npz", allow_pickle=False) as y:
        for key in ("label", "probability", "logit"):
            if not np.array_equal(x[key], y[key]):
                errors.append(f"Validation predictions differ: {key}")
    return {"left": str(left_dir), "right": str(right_dir), "bit_identical": not errors,
            "differences": errors, "checkpoint_sha256_left": digest(left_dir / "best.pt"),
            "checkpoint_sha256_right": digest(right_dir / "best.pt")}


def verify_control(run_dir, gold_dir, spec, tolerance):
    result = verify_run(run_dir, gold_dir, spec, False, tolerance)
    metrics = json.loads((run_dir / "metrics.json").read_text())
    aucs = [epoch["val"]["auc"] for epoch in metrics["history"]]
    result["validation_aucs"] = aucs
    result["last_auc"] = aucs[-1]
    result["best_auc"] = metrics["best_val"]["auc"]
    result["within_predeclared_band"] = 0.49 <= result["best_auc"] <= 0.51
    result["interpretation"] = (
        "Fixed shuffled-training-label control; validation labels stay original. "
        "Inspect control construction/epoch selection if outside the predeclared band; "
        "an above-chance result alone does not establish leakage.")
    if not result["within_predeclared_band"]:
        result["passed"] = False
        result["errors"].append("Shuffled control best validation AUC is outside 0.49--0.51")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-dir", type=Path, default=ROOT / "data/gold")
    parser.add_argument("--run", action="append", nargs=2, metavar=("LABEL", "RUN_DIR"), default=[])
    parser.add_argument("--reproduce", action="append", nargs=2, metavar=("RUN_A", "RUN_B"), default=[])
    parser.add_argument("--control", action="append", type=Path, default=[])
    parser.add_argument("--include-holdouts", action="store_true",
                        help="Read saved held-out predictions only after final model selection/evaluation")
    parser.add_argument("--tolerance", type=float, default=1e-12)
    parser.add_argument("--output", type=Path,
                        default=ROOT / "checkpoints/phase55-verification/results-audit.json")
    args = parser.parse_args()
    if not args.run and not args.reproduce and not args.control:
        parser.error("Provide at least one --run, --reproduce, or --control")
    spec = json.loads((args.gold_dir / "_metadata/feature_spec.json").read_text())
    report = {"holdouts_authorized": args.include_holdouts, "runs": {}, "reproducibility": [], "controls": []}
    for label, directory in args.run:
        if label in report["runs"]:
            parser.error(f"Duplicate run label: {label}")
        report["runs"][label] = verify_run(Path(directory), args.gold_dir, spec,
                                           args.include_holdouts, args.tolerance)
    for left, right in args.reproduce:
        report["reproducibility"].append(compare_reproducibility(Path(left), Path(right)))
    for control in args.control:
        report["controls"].append(verify_control(control, args.gold_dir, spec, args.tolerance))
    if {"lr", "deepfm"} <= report["runs"].keys():
        lr = report["runs"]["lr"]["splits"]["val"]["sklearn"]["auc"]
        deepfm = report["runs"]["deepfm"]["splits"]["val"]["sklearn"]["auc"]
        report["acceptance"] = {"lr_auc_at_least_0_696": lr >= 0.696,
                                "deepfm_auc_above_lr": deepfm > lr,
                                "lr_val_auc": lr, "deepfm_val_auc": deepfm,
                                "validation_auc_gain": deepfm - lr,
                                "plausibility_investigation_needed": not (0.70 <= lr <= 0.85 and 0.70 <= deepfm <= 0.85)}
    passed = all(item["passed"] for item in report["runs"].values())
    passed &= all(item["bit_identical"] for item in report["reproducibility"])
    passed &= all(item["passed"] for item in report["controls"])
    if "acceptance" in report:
        passed &= report["acceptance"]["lr_auc_at_least_0_696"] and report["acceptance"]["deepfm_auc_above_lr"]
    report["passed"] = bool(passed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(json.dumps({"passed": report["passed"], "run_labels": list(report["runs"]),
                      "reproducibility_comparisons": len(report["reproducibility"]),
                      "controls": len(report["controls"]), "acceptance": report.get("acceptance"),
                      "report": str(args.output)}, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
