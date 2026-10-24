"""Independently verify saved training artifacts; never run model inference."""

import argparse
import hashlib
import json
import math
from numbers import Real
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


def _finite_number(value):
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, Real):
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def validate_tolerance(tolerance):
    if not _finite_number(tolerance) or tolerance < 0:
        raise ValueError("Tolerance must be a finite nonnegative number")


def nonfinite_state_paths(value, path):
    """Find invalid checkpoint tensors without constructing or running a model."""
    if isinstance(value, torch.Tensor):
        return [] if torch.isfinite(value).all().item() else [path]
    if isinstance(value, dict):
        return [problem for key, child in value.items()
                for problem in nonfinite_state_paths(child, f"{path}.{key}")]
    if isinstance(value, (list, tuple)):
        return [problem for index, child in enumerate(value)
                for problem in nonfinite_state_paths(child, f"{path}[{index}]")]
    return []


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
    validate_tolerance(tolerance)
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
        if key not in expected or not _finite_number(expected[key]):
            errors.append(f"Saved {key} must be a finite numeric scalar: {expected.get(key)!r}")
        elif abs(value - expected[key]) > tolerance:
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


def _verify_permuted_data(source, control, spec, seed):
    """Reconstruct both permutations and compare every non-target source value."""
    errors, splits = [], {}
    manifest = json.loads((control / "_metadata/shuffle_manifest.json").read_text())
    if (manifest.get("protocol") != "permuted-train-and-validation-v1"
            or manifest.get("validation_permuted") is not True or manifest.get("seed") != seed):
        errors.append("Shuffle manifest does not declare the independent train/val protocol and seed")
    if Path(manifest["source_gold_dir"]).resolve() != source:
        errors.append("Shuffle manifest source differs from the independently audited source")
    if (json.loads((control / "_metadata/feature_spec.json").read_text()) != spec
            or digest(control / "_metadata/feature_spec.json") != digest(source / "_metadata/feature_spec.json")):
        errors.append("Control feature spec is not an unchanged source copy")
    if any((control / name).exists() for name in ("test", "test_random")):
        errors.append("Permutation control contains a held-out split")
    for split in ("train", "val"):
        original = sorted((source / split).glob("*.parquet"))
        shuffled = sorted((control / split).glob("*.parquet"))
        split_errors = []
        if not original or [path.name for path in original] != [path.name for path in shuffled]:
            split_errors.append("Source/control Parquet filenames or file counts differ")
        source_labels = []
        for path in original:
            before = pq.read_table(path)
            source_labels.append(before["label"].to_numpy())
            destination = control / split / path.name
            if destination.exists():
                after = pq.read_table(destination)
                if not before.schema.equals(after.schema, check_metadata=True):
                    split_errors.append(f"Schema changed in {path.name}")
                if not before.drop(["label"]).equals(after.drop(["label"]), check_metadata=True):
                    split_errors.append(f"Features or row order changed in {path.name}")
        labels = np.concatenate(source_labels)
        actual = np.concatenate([pq.read_table(path, columns=["label"])["label"].to_numpy() for path in shuffled])
        order = np.random.default_rng(seed if split == "train" else [seed, 1]).permutation(len(labels))
        permutation_digest = hashlib.sha256(order.astype("<i8").tobytes()).hexdigest()
        counts = {str(label): int(np.count_nonzero(labels == label)) for label in (0, 1)}
        if not np.array_equal(actual, labels[order]):
            split_errors.append("Labels differ from the independently reconstructed global permutation")
        if not np.array_equal(np.sort(labels), np.sort(actual)):
            split_errors.append("Label multiset changed")
        if len(labels) != spec["splits"][split]["rows"] or len(actual) != len(labels):
            split_errors.append("Full row counts differ from the source feature spec")
        for key, expected in ((f"{split}_rows", len(labels)), (f"{split}_files", len(original)),
                              ("label_counts" if split == "train" else "val_label_counts", counts),
                              ("permutation_sha256" if split == "train" else "val_permutation_sha256", permutation_digest)):
            if manifest.get(key) != expected:
                split_errors.append(f"Shuffle manifest {key} differs from independent reconstruction")
        splits[split] = {"passed": not split_errors, "errors": split_errors,
                         "rows": len(labels), "files": len(original),
                         "label_counts": counts, "permutation_sha256": permutation_digest}
        errors.extend(f"{split}: {error}" for error in split_errors)
    return {"passed": not errors, "errors": errors, "splits": splits}


def verify_permutation_control(bundle_dir, gold_dir, spec, tolerance=1e-12,
                               selection_path=ROOT / "training/results/phase5_selection.json"):
    """Audit a fixed-budget train/val permutation-null bundle without inference."""
    bundle, source, selection_path = Path(bundle_dir).resolve(), Path(gold_dir).resolve(), Path(selection_path)
    result = {"bundle_dir": str(bundle), "passed": False, "errors": [],
              "protocol": {}, "models": {}, "data": {}}
    errors = result["errors"]
    try:
        reference = json.loads(selection_path.read_text())
        protocol = json.loads((bundle / "protocol.json").read_text())
        saved_bundle = json.loads((bundle / "results.json").read_text())
        result["protocol"] = protocol
        if (bundle / "reference_selection.json").read_bytes() != selection_path.read_bytes():
            errors.append("Frozen reference selection bytes differ from the authoritative selection")
        spec_hash = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
        budgets = {name: reference["models"][name]["best_epoch"] for name in ("lr", "deepfm")}
        selected_hashes = {name: reference["models"][name]["checkpoint_sha256"] for name in budgets}
        for key, expected in (("id", "permutation-null-v1"), ("selection_sha256", digest(selection_path)),
                              ("feature_spec_sha256", spec_hash), ("auc_band", [0.49, 0.51]),
                              ("selection_rule", "fixed_terminal_epoch"), ("budgets", budgets),
                              ("selected_checkpoint_sha256", selected_hashes)):
            if protocol.get(key) != expected:
                errors.append(f"Protocol {key} differs from its frozen definition")
        if type(protocol.get("seed")) is not int or protocol["seed"] < 0:
            raise ValueError("Protocol seed must be a nonnegative integer")
        if json.loads((source / "_metadata/feature_spec.json").read_text()) != spec:
            errors.append("Provided feature spec differs from source gold")
        if saved_bundle.get("protocol") != protocol or set(saved_bundle.get("models", {})) != set(budgets):
            errors.append("Bundle summary protocol/model keys differ from recorded artifacts")
        result["data"] = _verify_permuted_data(source, bundle / "gold", spec, protocol["seed"])
        errors.extend(result["data"]["errors"])
        for name, budget in budgets.items():
            model_errors = []
            selected = reference["models"][name]
            config = selected["config"]
            if (type(budget) is not int or not 1 <= budget <= config["epochs"]
                    or config["model"] != name or config["num_workers"] != 0
                    or config["evaluate_test"] or config["max_train_batches"] is not None
                    or config["max_eval_batches"] is not None):
                model_errors.append("Frozen config/budget is not a full train/val-only in-process run")
            if selected["feature_spec_sha256"] != spec_hash:
                model_errors.append("Frozen selected model feature spec differs from source")
            selected_dir = Path(selected["run_dir"])
            selected_checkpoint = (selected_dir if selected_dir.is_absolute() else ROOT / selected_dir) / "best.pt"
            if digest(selected_checkpoint) != selected["checkpoint_sha256"]:
                model_errors.append("Authoritative selected source checkpoint digest changed")
            directory = bundle / name
            saved = json.loads((directory / "results.json").read_text())
            checkpoint_path, prediction_path = directory / "final.pt", directory / "predictions.npz"
            checkpoint = torch.load(checkpoint_path, map_location="cpu", weights_only=True)
            if saved.get("model") != name or canonical(saved["config"]) != canonical(config):
                model_errors.append("Model result config differs from frozen selected config")
            if saved.get("epochs") != budget or checkpoint["epoch"] != budget:
                model_errors.append("Final checkpoint/result epoch differs from frozen terminal budget")
            if canonical(checkpoint["model_config"]) != canonical(config) or checkpoint["feature_spec"] != spec:
                model_errors.append("Final checkpoint config/spec differs from frozen source")
            if not {"model_state_dict", "optimizer_state_dict"} <= checkpoint.keys():
                model_errors.append("Final checkpoint lacks model or optimizer state")
            for key in ("model_state_dict", "optimizer_state_dict"):
                state = checkpoint.get(key)
                if not isinstance(state, dict) or not state:
                    model_errors.append(f"Final checkpoint {key} must be a nonempty dictionary")
                else:
                    for path in nonfinite_state_paths(state, key):
                        model_errors.append(f"Final checkpoint contains a nonfinite tensor: {path}")
            history = saved["history"]
            if len(history) != budget or [row.get("epoch") for row in history] != list(range(1, budget + 1)):
                model_errors.append("History does not contain exactly the frozen epochs")
            for row in history:
                if (set(row) != {"epoch", "train_rows", "train_loss"}
                        or row["train_rows"] != spec["splits"]["train"]["rows"]
                        or not np.isfinite(row["train_loss"])):
                    model_errors.append("History contains early validation, incomplete rows, or nonfinite training loss")
            if saved.get("checkpoint_sha256") != digest(checkpoint_path):
                model_errors.append("Final checkpoint SHA256 differs from recorded digest")
            if saved.get("prediction_sha256") != digest(prediction_path):
                model_errors.append("Prediction SHA256 differs from recorded digest")
            predictions = verify_predictions(prediction_path, saved["validation"], "val", spec,
                                             bundle / "gold", 0, tolerance)
            model_errors.extend(predictions["errors"])
            within_band = 0.49 <= predictions["sklearn"]["auc"] <= 0.51
            if not within_band:
                model_errors.append("Permutation-null validation AUC is outside the fixed 0.49--0.51 band")
            if saved.get("passed") is not within_band:
                model_errors.append("Saved model pass claim differs from the independently computed AUC gate")
            if saved_bundle.get("models", {}).get(name) != saved:
                model_errors.append("Bundle and model result metadata differ")
            result["models"][name] = {"passed": not model_errors, "errors": model_errors,
                                      "epochs": budget, "within_predeclared_band": within_band,
                                      "validation": predictions,
                                      "checkpoint_sha256": digest(checkpoint_path)}
            errors.extend(f"{name}: {error}" for error in model_errors)
        claimed_pass = all(saved_bundle["models"][name].get("passed") is True for name in budgets)
        if saved_bundle.get("passed") is not claimed_pass:
            errors.append("Bundle passed claim differs from its per-model claims")
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as error:
        errors.append(f"Permutation control audit could not complete: {error}")
    result["passed"] = not errors
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-dir", type=Path, default=ROOT / "data/gold")
    parser.add_argument("--run", action="append", nargs=2, metavar=("LABEL", "RUN_DIR"), default=[])
    parser.add_argument("--reproduce", action="append", nargs=2, metavar=("RUN_A", "RUN_B"), default=[])
    parser.add_argument("--control", action="append", type=Path, default=[])
    parser.add_argument("--permutation-control", action="append", type=Path, default=[],
                        help="Audit a frozen-budget train/val permutation-null bundle")
    parser.add_argument("--include-holdouts", action="store_true",
                        help="Read saved held-out predictions only after final model selection/evaluation")
    parser.add_argument("--tolerance", type=float, default=1e-12)
    parser.add_argument("--output", type=Path,
                        default=ROOT / "checkpoints/phase55-verification/results-audit.json")
    args = parser.parse_args()
    try:
        validate_tolerance(args.tolerance)
    except ValueError as error:
        parser.error(str(error))
    if not args.run and not args.reproduce and not args.control and not args.permutation_control:
        parser.error("Provide at least one --run, --reproduce, --control, or --permutation-control")
    spec = json.loads((args.gold_dir / "_metadata/feature_spec.json").read_text())
    report = {"holdouts_authorized": args.include_holdouts, "runs": {}, "reproducibility": [],
              "controls": [], "permutation_controls": []}
    for label, directory in args.run:
        if label in report["runs"]:
            parser.error(f"Duplicate run label: {label}")
        report["runs"][label] = verify_run(Path(directory), args.gold_dir, spec,
                                           args.include_holdouts, args.tolerance)
    for left, right in args.reproduce:
        report["reproducibility"].append(compare_reproducibility(Path(left), Path(right)))
    for control in args.control:
        report["controls"].append(verify_control(control, args.gold_dir, spec, args.tolerance))
    for bundle in args.permutation_control:
        report["permutation_controls"].append(verify_permutation_control(bundle, args.gold_dir, spec, args.tolerance))
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
    passed &= all(item["passed"] for item in report["permutation_controls"])
    if "acceptance" in report:
        passed &= report["acceptance"]["lr_auc_at_least_0_696"] and report["acceptance"]["deepfm_auc_above_lr"]
    report["passed"] = bool(passed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    print(json.dumps({"passed": report["passed"], "run_labels": list(report["runs"]),
                      "reproducibility_comparisons": len(report["reproducibility"]),
                      "controls": len(report["controls"]), "permutation_controls": len(report["permutation_controls"]),
                      "acceptance": report.get("acceptance"),
                      "report": str(args.output)}, indent=2))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
