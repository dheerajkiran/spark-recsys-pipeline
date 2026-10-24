"""Fixed-budget randomized-target sanity checks, without held-out data access."""

import argparse
from dataclasses import asdict
import hashlib
import io
import json
from pathlib import Path
import random
import sys

import numpy as np
import torch

ROOT = Path(__file__).resolve().parents[1]
if __package__ in (None, ""):
    sys.path.insert(0, str(ROOT))

from training.dataset import GoldDataset, load_spec
from training.shuffle_labels import prepare_shuffled_gold
from training.train_deepfm import (TrainConfig, check_rows, evaluate, make_loader,
                                   make_model, train_epoch)


def _canonical(value):
    return json.loads(json.dumps(value, sort_keys=True))


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def run_permutation_control(gold_dir, run_dir, selection_path, seed=42) -> dict:
    """Train fresh models to frozen epochs, then score randomized val once.

    Budgets and configurations come from the original pre-test selection.
    Original run artifacts and held-out splits are never modified or evaluated.
    """
    gold_dir, run_dir = Path(gold_dir).resolve(), Path(run_dir).resolve()
    selection_path = Path(selection_path).resolve()
    if type(seed) is not int or seed < 0:
        raise ValueError("seed must be a nonnegative integer")
    if run_dir.exists() and (not run_dir.is_dir() or any(run_dir.iterdir())):
        raise FileExistsError(f"Control run directory must be empty: {run_dir}")
    selection_bytes = selection_path.read_bytes()
    selection = json.loads(selection_bytes)
    if set(selection["models"]) != {"lr", "deepfm"}:
        raise ValueError("Selection must contain exactly lr and deepfm models")
    spec = load_spec(gold_dir)
    spec_sha = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    configs, budgets, hashes = {}, {}, {}
    for name, selected in selection["models"].items():
        config = TrainConfig(**selected["config"])
        budget = selected["best_epoch"]
        if config.model != name:
            raise ValueError(f"{name}: selected configuration has a different model")
        if (config.evaluate_test or config.max_train_batches is not None
                or config.max_eval_batches is not None or config.num_workers != 0):
            raise ValueError("Control needs a full validation-only configuration with num_workers=0")
        if type(budget) is not int or not 1 <= budget <= config.epochs:
            raise ValueError(f"{name}: selected epoch must be within the original training budget")
        checkpoint_path = ROOT / selected["run_dir"] / "best.pt"
        checkpoint_bytes = checkpoint_path.read_bytes()
        checkpoint_sha = hashlib.sha256(checkpoint_bytes).hexdigest()
        if checkpoint_sha != selected["checkpoint_sha256"]:
            raise ValueError(f"{name}: selected checkpoint hash differs from the manifest")
        checkpoint = torch.load(io.BytesIO(checkpoint_bytes), map_location="cpu", weights_only=True)
        if (_canonical(checkpoint["model_config"]) != _canonical(selected["config"])
                or checkpoint["feature_spec"] != spec or selected["feature_spec_sha256"] != spec_sha
                or checkpoint["epoch"] != budget or checkpoint["val_metrics"] != selected["val"]):
            raise ValueError(f"{name}: checkpoint config/spec/epoch/validation differs from the manifest")
        configs[name], budgets[name], hashes[name] = config, budget, checkpoint_sha

    protocol = {"id": "permutation-null-v1", "seed": seed,
                "selection_sha256": hashlib.sha256(selection_bytes).hexdigest(),
                "feature_spec_sha256": spec_sha, "auc_band": [0.49, 0.51],
                "selection_rule": "fixed_terminal_epoch", "budgets": budgets,
                "selected_checkpoint_sha256": hashes}
    run_dir.mkdir(parents=True, exist_ok=True)
    # Exclusive creation preserves a declared protocol before any outcomes and
    # prevents simultaneous processes from sharing a control output directory.
    with (run_dir / "reference_selection.json").open("xb") as stream:
        stream.write(selection_bytes)
    _write_json(run_dir / "protocol.json", protocol)
    control_gold = run_dir / "gold"
    prepare_shuffled_gold(gold_dir, control_gold, seed=seed, permute_validation=True)
    models = {}
    for name, config in configs.items():
        random.seed(config.seed)
        np.random.seed(config.seed)
        torch.manual_seed(config.seed)
        torch.set_num_threads(config.threads)
        torch.use_deterministic_algorithms(True)
        model = make_model(spec, config)
        optimizer = torch.optim.Adam(model.parameters(), lr=config.learning_rate)
        train = GoldDataset(control_gold / "train", spec, config.batch_size,
                            shuffle=True, seed=config.seed)
        val = GoldDataset(control_gold / "val", spec, config.batch_size)
        history = []
        for epoch in range(1, budgets[name] + 1):
            train.set_epoch(epoch - 1)
            trained = train_epoch(model, optimizer, make_loader(train, config))
            check_rows(spec, "train", trained["rows"], None)
            history.append({"epoch": epoch, "train_rows": trained["rows"],
                            "train_loss": trained["loss"]})
            print(f"control={name} epoch={epoch}/{budgets[name]} train_rows={trained['rows']} "
                  f"train_loss={trained['loss']:.6f}", flush=True)
        output = run_dir / name
        output.mkdir()
        # Save the terminal state before observing control-validation outcomes.
        torch.save({"model_state_dict": model.state_dict(),
                    "optimizer_state_dict": optimizer.state_dict(),
                    "model_config": asdict(config), "feature_spec": spec,
                    "epoch": budgets[name]}, output / "final.pt")
        scores, predictions = evaluate(model, make_loader(val, config))
        check_rows(spec, "val", scores["rows"], None)
        np.savez(output / "predictions.npz", **predictions)
        result = {"model": name, "config": asdict(config), "epochs": budgets[name],
                  "history": history, "validation": scores,
                  "checkpoint_sha256": _sha(output / "final.pt"),
                  "prediction_sha256": _sha(output / "predictions.npz"),
                  "passed": protocol["auc_band"][0] <= scores["auc"] <= protocol["auc_band"][1]}
        _write_json(output / "results.json", result)
        models[name] = result
        print(f"control={name} terminal_val_auc={scores['auc']:.6f} passed={result['passed']}", flush=True)
    result = {"protocol": protocol, "models": models,
              "passed": all(model["passed"] for model in models.values())}
    _write_json(run_dir / "results.json", result)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-dir", type=Path, default=ROOT / "data/gold")
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--selection-manifest", type=Path,
                        default=ROOT / "training/results/phase5_selection.json")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    try:
        result = run_permutation_control(args.gold_dir, args.run_dir, args.selection_manifest, args.seed)
    except (OSError, ValueError, TypeError, KeyError) as error:
        parser.error(str(error))
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
