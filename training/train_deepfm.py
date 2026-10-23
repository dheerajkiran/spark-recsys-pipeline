"""Train on exported gold; select by val AUC, optionally evaluate held-out splits.

Run with the host training venv, either as a module or directly as a script.
Every run gets its own directory containing best.pt, metrics.json, and the
predictions used for its reported metrics. Test evaluation is opt-in.
"""

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
from itertools import islice
import json
import math
from pathlib import Path
import random
import sys
import uuid

import numpy as np
from sklearn.metrics import log_loss, roc_auc_score
import torch
from torch import nn
from torch.utils.data import DataLoader
import yaml

# Direct execution puts training/, rather than the repository, on sys.path.
ROOT = Path(__file__).resolve().parents[1]
if __package__ in (None, ""):
    sys.path.insert(0, str(ROOT))

from training.dataset import GoldDataset, load_spec
from training.models import DeepFM, LogisticRegression


@dataclass
class TrainConfig:
    model: str = "deepfm"
    epochs: int = 20
    batch_size: int = 4096
    learning_rate: float = 0.001
    patience: int = 3
    seed: int = 42
    embedding_dim: int = 16
    hidden_dims: tuple = (128, 64)
    dropout: float = 0.0
    num_workers: int = 0
    threads: int = 2
    max_train_batches: int | None = None
    max_eval_batches: int | None = None
    evaluate_test: bool = False

    def __post_init__(self):
        if self.model not in ("lr", "deepfm"):
            raise ValueError("model must be lr or deepfm")
        for name in ("epochs", "batch_size", "patience", "embedding_dim", "threads"):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("max_train_batches", "max_eval_batches"):
            value = getattr(self, name)
            if value is not None and (type(value) is not int or value < 1):
                raise ValueError(f"{name} must be a positive integer or None")
        if type(self.num_workers) is not int or self.num_workers < 0:
            raise ValueError("num_workers must be a nonnegative integer")
        if type(self.seed) is not int or self.seed < 0 or self.seed >= 2 ** 32:
            raise ValueError("seed must be an integer in [0, 2**32)")
        if not math.isfinite(self.learning_rate) or self.learning_rate <= 0:
            raise ValueError("learning_rate must be finite and positive")
        if not 0 <= self.dropout < 1:
            raise ValueError("dropout must be in [0, 1)")
        self.hidden_dims = tuple(self.hidden_dims)
        if any(type(width) is not int or width < 1 for width in self.hidden_dims):
            raise ValueError("hidden_dims must contain positive integers")
        if type(self.evaluate_test) is not bool:
            raise ValueError("evaluate_test must be a boolean")


def binary_metrics(labels, probabilities) -> dict:
    """Global metrics, including calibration; AUC requires both classes."""
    labels = np.asarray(labels, dtype=np.float64)
    probabilities = np.asarray(probabilities, dtype=np.float64)
    if labels.ndim != 1 or labels.shape != probabilities.shape or labels.size == 0:
        raise ValueError("Evaluation needs nonempty matching 1D labels and probabilities")
    if not np.isfinite(labels).all() or not np.isfinite(probabilities).all():
        raise ValueError("Evaluation labels and probabilities must be finite")
    if not np.isin(labels, [0, 1]).all() or ((probabilities < 0) | (probabilities > 1)).any():
        raise ValueError("Labels must be binary and probabilities in [0, 1]")
    if np.unique(labels).size != 2:
        raise ValueError("AUC requires both label classes; increase the evaluation subset")
    return {"rows": int(labels.size), "auc": float(roc_auc_score(labels, probabilities)),
            "log_loss": float(log_loss(labels, probabilities, labels=[0, 1])),
            "mean_probability": float(probabilities.mean()),
            "positive_rate": float(labels.mean())}


def evaluate(model, loader, max_batches=None) -> tuple:
    """Return global metrics and their exact source predictions, without updates."""
    model.eval()
    chunks = {name: [] for name in ("label", "probability", "logit")}
    with torch.inference_mode():
        for batch in islice(loader, max_batches):
            logits = model(batch)
            if logits.shape != batch["label"].shape or not torch.isfinite(logits).all():
                raise ValueError("Model must return finite [B] logits")
            chunks["label"].append(batch["label"].cpu().numpy())
            chunks["logit"].append(logits.cpu().numpy())
            chunks["probability"].append(logits.double().sigmoid().cpu().numpy())
    if not chunks["label"]:
        raise ValueError("Evaluation split is empty")
    predictions = {name: np.concatenate(values) for name, values in chunks.items()}
    return binary_metrics(predictions["label"], predictions["probability"]), predictions


def make_model(spec, config):
    if config.model == "lr":
        return LogisticRegression(spec)
    return DeepFM(spec, embedding_dim=config.embedding_dim,
                  hidden_dims=config.hidden_dims, dropout=config.dropout)


def make_loader(dataset, config):
    # Recreate workers each epoch: persistent worker copies would otherwise
    # miss set_epoch() in the parent. Default 0 avoids macOS spawn overhead.
    generator = torch.Generator().manual_seed(config.seed + dataset.epoch)
    return DataLoader(dataset, batch_size=None, num_workers=config.num_workers,
                      generator=generator)


def check_rows(spec, split, rows, limit):
    if limit is None and rows != spec["splits"][split]["rows"]:
        raise ValueError(f"{split}: read {rows} rows; spec expects {spec['splits'][split]['rows']}")


def run_training(gold_dir, run_dir, config: TrainConfig) -> dict:
    """Train on CPU, restore the best val checkpoint, and save run artifacts.

    A nonempty run directory is rejected. With evaluate_test=False, no held-out
    directory is opened. Limited smoke runs are explicitly marked in metrics.
    """
    gold_dir, run_dir = Path(gold_dir).resolve(), Path(run_dir).resolve()
    spec = load_spec(gold_dir)
    random.seed(config.seed)
    np.random.seed(config.seed)
    torch.manual_seed(config.seed)
    torch.set_num_threads(config.threads)
    torch.use_deterministic_algorithms(True)
    train_data = GoldDataset(gold_dir / "train", spec, config.batch_size,
                             shuffle=True, seed=config.seed)
    val_data = GoldDataset(gold_dir / "val", spec, config.batch_size)
    held_out = {}
    if config.evaluate_test:
        for split in ("test", "test_random"):
            if split in spec["splits"]:
                held_out[split] = GoldDataset(gold_dir / split, spec, config.batch_size)
        if "test" not in held_out:
            raise ValueError("Final evaluation requires a test split in the feature spec")
    run_dir.mkdir(parents=True, exist_ok=True)
    if any(run_dir.iterdir()):
        raise FileExistsError(f"Run directory must be empty: {run_dir}")
    prediction_dir = run_dir / "predictions"
    prediction_dir.mkdir()
    model = make_model(spec, config)
    optimizer = torch.optim.Adam(model.parameters(), lr=config.learning_rate)
    loss_fn = nn.BCEWithLogitsLoss()
    history = []
    best_auc, best_epoch, stale_epochs = -math.inf, 0, 0
    best_val = None
    for epoch in range(1, config.epochs + 1):
        train_data.set_epoch(epoch - 1)
        model.train()
        total_loss, rows = 0.0, 0
        for batch in islice(make_loader(train_data, config), config.max_train_batches):
            optimizer.zero_grad(set_to_none=True)
            logits = model(batch)
            if logits.shape != batch["label"].shape:
                raise ValueError("Model must return [B] logits")
            loss = loss_fn(logits, batch["label"])
            if not torch.isfinite(loss):
                raise ValueError("Training loss is not finite")
            loss.backward()
            optimizer.step()
            n = len(batch["label"])
            total_loss += loss.item() * n
            rows += n
        if rows == 0:
            raise ValueError("Training split is empty")
        check_rows(spec, "train", rows, config.max_train_batches)
        val, predictions = evaluate(model, make_loader(val_data, config), config.max_eval_batches)
        check_rows(spec, "val", val["rows"], config.max_eval_batches)
        history.append({"epoch": epoch, "train_loss": total_loss / rows,
                        "train_rows": rows, "val": val})
        print(f"epoch={epoch} train_rows={rows} train_loss={total_loss / rows:.6f} "
              f"val_auc={val['auc']:.6f} val_log_loss={val['log_loss']:.6f}", flush=True)
        if val["auc"] > best_auc:
            best_auc, best_epoch, best_val, stale_epochs = val["auc"], epoch, val, 0
            torch.save({"model_state_dict": model.state_dict(),
                        "optimizer_state_dict": optimizer.state_dict(),
                        "model_config": asdict(config), "feature_spec": spec,
                        "epoch": epoch, "val_metrics": val}, run_dir / "best.pt")
            np.savez(prediction_dir / "val.npz", **predictions)
        else:
            stale_epochs += 1
        if stale_epochs >= config.patience:
            break
    checkpoint = torch.load(run_dir / "best.pt", map_location="cpu", weights_only=True)
    model.load_state_dict(checkpoint["model_state_dict"])
    final = {"val": best_val}
    for split, dataset in held_out.items():
        scores, predictions = evaluate(model, make_loader(dataset, config), config.max_eval_batches)
        check_rows(spec, split, scores["rows"], config.max_eval_batches)
        final[split] = scores
        np.savez(prediction_dir / f"{split}.npz", **predictions)
    result = {"model": config.model, "config": asdict(config), "gold_dir": str(gold_dir),
              "feature_spec_sha256": hashlib.sha256(
                  json.dumps(spec, sort_keys=True).encode()).hexdigest(),
              "history": history, "best_epoch": best_epoch, "best_val": best_val,
              "stopped_early": len(history) < config.epochs,
              "partial_run": config.max_train_batches is not None or config.max_eval_batches is not None,
              "test_evaluated": bool(held_out), "final": final}
    (run_dir / "metrics.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    print(f"Best epoch: {best_epoch}; artifacts: {run_dir}", flush=True)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config" / "pipeline.yaml")
    parser.add_argument("--gold-dir", type=Path)
    parser.add_argument("--run-dir", "--output-dir", dest="run_dir", type=Path)
    parser.add_argument("--model", choices=("lr", "deepfm"))
    for flag in ("epochs", "batch-size", "patience", "seed", "embedding-dim", "num-workers",
                 "threads", "max-train-batches", "max-eval-batches"):
        parser.add_argument(f"--{flag}", type=int)
    parser.add_argument("--learning-rate", type=float)
    parser.add_argument("--dropout", type=float)
    parser.add_argument("--hidden-dims", type=int, nargs="+")
    parser.add_argument("--evaluate-test", action="store_true", default=None,
                        help="Evaluate held-out splits once, using the best checkpoint")
    args = parser.parse_args(argv)
    try:
        raw = yaml.safe_load(args.config.read_text())
        settings = dict(raw.get("training", {}))
        configured_gold = ROOT / settings.pop("gold_dir", "data/gold")
        gold_dir = args.gold_dir or configured_gold
        output_root = ROOT / settings.pop("output_dir", "checkpoints")
        settings.setdefault("seed", raw.get("seed", 42))
        for key in TrainConfig.__dataclass_fields__:
            value = getattr(args, key, None)
            if value is not None:
                settings[key] = value
        config = TrainConfig(**settings)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        run_dir = args.run_dir or output_root / f"{config.model}-{stamp}-{uuid.uuid4().hex[:8]}"
        run_training(gold_dir, run_dir, config)
    except (OSError, ValueError, TypeError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
