"""Trusted local/container entry point for bounded CPU training checks."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

import torch

from training.models import DeepFM


def required_dir(name: str) -> Path:
    raw = os.environ.get(name)
    if not raw or not Path(raw).is_absolute():
        raise ValueError(f"{name} must be an absolute directory path")
    path = Path(raw).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def selfcheck() -> dict:
    work = required_dir("LOOP_WORK_ROOT")
    artifacts = required_dir("LOOP_ARTIFACT_ROOT")
    spec = {
        "categorical": [{"name": "item", "vocab_size": 4}],
        "numeric": ["score"], "flags": [],
        "sequence": {"name": "history", "vocab_size": 4, "pad": 0},
    }
    model = DeepFM(spec, embedding_dim=4, hidden_dims=(8,)).eval()
    with torch.no_grad():
        logits = model({
            "categorical": torch.tensor([[1]]),
            "numeric": torch.tensor([[0.5]]),
            "sequence": torch.tensor([[0, 2]]),
        })
    if logits.shape != (1,) or not torch.isfinite(logits).all():
        raise RuntimeError("CPU model forward check failed")
    return {"status": "ok", "device": "cpu", "torch": torch.__version__,
            "work_root": str(work), "artifact_root": str(artifacts)}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["selfcheck", "train"])
    parser.add_argument("--gold-dir", type=Path)
    parser.add_argument("--run-id")
    args = parser.parse_args()
    try:
        if args.command == "selfcheck":
            print(json.dumps(selfcheck()))
            return
        required_dir("LOOP_WORK_ROOT")
        artifacts = required_dir("LOOP_ARTIFACT_ROOT")
        if not args.gold_dir or not args.gold_dir.is_absolute() or not args.gold_dir.is_dir():
            raise ValueError("--gold-dir must be an existing absolute directory")
        if not args.run_id or not args.run_id.replace("-", "").replace("_", "").isalnum():
            raise ValueError("--run-id must contain only letters, digits, hyphens, or underscores")
        run_dir = artifacts / args.run_id
        command = [sys.executable, "-m", "training.train_deepfm", "--gold-dir",
                   str(args.gold_dir), "--run-dir", str(run_dir), "--model", "deepfm"]
        subprocess.run(command, check=True)
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
